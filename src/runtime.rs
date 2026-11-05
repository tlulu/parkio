use mio::{Events, Interest, Registry, Token};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, JoinHandle},
};
use threadpool::ThreadPool;

use crate::runtime::TaskState::{Idle, Notified, Scheduled};

type AsyncTask = Pin<Box<dyn Future<Output = ()> + Send>>;

// The states transition in two paths:
// 1. Idle -> Scheduled -> Notified
// 2. Idle -> Scheduled
enum TaskState {
    Idle,      // The initial state of the task
    Scheduled, // The task has been scheduled. The task is either running or is blocked.
    Notified,  // The scheduled task is woken up
}

// Sends a message to the runtime thread to continue work on the task.
struct TaskWaker {
    task_id: usize,
    sender: Sender<Msg>, // A copy of the runtime's Sender.
}

// Messages sent to the runtime from client threads or the runtime's own worker threads.
enum Msg {
    Wake(usize),             // Schedule a task into the task queue.
    Yield(usize, AsyncTask), // Task is blocked - return it back to the runtime
    Finish(usize),           // The task is finished
}

// The runtime uses the main thread to run async tasks.
// Tasks get queued in a mpsc channel and are processed concurrently.

// For OS events, the runtime listens on a separate thread.
// This event listener uses mio::Poll, a low level abstraction for epoll, kqueue, IOCP that returns
// events by the OS when IO is ready to proceed.

// The runtime finishes when there are no pending tasks.
pub struct Runtime {
    tasks: HashMap<usize, AsyncTask>,
    task_states: HashMap<usize, TaskState>,
    pending_tasks: usize,
    next_task_id: usize,
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    threadpool: ThreadPool,

    // IO event listener
    pub io_registry: Arc<Registry>,
    io_waker: mio::Waker,
    io_listener_shutdown: Arc<AtomicBool>,
    io_thread_handle: Option<JoinHandle<()>>,
}

impl Runtime {
    pub fn spawn(&mut self, async_task: AsyncTask) {
        let task_id = self.next_task_id;
        self.tasks.insert(task_id, async_task);
        self.task_states.insert(task_id, Idle);
        self.next_task_id += 1;
        self.pending_tasks += 1;
        let _ = self.sender.send(Msg::Wake(task_id));
    }

    pub fn register_io<T: mio::event::Source>(
        &mut self,
        source: &mut T,
        id: usize,
        interests: Interest,
    ) {
        let _ = self.io_registry.register(source, Token(id), interests);
    }

    pub fn start(&mut self) {
        while self.pending_tasks > 0 {
            // recv is a blocking call until a new message arrives.
            let msg = self.receiver.recv().unwrap();
            match msg {
                Msg::Wake(task_id) => {
                    if let Some(state) = self.task_states.get(&task_id) {
                        match state {
                            Idle => {
                                if let Some(task) = self.tasks.remove(&task_id) {
                                    let sender = self.sender.clone();
                                    self.task_states.insert(task_id, Scheduled);
                                    self.threadpool.execute(move || {
                                        Self::process(task, task_id, sender);
                                    });
                                }
                            }
                            Scheduled => {
                                // There can a scenario where wake is called before Poll::Pending is returned
                                // We need to track tasks that were notified so we can wake them up.
                                self.task_states.insert(task_id, Notified);
                            }
                            Notified => {}
                        }
                    }
                }
                Msg::Finish(task_id) => {
                    self.pending_tasks -= 1;
                    self.task_states.remove(&task_id);
                }
                Msg::Yield(task_id, task) => {
                    self.tasks.insert(task_id, task);

                    if let Some(state) = self.task_states.get(&task_id) {
                        match state {
                            Notified => {
                                // Re-schedule the task
                                self.task_states.insert(task_id, Idle);
                                let _ = self.sender.send(Msg::Wake(task_id));
                            }
                            _ => {
                                self.task_states.insert(task_id, Idle);
                            }
                        }
                    }
                }
            }
        }
    }

    fn process(mut task: AsyncTask, task_id: usize, sender: Sender<Msg>) {
        let waker = Waker::from(Arc::new(TaskWaker {
            task_id,
            sender: sender.clone(),
        }));
        let mut context = Context::from_waker(&waker);

        println!("[Thread] working on task {task_id}");
        let _ = match task.as_mut().poll(&mut context) {
            Poll::Ready(_) => sender.send(Msg::Finish(task_id)),
            Poll::Pending => sender.send(Msg::Yield(task_id, task)),
        };
    }

    pub fn new(poolsize: usize) -> Self {
        let (sender, receiver): (Sender<Msg>, Receiver<Msg>) = mpsc::channel();
        let poll = mio::Poll::new().expect("[Runtime] Failed to initialize mio::Poll");
        let registry = Arc::new(
            poll.registry()
                .try_clone()
                .expect("[Runtime] Failed to clone registry"),
        );
        let io_listener_shutdown = Arc::new(AtomicBool::new(false));
        let io_sender = sender.clone();
        let shutdown_clone = Arc::clone(&io_listener_shutdown);
        let io_waker = mio::Waker::new(&poll.registry(), IO_THREAD_SHUTDOWN_TOKEN)
            .expect("[Runtime] Failed to initialize mio Waker");

        let io_thread_handle = thread::spawn(move || {
            Self::io_listener(poll, io_sender, shutdown_clone);
        });

        Self {
            tasks: HashMap::new(),
            task_states: HashMap::new(),
            sender,
            receiver,
            pending_tasks: 0,
            next_task_id: 0,
            threadpool: ThreadPool::new(poolsize),
            io_registry: registry,
            io_waker: io_waker,
            io_listener_shutdown: io_listener_shutdown,
            io_thread_handle: Some(io_thread_handle),
        }
    }

    fn io_listener(
        mut io_poll: mio::Poll,
        sender: Sender<Msg>,
        io_listener_shutdown: Arc<AtomicBool>,
    ) {
        let mut events = Events::with_capacity(1024);
        loop {
            // Blocking call until events arrive.
            if let Err(e) = io_poll.poll(&mut events, None) {
                eprintln!("[Runtime] poll error: {e}");
                break;
            }

            for event in events.iter() {
                match event.token() {
                    IO_THREAD_SHUTDOWN_TOKEN => {
                        if io_listener_shutdown.load(Ordering::Relaxed) {
                            return;
                        }
                    }
                    Token(task_id) => {
                        println!("[Runtime] Received OS event for task {task_id}");
                        let _ = sender.send(Msg::Wake(task_id));
                    }
                }
            }
        }
    }

    fn stop_io_listener(&mut self) {
        self.io_listener_shutdown.store(true, Ordering::Relaxed);
        let _ = self.io_waker.wake();
        if let Some(handle) = self.io_thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop_io_listener();
    }
}

const IO_THREAD_SHUTDOWN_TOKEN: Token = Token(usize::MAX);

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.sender.send(Msg::Wake(self.task_id));
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.sender.send(Msg::Wake(self.task_id));
    }
}
