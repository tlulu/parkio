use mio::{Events, Interest, Registry, Token};
use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread::{self, JoinHandle, Thread, current},
};

pub struct AsyncTask {
    f: Pin<Box<dyn Future<Output = ()>>>,
}

// Sends a message to the runtime thread to continue work on the task.
struct TaskWaker {
    task_id: usize,
    thread: Thread,
    sender: Sender<usize>, // A copy of the runtime's Sender.
}

// An single-threaded runtime for running async tasks.
//
// This runtime uses the main thread to work on tasks concurrently.
// The main worker pulls work from the task queue and parks until there is no work.
// The worker resumes work via the TaskWaker which is stored on the heap.

// The runtime creates a separate thread that listens for OS events.
// This event listener is handled by mio::Poll, a low level abstraction for epoll, kqueue, IOCP that returns
// events by the OS when IO is ready to proceed.

// The runtime finishes when there are no pending tasks left in the queue.

// TODO keep the runtime running even if there are no pending tasks, like an ExecutorService.
pub struct Runtime {
    task_queue: VecDeque<usize>,
    task_map: HashMap<usize, AsyncTask>,
    pending_tasks: u32,
    sender: Sender<usize>,
    receiver: Receiver<usize>,
    next_task_id: usize,

    // IO event listener
    pub io_registry: Arc<Registry>,
    io_waker: mio::Waker,
    io_listener_shutdown: Arc<AtomicBool>,
    io_thread_handle: Option<JoinHandle<()>>,
}

impl Runtime {
    pub fn spawn(&mut self, f: Pin<Box<dyn Future<Output = ()>>>) {
        let async_task = AsyncTask { f: f };
        self.task_queue.push_back(self.next_task_id);
        self.task_map.insert(self.next_task_id, async_task);
        self.pending_tasks += 1;
        self.next_task_id += 1;
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
        loop {
            for task_id in self.receiver.try_iter() {
                self.task_queue.push_back(task_id);
            }

            let Some(task_id) = self.task_queue.pop_front() else {
                if self.pending_tasks == 0 {
                    break;
                }
                println!("[Runtime] Parking");
                thread::park();
                continue;
            };

            if let Some(task) = self.task_map.get_mut(&task_id) {
                let waker = get_waker(TaskWaker {
                    task_id: task_id,
                    thread: current(), // TODO add threadpool.
                    sender: self.sender.clone(),
                });
                let mut context = Context::from_waker(&waker);

                println!("[Runtime] working on task {task_id}");
                match task.f.as_mut().poll(&mut context) {
                    Poll::Ready(_) => {
                        self.task_map.remove(&task_id);
                        self.pending_tasks -= 1
                    }
                    Poll::Pending => {}
                }
            }
        }
    }

    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel();
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

        let runtime_thread = current();
        let io_thread_handle = thread::spawn(move || {
            Self::io_listener(runtime_thread, poll, io_sender, shutdown_clone);
        });

        Self {
            task_queue: VecDeque::new(),
            task_map: HashMap::new(),
            pending_tasks: 0,
            sender,
            receiver,
            next_task_id: 0,
            io_registry: registry,
            io_waker: io_waker,
            io_listener_shutdown: io_listener_shutdown,
            io_thread_handle: Some(io_thread_handle),
        }
    }

    fn io_listener(
        runtime_thread: Thread,
        mut io_poll: mio::Poll,
        sender: Sender<usize>,
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
                        let _ = sender.send(task_id);
                        runtime_thread.unpark();
                    }
                }
            }
        }
    }

    fn stop_listener(&mut self) {
        self.io_listener_shutdown.store(true, Ordering::Relaxed);
        let _ = self.io_waker.wake();
        if let Some(handle) = self.io_thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop_listener();
    }
}

fn get_waker(task_waker: TaskWaker) -> Waker {
    let p: *mut TaskWaker = Box::into_raw(Box::new(task_waker));
    let waker = my_waker(p as *const ());
    return waker;
}

const IO_THREAD_SHUTDOWN_TOKEN: Token = Token(usize::MAX);

type WakerData = *const ();

fn clone(w: WakerData) -> RawWaker {
    my_raw_waker(w)
}

fn wake(w: WakerData) {
    let typed: *const TaskWaker = w as *const TaskWaker;
    let task_waker: &TaskWaker = unsafe { &*typed };
    task_waker.thread.unpark();
    let _ = task_waker.sender.send(task_waker.task_id);
}

fn wake_by_ref(w: WakerData) {
    wake(w)
}
fn drop(_: WakerData) {}

static MY_VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

fn my_raw_waker(w: WakerData) -> RawWaker {
    RawWaker::new(w, &MY_VTABLE)
}

fn my_waker(w: WakerData) -> Waker {
    unsafe { Waker::from_raw(my_raw_waker(w)) }
}
