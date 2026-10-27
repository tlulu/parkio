use mio::Interest;
use runtime::Runtime;
use std::{
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

mod futures;
mod runtime;

async fn sleeper() {
    println!("sleeper starting");
    futures::SleepFuture::new(Duration::from_secs(3)).await;
    println!("sleeper finished");
}

async fn ready_once() {
    let a = futures::ReadyOnce { value: 10 }.await;
    let b = futures::ReadyOnce { value: 20 }.await;
    let c = futures::ReadyOnce { value: 30 }.await;
    assert_eq!(a + b + c, 60);
    println!("ready_once finished");
}

async fn yield_once(counter: Arc<AtomicUsize>) {
    futures::PollOnce::default().await;
    println!("yield finished");
    counter.fetch_add(1, Ordering::Relaxed);
}

fn test_runtime() {
    let mut runtime = Runtime::new();

    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
        runtime.spawn(Box::pin(yield_once(Arc::clone(&counter))));
    }

    runtime.spawn(Box::pin(ready_once()));
    runtime.spawn(Box::pin(sleeper()));

    runtime.start();

    assert_eq!(counter.load(Ordering::Relaxed), 100);
}

fn test_runtime_with_io() -> io::Result<()> {
    // Create server
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;

    // Client connect
    let std_stream = std::net::TcpStream::connect(addr)?;
    std_stream.set_nonblocking(true)?;
    let mut client = mio::net::TcpStream::from_std(std_stream);

    // Server accept
    let (server_sock, _peer) = listener.accept()?;
    server_sock.set_nonblocking(true)?;
    let mut server = mio::net::TcpStream::from_std(server_sock);

    let mut runtime = Runtime::new();

    // Client task
    runtime.spawn(Box::pin(async move {
        // Sleep briefly so server attempts to read first and parks on WouldBlock
        // We should see "[IO Future] Blocked on read" in the logs.
        println!("[Client] Sleeping 200ms before sending...");
        futures::SleepFuture::new(Duration::from_millis(200)).await;
        println!("[Client] Sending ping");
        client.write(b"ping").unwrap();
    }));

    // Server task
    runtime.register_io(&mut server, 1, Interest::READABLE); // TODO remove hard coded ID.
    runtime.spawn(Box::pin(async move {
        let mut buf = [0u8; 64];
        let n = futures::TcpReadFuture::new(&mut server, &mut buf)
            .await
            .unwrap();
        let msg = std::str::from_utf8(&buf[..n]).unwrap();
        println!("[Server] Read {} bytes: {}", n, msg);
        assert_eq!(msg, "ping");
    }));

    runtime.start();
    Ok(())
}

fn main() {
    test_runtime();
    test_runtime_with_io().unwrap();
}
