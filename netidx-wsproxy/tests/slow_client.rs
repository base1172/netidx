//! End-to-end test that a single slow websocket client does not block update
//! delivery to the other clients when `--per-client-buffer` is enabled.
//!
//! Topology (all in-process, loopback):
//!   resolver server  <-  publisher (publishes /bench/data, updates rapidly)
//!                    <-  wsproxy (shared Subscriber)  <-  two ws clients
//!
//! Client A reads continuously; client B subscribes then stops reading,
//! simulating a slow consumer. We measure how many updates A receives in the
//! final window, after buffers have had time to fill. With per-client buffering
//! A keeps flowing; without it A stalls because B parks the shared subscriber
//! connection (the bug this change fixes).
//!
//! The websocket client is hand-rolled over a raw TCP socket (a few dozen lines
//! below) specifically so the test can stop reading and exert real TCP
//! backpressure — which is the whole point of the slow-consumer scenario, and
//! something the off-the-shelf test clients (e.g. warp::test::ws) cannot do
//! because they drain the socket for you. It also avoids a new dependency.

use netidx::{
    config::Config as ClientConfig,
    path::Path,
    protocol::value::Value,
    publisher::{BindCfg, Publisher, PublisherBuilder},
    resolver_client::DesiredAuth,
    resolver_server::{config::Config as ServerConfig, Server},
    subscriber::Subscriber,
};
use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
};

const PATH: &str = "/bench/data";
const PAYLOAD: usize = 64 * 1024;
const SETTLE: Duration = Duration::from_secs(2);
const TOTAL: Duration = Duration::from_secs(4);

// Minimal websocket client over a raw TCP socket. Only implements what this
// test needs: the client handshake, sending masked text frames, and reading
// (unmasked) server frames. Crucially, it only reads when we ask it to, so a
// client that stops calling `read_frame` exerts real TCP backpressure.
struct RawWs {
    stream: TcpStream,
}

impl RawWs {
    async fn connect(addr: SocketAddr, path: &str) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        // "dGhlIHNhbXBsZSBub25jZQ==" is the canonical example 16-byte key; the
        // server validates its presence and computes the accept hash. We don't
        // verify the response beyond consuming the headers.
        let req = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: {addr}\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await?;
        // Read up to (and including) the end of the response headers, one byte
        // at a time so we never consume into the websocket frame stream.
        let mut buf: Vec<u8> = Vec::with_capacity(256);
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).await?;
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        Ok(Self { stream })
    }

    // Send a masked text frame (client frames must be masked per RFC 6455).
    async fn send_text(&mut self, text: &str) -> io::Result<()> {
        let payload = text.as_bytes();
        let len = payload.len();
        let mut frame = Vec::with_capacity(len + 14);
        frame.push(0x81); // FIN + text opcode
        if len < 126 {
            frame.push(0x80 | len as u8);
        } else if len <= 0xffff {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        let mask = [0x12u8, 0x34, 0x56, 0x78];
        frame.extend_from_slice(&mask);
        for (i, b) in payload.iter().enumerate() {
            frame.push(b ^ mask[i % 4]);
        }
        self.stream.write_all(&frame).await
    }

    // Read one server frame, returning (opcode, payload). Server frames are not
    // masked. read_exact reassembles frames split across TCP segments.
    async fn read_frame(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut hdr = [0u8; 2];
        self.stream.read_exact(&mut hdr).await?;
        let opcode = hdr[0] & 0x0f;
        let masked = hdr[1] & 0x80 != 0;
        let mut len = (hdr[1] & 0x7f) as usize;
        if len == 126 {
            let mut b = [0u8; 2];
            self.stream.read_exact(&mut b).await?;
            len = u16::from_be_bytes(b) as usize;
        } else if len == 127 {
            let mut b = [0u8; 8];
            self.stream.read_exact(&mut b).await?;
            len = u64::from_be_bytes(b) as usize;
        }
        let mut mask = [0u8; 4];
        if masked {
            self.stream.read_exact(&mut mask).await?;
        }
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await?;
        if masked {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= mask[i % 4];
            }
        }
        Ok((opcode, payload))
    }

    // Subscribe to PATH and block until the subscription is confirmed.
    async fn subscribe(&mut self) -> io::Result<()> {
        self.send_text(&format!(r#"{{"type":"Subscribe","path":"{PATH}"}}"#)).await?;
        loop {
            let (opcode, payload) = self.read_frame().await?;
            if opcode == 0x1
                && String::from_utf8_lossy(&payload).contains(r#""type":"Subscribed""#)
            {
                return Ok(());
            }
        }
    }
}

async fn start_backend() -> (Server, Publisher, Subscriber) {
    let server_cfg =
        ServerConfig::load("../cfg/simple-server.json").expect("load server cfg");
    let mut client_cfg =
        ClientConfig::load("../cfg/simple-client.json").expect("load client cfg");
    let server = Server::new(server_cfg, false, 0).await.expect("start resolver");
    client_cfg.addrs[0].0 = *server.local_addr();
    let publisher = PublisherBuilder::new(client_cfg.clone())
        .desired_auth(DesiredAuth::Anonymous)
        .bind_cfg(Some(BindCfg::Local))
        .build()
        .await
        .expect("build publisher");
    let subscriber =
        Subscriber::new(client_cfg, DesiredAuth::Anonymous).expect("build subscriber");
    (server, publisher, subscriber)
}

// Publish PATH and spawn a task that updates it rapidly with a large payload
// (large so a non-reading client's socket buffers fill quickly).
fn start_publishing(publisher: &Publisher) -> JoinHandle<()> {
    let val = publisher
        .publish(Path::from(PATH), Value::from("init".to_string()))
        .expect("publish");
    let publisher = publisher.clone();
    tokio::spawn(async move {
        let filler = "x".repeat(PAYLOAD);
        let mut n: u64 = 0u64;
        loop {
            n += 1;
            let mut batch = publisher.start_batch();
            val.update(&mut batch, Value::from(format!("{n}-{filler}")));
            // None timeout: never forcibly disconnect a slow subscriber, so we
            // observe the steady-state head-of-line behavior.
            batch.commit(None).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
}

// Run one scenario; returns the number of Update messages client A receives in
// the final (TOTAL - SETTLE) window while client B is a stalled consumer.
async fn run_scenario(per_client_buffer: bool) -> u64 {
    let (server, publisher, subscriber) = start_backend().await;
    let updater = start_publishing(&publisher);

    let routes = netidx_wsproxy::filter_opts(
        publisher.clone(),
        subscriber.clone(),
        "ws",
        None,
        per_client_buffer,
    );
    let (addr, srv) = warp::serve(routes).bind_ephemeral(([127, 0, 0, 1], 0u16));
    let proxy = tokio::spawn(srv);

    let mut a = RawWs::connect(addr, "/ws").await.expect("connect A");
    let mut b = RawWs::connect(addr, "/ws").await.expect("connect B");
    a.subscribe().await.expect("subscribe A");
    b.subscribe().await.expect("subscribe B");

    // From here on, B never reads again: it is the slow consumer.
    let start = Instant::now();
    let mut count = 0u64;
    loop {
        let elapsed = start.elapsed();
        if elapsed >= TOTAL {
            break;
        }
        match tokio::time::timeout(TOTAL - elapsed, a.read_frame()).await {
            Ok(Ok((0x1, payload))) => {
                if start.elapsed() >= SETTLE
                    && String::from_utf8_lossy(&payload).contains(r#""type":"Update""#)
                {
                    count += 1;
                }
            }
            Ok(Ok((0x8, _))) => break, // server closed
            Ok(Ok(_)) => (),          // other control/data frame
            Ok(Err(e)) => panic!("client A read error: {e}"),
            Err(_) => break, // measurement window elapsed
        }
    }

    drop(b);
    updater.abort();
    proxy.abort();
    drop(server);
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_client_does_not_block_others() {
    // The fix: with per-client buffering, A keeps receiving updates even though
    // B has stopped reading.
    let fixed = run_scenario(true).await;
    // The bug: without it, B's stall parks the shared subscriber connection and
    // A stops receiving updates.
    let buggy = run_scenario(false).await;

    eprintln!(
        "updates received by fast client A in final {}s window: \
         per-client-buffer ON = {fixed}, OFF = {buggy}",
        (TOTAL - SETTLE).as_secs(),
    );

    assert!(
        fixed >= 50,
        "with per-client-buffer ON, the fast client should keep receiving \
         updates despite a slow peer, but only got {fixed}"
    );
    assert!(
        buggy * 5 < fixed,
        "expected the fast client to be starved with per-client-buffer OFF \
         (got {buggy}) relative to ON (got {fixed}); the slow peer should stall it"
    );
}
