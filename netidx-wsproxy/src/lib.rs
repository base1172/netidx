use crate::protocol::{Request, Response, Update};
use ahash::AHashMap;
use anyhow::{anyhow, bail, Result};
use futures::{
    channel::mpsc,
    prelude::*,
    select_biased,
    stream::{FuturesUnordered, SplitSink},
    StreamExt,
};
use log::warn;
use netidx::{
    path::Path,
    protocol::value::Value,
    publisher::{Id as PubId, Publisher, UpdateBatch, Val as Pub},
    subscriber::{Dval as Sub, Event, SubId, Subscriber, UpdatesFlags},
    utils::{BatchItem, Batched},
};
use netidx_protocols::rpc::client::Proc;
use nohash::IntMap;
use poolshark::global::{GPooled, Pool};
use std::time::Duration;
use std::{
    collections::hash_map::Entry, net::SocketAddr, pin::Pin, result, sync::LazyLock,
};
use warp::{
    filters::BoxedFilter,
    ws::{Message, WebSocket, Ws},
    Filter, Reply,
};

pub mod config;
mod protocol;

struct SubEntry {
    count: usize,
    path: Path,
    val: Sub,
}

struct PubEntry {
    path: Path,
    val: Pub,
}

type PendingCall =
    Pin<Box<dyn Future<Output = (u64, Result<Value>)> + Send + Sync + 'static>>;

/// An outbound message queued for the per-client writer task (buffered mode).
enum OutMsg {
    Resp(Response),
}

/// Enqueue an outbound response for the writer task. Returns an error if the
/// writer task has exited (e.g. the client hit its send timeout), which tells
/// the control loop to shut this client down.
fn send_buffered(
    tx_out: &mpsc::UnboundedSender<OutMsg>,
    resp: Response,
) -> Result<()> {
    tx_out
        .unbounded_send(OutMsg::Resp(resp))
        .map_err(|_| anyhow!("writer task gone"))
}

async fn reply<'a>(
    tx: &mut SplitSink<WebSocket, Message>,
    m: &Response,
    timeout: Option<Duration>,
) -> Result<()> {
    let s = serde_json::to_string(m)?;
    // CR base1172 for estokes: Here we're only enforcing that the SplitSink write completes within
    // [timeout], with no guarantee on how long it takes to actually flush the message to the client.
    // In a perfect world we'd probably want a proper flush timeout (similar to what [WriteChannel] does).
    // For now, just requiring that [tx.send(..)] completes within [timeout] is probably good enough.
    // DUR
    let fut = tx.send(Message::text(s));
    match timeout {
        None => Ok(fut.await?),
        Some(timeout) => Ok(tokio::time::timeout(timeout, fut).await??),
    }
}
async fn err(
    tx: &mut SplitSink<WebSocket, Message>,
    message: impl Into<String>,
    timeout: Option<Duration>,
) -> Result<()> {
    reply(tx, &Response::Error { error: message.into() }, timeout).await
}

struct ClientCtx {
    publisher: Publisher,
    subscriber: Subscriber,
    subs: IntMap<SubId, SubEntry>,
    pubs: IntMap<PubId, PubEntry>,
    subs_by_path: AHashMap<Path, SubId>,
    pubs_by_path: AHashMap<Path, PubId>,
    rpcs: AHashMap<Path, Proc>,
    tx_up: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
}

impl ClientCtx {
    fn new(
        publisher: Publisher,
        subscriber: Subscriber,
        tx_up: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        Self {
            publisher,
            subscriber,
            tx_up,
            subs: IntMap::default(),
            pubs: IntMap::default(),
            subs_by_path: AHashMap::default(),
            pubs_by_path: AHashMap::default(),
            rpcs: AHashMap::default(),
        }
    }

    fn subscribe(&mut self, path: Path) -> SubId {
        match self.subs_by_path.entry(path) {
            Entry::Occupied(e) => {
                let se = self.subs.get_mut(e.get()).unwrap();
                se.count += 1;
                se.val.id()
            }
            Entry::Vacant(e) => {
                let path = e.key().clone();
                let val = self.subscriber.subscribe(path.clone());
                let id = val.id();
                val.updates(UpdatesFlags::BEGIN_WITH_LAST, self.tx_up.clone());
                self.subs.insert(id, SubEntry { count: 1, path, val });
                e.insert(id);
                id
            }
        }
    }

    fn unsubscribe(&mut self, id: SubId) -> Result<()> {
        match self.subs.get_mut(&id) {
            None => bail!("not subscribed"),
            Some(se) => {
                se.count -= 1;
                if se.count == 0 {
                    let path = se.path.clone();
                    self.subs.remove(&id);
                    self.subs_by_path.remove(&path);
                }
                Ok(())
            }
        }
    }

    fn write(&mut self, id: SubId, val: Value) -> Result<()> {
        match self.subs.get(&id) {
            None => bail!("not subscribed"),
            Some(se) => {
                se.val.write(val);
                Ok(())
            }
        }
    }

    fn publish(&mut self, path: Path, val: Value) -> Result<PubId> {
        match self.pubs_by_path.entry(path) {
            Entry::Occupied(_) => bail!("already published"),
            Entry::Vacant(e) => {
                let path = e.key().clone();
                let val = self.publisher.publish(path.clone(), val)?;
                let id = val.id();
                e.insert(id);
                self.pubs.insert(id, PubEntry { val, path });
                Ok(id)
            }
        }
    }

    fn unpublish(&mut self, id: PubId) -> Result<()> {
        match self.pubs.remove(&id) {
            None => bail!("not published"),
            Some(pe) => {
                self.pubs_by_path.remove(&pe.path);
                Ok(())
            }
        }
    }

    fn update(
        &mut self,
        batch: &mut UpdateBatch,
        mut updates: GPooled<Vec<protocol::BatchItem>>,
    ) -> Result<()> {
        for up in updates.drain(..) {
            match self.pubs.get(&up.id) {
                None => bail!("not published"),
                Some(pe) => pe.val.update(batch, up.data),
            }
        }
        Ok(())
    }

    fn call(
        &mut self,
        id: u64,
        path: Path,
        mut args: GPooled<Vec<(GPooled<String>, Value)>>,
    ) -> Result<PendingCall> {
        let proc = match self.rpcs.entry(path) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let proc = Proc::new(&self.subscriber, e.key().clone())?;
                e.insert(proc)
            }
        }
        .clone();
        Ok(Box::pin(async move { (id, proc.call(args.drain(..)).await) }) as PendingCall)
    }

    async fn process_from_client(
        &mut self,
        tx: &mut SplitSink<WebSocket, Message>,
        queued: &mut Vec<result::Result<Message, warp::Error>>,
        calls_pending: &mut FuturesUnordered<PendingCall>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut batch = self.publisher.start_batch();
        for r in queued.drain(..) {
            let m = r?;
            if m.is_ping() {
                continue;
            }
            match m.to_str() {
                Err(_) => err(tx, "expected text", timeout).await?,
                Ok(txt) => match serde_json::from_str::<Request>(txt) {
                    Err(e) => {
                        err(tx, format!("could not parse message {}", e), timeout).await?
                    }
                    Ok(req) => match req {
                        Request::Subscribe { path } => {
                            let id = self.subscribe(path);
                            reply(tx, &Response::Subscribed { id }, timeout).await?
                        }
                        Request::Unsubscribe { id } => match self.unsubscribe(id) {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(()) => reply(tx, &Response::Unsubscribed, timeout).await?,
                        },
                        Request::Write { id, val } => match self.write(id, val) {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(()) => reply(tx, &Response::Wrote, timeout).await?,
                        },
                        Request::Publish { path, init } => match self.publish(path, init)
                        {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(id) => {
                                reply(tx, &Response::Published { id }, timeout).await?
                            }
                        },
                        Request::Unpublish { id } => match self.unpublish(id) {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(()) => reply(tx, &Response::Unpublished, timeout).await?,
                        },
                        Request::Update { updates } => {
                            match self.update(&mut batch, updates) {
                                Err(e) => err(tx, e.to_string(), timeout).await?,
                                Ok(()) => reply(tx, &Response::Updated, timeout).await?,
                            }
                        }
                        Request::Call { id, path, args } => {
                            match self.call(id, path, args) {
                                Ok(pending) => calls_pending.push(pending),
                                Err(e) => {
                                    let error = format!("rpc call failed {}", e);
                                    reply(
                                        tx,
                                        &Response::CallFailed { id, error },
                                        timeout,
                                    )
                                    .await?
                                }
                            }
                        }
                        Request::Unknown => err(tx, "unknown request", timeout).await?,
                    },
                },
            }
        }
        Ok(batch.commit(timeout).await)
    }

    // Buffered-mode counterpart of [process_from_client]. Instead of writing
    // replies directly to the websocket sink (which can block the control loop
    // on a slow client), every reply is enqueued on [tx_out] for the dedicated
    // writer task. Reuses the same request-handling methods as the unbuffered
    // path; only the reply delivery differs.
    async fn process_from_client_buffered(
        &mut self,
        tx_out: &mpsc::UnboundedSender<OutMsg>,
        queued: &mut Vec<result::Result<Message, warp::Error>>,
        calls_pending: &mut FuturesUnordered<PendingCall>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut batch = self.publisher.start_batch();
        for r in queued.drain(..) {
            let m = r?;
            if m.is_ping() {
                continue;
            }
            let resp = match m.to_str() {
                Err(_) => Response::Error { error: "expected text".into() },
                Ok(txt) => match serde_json::from_str::<Request>(txt) {
                    Err(e) => Response::Error {
                        error: format!("could not parse message {}", e),
                    },
                    Ok(req) => match req {
                        Request::Subscribe { path } => {
                            Response::Subscribed { id: self.subscribe(path) }
                        }
                        Request::Unsubscribe { id } => match self.unsubscribe(id) {
                            Err(e) => Response::Error { error: e.to_string() },
                            Ok(()) => Response::Unsubscribed,
                        },
                        Request::Write { id, val } => match self.write(id, val) {
                            Err(e) => Response::Error { error: e.to_string() },
                            Ok(()) => Response::Wrote,
                        },
                        Request::Publish { path, init } => {
                            match self.publish(path, init) {
                                Err(e) => Response::Error { error: e.to_string() },
                                Ok(id) => Response::Published { id },
                            }
                        }
                        Request::Unpublish { id } => match self.unpublish(id) {
                            Err(e) => Response::Error { error: e.to_string() },
                            Ok(()) => Response::Unpublished,
                        },
                        Request::Update { updates } => {
                            match self.update(&mut batch, updates) {
                                Err(e) => Response::Error { error: e.to_string() },
                                Ok(()) => Response::Updated,
                            }
                        }
                        Request::Call { id, path, args } => {
                            match self.call(id, path, args) {
                                Ok(pending) => {
                                    calls_pending.push(pending);
                                    continue;
                                }
                                Err(e) => Response::CallFailed {
                                    id,
                                    error: format!("rpc call failed {}", e),
                                },
                            }
                        }
                        Request::Unknown => {
                            Response::Error { error: "unknown request".into() }
                        }
                    },
                },
            };
            send_buffered(tx_out, resp)?;
        }
        Ok(batch.commit(timeout).await)
    }
}

async fn handle_client(
    publisher: Publisher,
    subscriber: Subscriber,
    ws: WebSocket,
    timeout: Option<Duration>,
) -> Result<()> {
    static UPDATES: LazyLock<Pool<Vec<Update>>> = LazyLock::new(|| Pool::new(50, 10000));
    let (tx_up, mut rx_up) = mpsc::channel::<GPooled<Vec<(SubId, Event)>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    let (mut tx_ws, rx_ws) = ws.split();
    let mut queued: Vec<result::Result<Message, warp::Error>> = Vec::new();
    let mut rx_ws = Batched::new(rx_ws.fuse(), 10_000);
    let mut calls_pending: FuturesUnordered<PendingCall> = FuturesUnordered::new();
    calls_pending.push(Box::pin(async { future::pending().await }) as PendingCall);
    loop {
        select_biased! {
            (id, res) = calls_pending.select_next_some() => match res {
                Ok(result) => {
                    reply(&mut tx_ws, &Response::CallSuccess { id, result }, timeout).await?
                }
                Err(e) => {
                    let error = format!("rpc call failed {}", e);
                    reply(&mut tx_ws, &Response::CallFailed { id, error }, timeout).await?
                }
            },
            r = rx_ws.select_next_some() => match r {
                BatchItem::InBatch(r) => queued.push(r),
                BatchItem::EndBatch => {
                    ctx.process_from_client(
                        &mut tx_ws,
                        &mut queued,
                        &mut calls_pending,
                        timeout
                    ).await?
                }
            },
            mut batch = rx_up.select_next_some() => {
                let mut updates = UPDATES.take();
                for (id, event) in batch.drain(..) {
                    updates.push(Update {id, event});
                }
                reply(&mut tx_ws, &Response::Update { updates }, timeout).await?
            },
        }
    }
}

// Per-client writer task used in buffered mode. It owns the websocket sink and
// drains the unbounded outbound channel, applying the same per-message timeout
// as [reply]. Because the control loop only ever feeds this task via a
// non-blocking [mpsc::UnboundedSender::unbounded_send], a slow client backs up
// here (in its own buffer) rather than stalling the control loop and, through
// it, the shared subscriber connection. Returns Err on send timeout or socket
// error, which signals the control loop to tear the client down.
async fn writer_task(
    mut tx: SplitSink<WebSocket, Message>,
    mut rx_out: mpsc::UnboundedReceiver<OutMsg>,
    timeout: Option<Duration>,
) -> Result<()> {
    while let Some(OutMsg::Resp(resp)) = rx_out.next().await {
        reply(&mut tx, &resp, timeout).await?;
    }
    // The control loop dropped tx_out: flush and close the socket cleanly.
    let _ = tx.close().await;
    Ok(())
}

// Buffered counterpart of [handle_client]. Identical request handling, but all
// websocket writes are offloaded to a dedicated [writer_task] via an unbounded
// channel so the control loop never blocks on a slow client. This keeps the
// bounded subscriber update channel (rx_up) drained promptly, so a slow client
// can no longer stall update delivery to the other clients that share this
// proxy's subscriber connection.
async fn handle_client_buffered(
    publisher: Publisher,
    subscriber: Subscriber,
    ws: WebSocket,
    timeout: Option<Duration>,
) -> Result<()> {
    static UPDATES: LazyLock<Pool<Vec<Update>>> = LazyLock::new(|| Pool::new(50, 10000));
    let (tx_up, mut rx_up) = mpsc::channel::<GPooled<Vec<(SubId, Event)>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    let (tx_ws, rx_ws) = ws.split();
    let (tx_out, rx_out) = mpsc::unbounded::<OutMsg>();
    let mut writer = tokio::spawn(writer_task(tx_ws, rx_out, timeout));
    let mut queued: Vec<result::Result<Message, warp::Error>> = Vec::new();
    let mut rx_ws = Batched::new(rx_ws.fuse(), 10_000);
    let mut calls_pending: FuturesUnordered<PendingCall> = FuturesUnordered::new();
    calls_pending.push(Box::pin(async { future::pending().await }) as PendingCall);
    loop {
        select_biased! {
            // If the writer task exits (client hit its send timeout, or the
            // socket errored), tear down so the ClientCtx drop unsubscribes
            // from the shared subscriber connection.
            j = (&mut writer).fuse() => break match j {
                Ok(r) => r,
                Err(e) => Err(anyhow!("writer task failed: {}", e)),
            },
            (id, res) = calls_pending.select_next_some() => match res {
                Ok(result) => {
                    send_buffered(&tx_out, Response::CallSuccess { id, result })?
                }
                Err(e) => {
                    let error = format!("rpc call failed {}", e);
                    send_buffered(&tx_out, Response::CallFailed { id, error })?
                }
            },
            r = rx_ws.select_next_some() => match r {
                BatchItem::InBatch(r) => queued.push(r),
                BatchItem::EndBatch => {
                    ctx.process_from_client_buffered(
                        &tx_out,
                        &mut queued,
                        &mut calls_pending,
                        timeout
                    ).await?
                }
            },
            mut batch = rx_up.select_next_some() => {
                let mut updates = UPDATES.take();
                for (id, event) in batch.drain(..) {
                    updates.push(Update {id, event});
                }
                send_buffered(&tx_out, Response::Update { updates })?
            },
        }
    }
}

/// If you want to integrate the netidx api server into your own warp project
/// this will return the filter path will be the http path where the websocket
/// lives
pub fn filter(
    publisher: Publisher,
    subscriber: Subscriber,
    path: &'static str,
    timeout: Option<Duration>,
) -> BoxedFilter<(impl Reply,)> {
    filter_opts(publisher, subscriber, path, timeout, false)
}

/// Like [filter], but with an explicit `per_client_buffer` option. When
/// `per_client_buffer` is true each client gets a dedicated writer task fed by
/// an unbounded buffer, so a single slow client cannot stall updates to the
/// other clients sharing this proxy's subscriber. When false the behavior is
/// identical to [filter].
pub fn filter_opts(
    publisher: Publisher,
    subscriber: Subscriber,
    path: &'static str,
    timeout: Option<Duration>,
    per_client_buffer: bool,
) -> BoxedFilter<(impl Reply,)> {
    warp::path(path)
        .and(warp::ws())
        .map(move |ws: Ws| {
            let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
            ws.on_upgrade(move |ws| {
                let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
                async move {
                    let r = if per_client_buffer {
                        handle_client_buffered(publisher, subscriber, ws, timeout).await
                    } else {
                        handle_client(publisher, subscriber, ws, timeout).await
                    };
                    if let Err(e) = r {
                        warn!("client handler exited: {}", e)
                    }
                }
            })
        })
        .boxed()
}

/// If you want to embed the websocket api in your own process, but you don't
/// want to serve any other warp filters then you can just call this in a task.
/// This will not return unless the server crashes, you should
/// probably run it in a task.
pub async fn run(
    config: config::Config,
    publisher: Publisher,
    subscriber: Subscriber,
    timeout: Option<Duration>,
) -> Result<()> {
    let routes =
        filter_opts(publisher, subscriber, "ws", timeout, config.per_client_buffer);
    match (&config.cert, &config.key) {
        (_, None) | (None, _) => {
            warp::serve(routes).run(config.listen.parse::<SocketAddr>()?).await
        }
        (Some(cert), Some(key)) => {
            warp::serve(routes)
                .tls()
                .cert_path(cert)
                .key_path(key)
                .run(config.listen.parse::<SocketAddr>()?)
                .await
        }
    }
    Ok(())
}
