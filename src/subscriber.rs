pub use crate::protocol::publisher::v1::Value;
use crate::{
    os::{self, ClientCtx, Krb5Ctx},
    channel::{Channel, ReadChannel, WriteChannel},
    chars::Chars,
    config,
    path::Path,
    protocol::{
        self,
        publisher::v1::{From, Id, To},
        resolver::v1::{Resolved, ResolverId},
    },
    resolver::{Auth, ResolverRead},
    utils::{self, BatchItem, Batched},
};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{
    // CR estokes: switch to tokio when it has is_closed.
    channel::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    prelude::*,
    select,
    select_biased,
};
use fxhash::FxBuildHasher;
use log::info;
use parking_lot::Mutex;
use rand::Rng;
use std::{
    cmp::{max, Eq, PartialEq},
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    iter, mem,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
    vec::Drain,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc::error::SendTimeoutError, oneshot},
    task,
    time::{self, Delay, Instant},
};

const BATCH: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubId(u64);

impl SubId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        SubId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug)]
struct SubscribeValRequest {
    path: Path,
    token: Bytes,
    resolver: ResolverId,
    finished: oneshot::Sender<Result<Val>>,
    con: UnboundedSender<ToCon>,
    deadline: Option<Instant>,
}

#[derive(Debug)]
enum ToCon {
    Subscribe(SubscribeValRequest),
    Unsubscribe(Id),
    Last(Id, oneshot::Sender<Value>),
    Stream { id: Id, sub_id: SubId, tx: Sender<Batch>, last: bool },
}

#[derive(Debug)]
struct ValInner {
    sub_id: SubId,
    id: Id,
    addr: SocketAddr,
    connection: UnboundedSender<ToCon>,
}

impl Drop for ValInner {
    fn drop(&mut self) {
        let _ = self.connection.send(ToCon::Unsubscribe(self.id));
    }
}

#[derive(Debug, Clone)]
pub struct ValWeak(Weak<ValInner>);

impl ValWeak {
    pub fn upgrade(&self) -> Option<Val> {
        Weak::upgrade(&self.0).map(|r| Val(r))
    }
}

/// A UVal is an untyped subscription to a value. A value has a
/// current value, and a stream of updates. The current value is
/// accessed by `UVal::last`, which will be available as long as the
/// subscription is alive. The stream of updates is accessed with the
/// `UVal::updates` function.
#[derive(Debug, Clone)]
pub struct Val(Arc<ValInner>);

impl Val {
    pub fn downgrade(&self) -> ValWeak {
        ValWeak(Arc::downgrade(&self.0))
    }

    /// Get the last published value, or None if the subscription is
    /// dead.
    pub async fn last(&self) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.connection.unbounded_send(ToCon::Last(self.0.id, tx));
        match rx.await {
            Ok(b) => Some(b),
            Err(_) => None,
        }
    }

    /// Get a stream of published values. Values will arrive in the
    /// order they are published. No value will be omitted. If
    /// `begin_with_last` is true, then the stream will start with the
    /// last published value at the time `updates` is called, and will
    /// then receive any updated values. Otherwise the stream will
    /// only receive new values.
    ///
    /// If the subscription dies the stream will end.
    pub fn updates(&self, begin_with_last: bool, tx: Sender<Batch>) {
        let m = ToCon::Stream {
            tx,
            sub_id: self.0.sub_id,
            last: begin_with_last,
            id: self.0.id,
        };
        let _ = self.0.connection.unbounded_send(m);
    }

    pub fn id(&self) -> SubId {
        self.0.sub_id
    }
}

#[derive(Debug, Copy, Clone)]
pub enum DVState {
    Subscribed,
    Unsubscribed,
}

#[derive(Debug)]
struct DValInner {
    sub_id: SubId,
    sub: Option<Val>,
    streams: Vec<Sender<Batch>>,
    states: Vec<UnboundedSender<(SubId, DVState)>>,
    tries: usize,
    next_try: Instant,
}

#[derive(Debug, Clone)]
struct DValWeak(Weak<Mutex<DValInner>>);

impl DValWeak {
    fn upgrade(&self) -> Option<DVal> {
        Weak::upgrade(&self.0).map(|s| DVal(s))
    }
}

/// `DUVal` is a durable value subscription. it behaves just like
/// `UVal`, except that if it dies a task within subscriber will
/// attempt to resubscribe. The resubscription process goes through
/// the entire resolution and connection process again, so `DUVal` is
/// robust to many failures. For example,
///
/// - multiple publishers are publishing on a path and one of them dies.
///   `DUVal` will transparently move to another one.
///
/// - a publisher is restarted (possibly on a different machine).
///   Since `DUVal` uses linear backoff to avoid saturating the
///   resolver, and the network, but assuming the publisher is restarted
///   quickly, resubscription will happen almost immediatly.
///
/// - The resolver server cluster is restarted. In this case existing
///   subscriptions won't die, but new ones will fail if the new
///   cluster is missing data. However even in this very bad case the
///   publishers will notice during their heartbeat (default every 10
///   minutes) that the resolver server is missing their data, and
///   they will republish it. If the resolver is restarted quickly,
///   then in the worst case all the data is back in 10 minutes, and
///   all DUVals waiting to subscribe to missing data will retry and
///   succeed 35 seconds after that.
///
/// A `DUVal` uses more memory than a `UVal` subscription, but other
/// than that the performance is the same. It is therefore recommended
/// that `DUVal` and `DVal` be considered the default value
/// subscription type where semantics allow.
#[derive(Debug, Clone)]
pub struct DVal(Arc<Mutex<DValInner>>);

impl DVal {
    fn downgrade(&self) -> DValWeak {
        DValWeak(Arc::downgrade(&self.0))
    }

    /// Get the last value published by the publisher, or None if the
    /// subscription is currently dead.
    pub async fn last(&self) -> Option<Value> {
        let sub = self.0.lock().sub.clone();
        match sub {
            None => None,
            Some(sub) => sub.last().await,
        }
    }

    /// Return a stream that produces a value when the state of the
    /// subscription changes. if include_current is true, then the
    /// current state will be immediatly emitted once even if there
    /// was no state change.
    pub fn state_updates(
        &self,
        include_current: bool,
        tx: UnboundedSender<(SubId, DVState)>,
    ) {
        let mut t = self.0.lock();
        t.states.retain(|c| !c.is_closed());
        if include_current {
            let current = match t.sub {
                None => DVState::Unsubscribed,
                Some(_) => DVState::Subscribed,
            };
            let _ = tx.unbounded_send((t.sub_id, current));
        }
        t.states.push(tx);
    }

    pub fn state(&self) -> DVState {
        if self.0.lock().sub.is_none() {
            DVState::Unsubscribed
        } else {
            DVState::Subscribed
        }
    }

    /// Gets a stream of updates just like `UVal::updates` except that
    /// the stream will not end when the subscription dies, it will
    /// just stop producing values, and will start again if
    /// resubscription is successful.
    pub fn updates(&self, begin_with_last: bool, tx: mpsc::Sender<Batch>) {
        let mut t = self.0.lock();
        t.streams.retain(|c| !c.is_closed());
        t.streams.push(tx.clone());
        if let Some(ref sub) = t.sub {
            let m = ToCon::Stream {
                tx,
                sub_id: t.sub_id,
                last: begin_with_last,
                id: sub.0.id,
            };
            let _ = sub.0.connection.unbounded_send(m);
        }
    }

    pub fn id(&self) -> SubId {
        self.0.lock().sub_id
    }
}

enum SubStatus {
    Subscribed(ValWeak),
    Pending(Vec<oneshot::Sender<Result<Val>>>),
}

struct SubscriberInner {
    resolver: ResolverRead,
    connections: HashMap<SocketAddr, UnboundedSender<ToCon>, FxBuildHasher>,
    subscribed: HashMap<Path, SubStatus>,
    durable_dead: HashMap<Path, DValWeak>,
    durable_alive: HashMap<Path, DValWeak>,
    trigger_resub: UnboundedSender<()>,
    desired_auth: Auth,
}

struct SubscriberWeak(Weak<Mutex<SubscriberInner>>);

impl SubscriberWeak {
    fn upgrade(&self) -> Option<Subscriber> {
        Weak::upgrade(&self.0).map(|s| Subscriber(s))
    }
}

#[derive(Clone)]
pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    pub fn new(
        resolver: config::resolver::Config,
        desired_auth: Auth,
    ) -> Result<Subscriber> {
        let (tx, rx) = mpsc::unbounded();
        let resolver = ResolverRead::new(resolver, desired_auth.clone())?;
        let t = Subscriber(Arc::new(Mutex::new(SubscriberInner {
            resolver,
            desired_auth,
            connections: HashMap::with_hasher(FxBuildHasher::default()),
            subscribed: HashMap::new(),
            durable_dead: HashMap::new(),
            durable_alive: HashMap::new(),
            trigger_resub: tx,
        })));
        t.start_resub_task(rx);
        Ok(t)
    }

    fn downgrade(&self) -> SubscriberWeak {
        SubscriberWeak(Arc::downgrade(&self.0))
    }

    fn start_resub_task(&self, incoming: UnboundedReceiver<()>) {
        async fn wait_retry(retry: &mut Option<Delay>) {
            match retry {
                None => future::pending().await,
                Some(d) => d.await,
            }
        }
        fn update_retry(subscriber: &mut SubscriberInner, retry: &mut Option<Delay>) {
            *retry = subscriber
                .durable_dead
                .values()
                .filter_map(|w| w.upgrade())
                .map(|ds| ds.0.lock().next_try)
                .fold(None, |min, v| match min {
                    None => Some(v),
                    Some(min) => {
                        if v < min {
                            Some(v)
                        } else {
                            Some(min)
                        }
                    }
                })
                .map(|t| time::delay_until(t + Duration::from_secs(1)));
        }
        async fn do_resub(subscriber: &SubscriberWeak, retry: &mut Option<Delay>) {
            if let Some(subscriber) = subscriber.upgrade() {
                let now = Instant::now();
                let (mut batch, timeout) = {
                    let mut b = HashMap::new();
                    let mut gc = Vec::new();
                    let mut subscriber = subscriber.0.lock();
                    let mut max_tries = 0;
                    for (p, w) in &subscriber.durable_dead {
                        match w.upgrade() {
                            None => {
                                gc.push(p.clone());
                            }
                            Some(s) => {
                                let (next_try, tries) = {
                                    let s = s.0.lock();
                                    (s.next_try, s.tries)
                                };
                                if next_try <= now {
                                    b.insert(p.clone(), s);
                                    max_tries = max(max_tries, tries);
                                }
                            }
                        }
                    }
                    for p in gc {
                        subscriber.durable_dead.remove(&p);
                    }
                    (b, Duration::from_secs(10 + max_tries as u64))
                };
                if batch.len() == 0 {
                    let mut subscriber = subscriber.0.lock();
                    update_retry(&mut *subscriber, retry);
                } else {
                    let r = subscriber
                        .subscribe_vals(batch.keys().cloned(), Some(timeout))
                        .await;
                    let mut subscriber = subscriber.0.lock();
                    let now = Instant::now();
                    for (p, r) in r {
                        let mut ds = batch.get_mut(&p).unwrap().0.lock();
                        match r {
                            Err(_) => {
                                // CR estokes: log this error?
                                ds.tries += 1;
                                ds.next_try = now + Duration::from_secs(ds.tries as u64);
                            }
                            Ok(sub) => {
                                ds.tries = 0;
                                let mut i = 0;
                                while i < ds.states.len() {
                                    match ds.states[i]
                                        .unbounded_send((ds.sub_id, DVState::Subscribed))
                                    {
                                        Ok(()) => {
                                            i += 1;
                                        }
                                        Err(_) => {
                                            ds.states.remove(i);
                                        }
                                    }
                                }
                                ds.streams.retain(|c| !c.is_closed());
                                for tx in ds.streams.iter().cloned() {
                                    let _ =
                                        sub.0.connection.unbounded_send(ToCon::Stream {
                                            tx,
                                            sub_id: ds.sub_id,
                                            last: true,
                                            id: sub.0.id,
                                        });
                                }
                                ds.sub = Some(sub);
                                let w = subscriber.durable_dead.remove(&p).unwrap();
                                subscriber.durable_alive.insert(p.clone(), w.clone());
                            }
                        }
                    }
                    update_retry(&mut *subscriber, retry);
                }
            }
        }
        let subscriber = self.downgrade();
        task::spawn(async move {
            let mut incoming = Batched::new(incoming, 100_000);
            let mut retry: Option<Delay> = None;
            loop {
                select! {
                    _ = wait_retry(&mut retry).fuse() => {
                        do_resub(&subscriber, &mut retry).await;
                    },
                    m = incoming.next() => match m {
                        None => break,
                        Some(BatchItem::InBatch(())) => (),
                        Some(BatchItem::EndBatch) => {
                            do_resub(&subscriber, &mut retry).await;
                        }
                    },
                }
            }
        });
    }

    /// Subscribe to the specified set of values.
    ///
    /// Path resolution and subscription are done in parallel, so the
    /// lowest latency per subscription will be achieved with larger
    /// batches.
    ///
    /// In case you are already subscribed to one or more of the paths
    /// in the batch, you will receive a reference to the existing
    /// subscription, no additional messages will be sent.
    ///
    /// It is safe to call this function concurrently with the same or
    /// overlapping sets of paths in the batch, only one subscription
    /// attempt will be made concurrently, and the result of that one
    /// attempt will be given to each concurrent caller upon success
    /// or failure.
    ///
    /// The timeout, if specified, will apply to each subscription
    /// individually. Any subscription that does not complete
    /// successfully before the specified timeout will result in an
    /// error, but that error will not effect other subscriptions in
    /// the batch, which may complete successfully. If you need all or
    /// nothing behavior, specify None for timeout and wrap the
    /// `subscribe_vals_ut` future in a `time::timeout`. In the case
    /// timeout is specified, `subscribe_vals_ut` is guaranteed to
    /// complete no later than `now + timeout`.
    pub async fn subscribe_vals(
        &self,
        batch: impl IntoIterator<Item = Path>,
        timeout: Option<Duration>,
    ) -> Vec<(Path, Result<Val>)> {
        enum St {
            Resolve,
            Subscribing(oneshot::Receiver<Result<Val>>),
            WaitingOther(oneshot::Receiver<Result<Val>>),
            Subscribed(Val),
            Error(Error),
        }
        let now = Instant::now();
        let paths = batch.into_iter().collect::<Vec<_>>();
        let mut pending: HashMap<Path, St> = HashMap::new();
        let r = {
            // Init
            let mut t = self.0.lock();
            for p in paths.clone() {
                match t.subscribed.entry(p.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(SubStatus::Pending(vec![]));
                        pending.insert(p, St::Resolve);
                    }
                    Entry::Occupied(mut e) => match e.get_mut() {
                        SubStatus::Pending(ref mut v) => {
                            let (tx, rx) = oneshot::channel();
                            v.push(tx);
                            pending.insert(p, St::WaitingOther(rx));
                        }
                        SubStatus::Subscribed(r) => match r.upgrade() {
                            Some(r) => {
                                pending.insert(p, St::Subscribed(r));
                            }
                            None => {
                                e.insert(SubStatus::Pending(vec![]));
                                pending.insert(p, St::Resolve);
                            }
                        },
                    },
                }
            }
            t.resolver.clone()
        };
        fn pick(n: usize) -> usize {
            let mut rng = rand::thread_rng();
            rng.gen_range(0, n)
        }
        {
            // Resolve, Connect, Subscribe
            let to_resolve = pending
                .iter()
                .filter(|(_, s)| match s {
                    St::Resolve => true,
                    _ => false,
                })
                .map(|(p, _)| p.clone())
                .collect::<Vec<_>>();
            let r = match timeout {
                None => Ok(r.resolve(to_resolve.clone()).await),
                Some(d) => time::timeout(d, r.resolve(to_resolve.clone())).await,
            };
            match r {
                Err(_) => {
                    for p in to_resolve {
                        pending.insert(
                            p.clone(),
                            St::Error(anyhow!(
                                "resolving path: {} failed: request timed out",
                                p
                            )),
                        );
                    }
                }
                Ok(Err(e)) => {
                    for p in to_resolve {
                        pending.insert(
                            p.clone(),
                            St::Error(anyhow!("resolving path: {} failed: {}", p, e)),
                        );
                    }
                }
                Ok(Ok(Resolved { addrs, resolver, krb5_spns })) => {
                    let mut t = self.0.lock();
                    let deadline = timeout.map(|t| now + t);
                    let desired_auth = t.desired_auth.clone();
                    for (p, addrs) in to_resolve.into_iter().zip(addrs.into_iter()) {
                        if addrs.len() == 0 {
                            pending.insert(p, St::Error(anyhow!("path not found")));
                        } else {
                            let addr = {
                                if addrs.len() == 1 {
                                    addrs[0].clone()
                                } else {
                                    addrs[pick(addrs.len())].clone()
                                }
                            };
                            let con = t.connections.entry(addr.0).or_insert_with(|| {
                                let (tx, rx) = mpsc::unbounded();
                                let target_spn = match krb5_spns.get(&addr.0) {
                                    None => Chars::new(),
                                    Some(p) => p.clone(),
                                };
                                task::spawn(connection(
                                    self.downgrade(),
                                    addr.0,
                                    target_spn,
                                    rx,
                                    desired_auth.clone(),
                                ));
                                tx
                            });
                            let (tx, rx) = oneshot::channel();
                            let con_ = con.clone();
                            let r = con.unbounded_send(ToCon::Subscribe(
                                SubscribeValRequest {
                                    path: p.clone(),
                                    token: addr.1,
                                    resolver,
                                    finished: tx,
                                    con: con_,
                                    deadline,
                                },
                            ));
                            match r {
                                Ok(()) => {
                                    pending.insert(p, St::Subscribing(rx));
                                }
                                Err(e) => {
                                    pending.insert(p, St::Error(Error::from(e)));
                                }
                            }
                        }
                    }
                }
            }
        }
        // Wait
        for (path, st) in pending.iter_mut() {
            match st {
                St::Resolve => unreachable!(),
                St::Subscribed(_) => (),
                St::Error(e) => {
                    let mut t = self.0.lock();
                    if let Some(sub) = t.subscribed.remove(path.as_ref()) {
                        match sub {
                            SubStatus::Subscribed(_) => unreachable!(),
                            SubStatus::Pending(waiters) => {
                                for w in waiters {
                                    let err = Err(anyhow!("{}", e));
                                    let _ = w.send(err);
                                }
                            }
                        }
                    }
                }
                St::WaitingOther(w) => match w.await {
                    Err(_) => *st = St::Error(anyhow!("other side died")),
                    Ok(Err(e)) => *st = St::Error(e),
                    Ok(Ok(raw)) => *st = St::Subscribed(raw),
                },
                St::Subscribing(w) => {
                    let res = match w.await {
                        Err(_) => Err(anyhow!("connection died")),
                        Ok(Err(e)) => Err(e),
                        Ok(Ok(raw)) => Ok(raw),
                    };
                    let mut t = self.0.lock();
                    match t.subscribed.entry(path.clone()) {
                        Entry::Vacant(_) => unreachable!(),
                        Entry::Occupied(mut e) => match res {
                            Err(err) => match e.remove() {
                                SubStatus::Subscribed(_) => unreachable!(),
                                SubStatus::Pending(waiters) => {
                                    for w in waiters {
                                        let err = Err(anyhow!("{}", err));
                                        let _ = w.send(err);
                                    }
                                    *st = St::Error(err);
                                }
                            },
                            Ok(raw) => {
                                let s = mem::replace(
                                    e.get_mut(),
                                    SubStatus::Subscribed(raw.downgrade()),
                                );
                                match s {
                                    SubStatus::Subscribed(_) => unreachable!(),
                                    SubStatus::Pending(waiters) => {
                                        for w in waiters {
                                            let _ = w.send(Ok(raw.clone()));
                                        }
                                        *st = St::Subscribed(raw);
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
        paths
            .into_iter()
            .map(|p| match pending.remove(&p).unwrap() {
                St::Resolve | St::Subscribing(_) | St::WaitingOther(_) => unreachable!(),
                St::Subscribed(raw) => (p, Ok(raw)),
                St::Error(e) => (p, Err(e)),
            })
            .collect()
    }

    /// Subscribe to one value. This is sufficient for a small number
    /// of paths, but if you need to subscribe to a lot of values it
    /// is more efficent to use `subscribe_vals_ut`
    pub async fn subscribe_val(
        &self,
        path: Path,
        timeout: Option<Duration>,
    ) -> Result<Val> {
        self.subscribe_vals(iter::once(path), timeout).await.pop().unwrap().1
    }

    /// Create a durable value subscription to `path`.
    ///
    /// Batching of durable subscriptions is automatic, if you create
    /// a lot of durable subscriptions all at once they will batch,
    /// minimizing the number of messages exchanged with both the
    /// resolver server and the publishers.
    ///
    /// As with regular subscriptions there is only ever one
    /// subscription for a given path, calling
    /// `subscribe_val_durable_ut` again for the same path will just
    /// return another pointer to it.
    pub fn durable_subscribe_val(&self, path: Path) -> DVal {
        let mut t = self.0.lock();
        if let Some(s) = t.durable_dead.get(&path).or_else(|| t.durable_alive.get(&path))
        {
            if let Some(s) = s.upgrade() {
                return s;
            }
        }
        let s = DVal(Arc::new(Mutex::new(DValInner {
            sub_id: SubId::new(),
            sub: None,
            streams: Vec::new(),
            states: Vec::new(),
            tries: 0,
            next_try: Instant::now(),
        })));
        t.durable_dead.insert(path, s.downgrade());
        let _ = t.trigger_resub.unbounded_send(());
        s
    }
}

#[derive(Clone)]
struct ChanWrap(Sender<Batch>);

impl PartialEq for ChanWrap {
    fn eq(&self, other: &ChanWrap) -> bool {
        self.0.same_receiver(&other.0)
    }
}

impl Eq for ChanWrap {}

impl Hash for ChanWrap {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash_receiver(state)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChanId(u64);

impl ChanId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        ChanId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

struct Sub {
    path: Path,
    streams: Vec<(SubId, ChanId, Sender<Batch>)>,
    last: Value,
}

fn unsubscribe(subscriber: &mut SubscriberInner, sub: Sub, id: Id, addr: SocketAddr) {
    if let Some(dsw) = subscriber.durable_alive.remove(&sub.path) {
        if let Some(ds) = dsw.upgrade() {
            let mut inner = ds.0.lock();
            inner.sub = None;
            let mut i = 0;
            while i < inner.states.len() {
                match inner.states[i]
                    .unbounded_send((inner.sub_id, DVState::Unsubscribed))
                {
                    Ok(()) => {
                        i += 1;
                    }
                    Err(_) => {
                        inner.states.remove(i);
                    }
                }
            }
            subscriber.durable_dead.insert(sub.path.clone(), dsw);
            let _ = subscriber.trigger_resub.unbounded_send(());
        }
    }
    match subscriber.subscribed.entry(sub.path) {
        Entry::Vacant(_) => (),
        Entry::Occupied(e) => match e.get() {
            SubStatus::Pending(_) => (),
            SubStatus::Subscribed(s) => match s.upgrade() {
                None => {
                    e.remove();
                }
                Some(s) => {
                    if s.0.id == id && s.0.addr == addr {
                        e.remove();
                    }
                }
            },
        },
    }
}

async fn hello_publisher(
    con: &mut Channel<ClientCtx>,
    auth: &Auth,
    target_spn: &Chars,
) -> Result<()> {
    use crate::protocol::publisher::v1::Hello;
    // negotiate protocol version
    con.send_one(&1u64).await?;
    let _ver: u64 = con.receive().await?;
    match auth {
        Auth::Anonymous => {
            con.send_one(&Hello::Anonymous).await?;
            let reply: Hello = con.receive().await?;
            match reply {
                Hello::Anonymous => (),
                _ => bail!("unexpected response from publisher"),
            }
        }
        Auth::Krb5 { upn, .. } => {
            let p = upn.as_ref().map(|p| p.as_bytes());
            let ctx = os::create_client_ctx(p, target_spn.as_bytes())?;
            let tok = ctx
                .step(None)?
                .map(|b| utils::bytes(&*b))
                .ok_or_else(|| anyhow!("expected step to generate a token"))?;
            con.send_one(&Hello::Token(tok)).await?;
            match con.receive().await? {
                Hello::Anonymous => bail!("publisher failed mutual authentication"),
                Hello::ResolverAuthenticate(_, _) => bail!("protocol error"),
                Hello::Token(tok) => {
                    if ctx.step(Some(&*tok))?.is_some() {
                        bail!("unexpected second token from step");
                    }
                }
            }
            con.set_ctx(ctx.clone()).await;
        }
    }
    Ok(())
}

const PERIOD: Duration = Duration::from_secs(10);
const FLUSH: Duration = Duration::from_secs(1);

lazy_static! {
    static ref BATCHES: Mutex<Vec<Vec<(SubId, Value)>>> = Mutex::new(Vec::new());
}

#[derive(Debug)]
pub struct Batch(Vec<(SubId, Value)>);

impl Drop for Batch {
    fn drop(&mut self) {
        let mut batches = BATCHES.lock();
        if batches.len() < 1000 {
            batches.push(mem::replace(&mut self.0, Vec::new()));
        }
    }
}

impl Batch {
    fn new() -> Self {
        let v = BATCHES.lock().pop().unwrap_or_else(Vec::new);
        Batch(v)
    }

    fn push(&mut self, v: (SubId, Value)) {
        self.0.push(v);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn consume<'a>(&'a mut self) -> Drain<'a, (SubId, Value)> {
        self.0.drain(..)
    }
}

// This is the fast path for the common case where the batch contains
// only updates. As of 2020-04-30, sending to an mpsc channel is
// pretty slow, about 250ns, so we go to great lengths to avoid it.
async fn process_updates_batch(
    by_chan: &mut HashMap<ChanId, (Sender<Batch>, Batch), FxBuildHasher>,
    batch: &mut Vec<protocol::publisher::v1::From>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
) {
    for m in batch.drain(..) {
        if let From::Update(i, m) = m {
            if let Some(sub) = subscriptions.get_mut(&i) {
                for (sub_id, chan_id, c) in sub.streams.iter() {
                    by_chan
                        .entry(*chan_id)
                        .or_insert_with(|| (c.clone(), Batch::new()))
                        .1
                        .push((*sub_id, m.clone()))
                }
                sub.last = m;
            }
        }
    }
    for (_, (mut c, batch)) in by_chan.drain() {
        let _ = c.send(batch).await;
    }
}

async fn process_batch(
    batch: &mut Vec<protocol::publisher::v1::From>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
    pending: &mut HashMap<Path, SubscribeValRequest>,
    con: &mut WriteChannel<ClientCtx>,
    subscriber: &Subscriber,
    addr: SocketAddr,
) -> Result<()> {
    for m in batch.drain(..) {
        match m {
            From::Update(i, m) => match subscriptions.get_mut(&i) {
                Some(sub) => {
                    for (id, _, c) in sub.streams.iter_mut() {
                        let mut b = Batch::new();
                        b.push((*id, m.clone()));
                        let _ = c.send(b).await;
                    }
                    sub.last = m;
                }
                None => con.queue_send(&To::Unsubscribe(i))?,
            },
            From::Heartbeat => (),
            From::NoSuchValue(path) => {
                if let Some(r) = pending.remove(&path) {
                    let _ = r.finished.send(Err(anyhow!("no such value")));
                }
            }
            From::Denied(path) => {
                if let Some(r) = pending.remove(&path) {
                    let _ = r.finished.send(Err(anyhow!("access denied")));
                }
            }
            From::Unsubscribed(id) => {
                if let Some(s) = subscriptions.remove(&id) {
                    let mut t = subscriber.0.lock();
                    unsubscribe(&mut *t, s, id, addr);
                }
            }
            From::Subscribed(p, id, m) => match pending.remove(&p) {
                None => con.queue_send(&To::Unsubscribe(id))?,
                Some(req) => {
                    let sub_id = SubId::new();
                    let s = Ok(Val(Arc::new(ValInner {
                        sub_id,
                        id,
                        addr,
                        connection: req.con,
                    })));
                    match req.finished.send(s) {
                        Err(_) => con.queue_send(&To::Unsubscribe(id))?,
                        Ok(()) => {
                            subscriptions.insert(
                                id,
                                Sub { path: req.path, last: m, streams: Vec::new() },
                            );
                        }
                    }
                }
            },
        }
    }
    Ok(())
}

async fn try_flush(con: &mut WriteChannel<ClientCtx>) -> Result<()> {
    if con.bytes_queued() > 0 {
        match con.flush_timeout(FLUSH).await {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Timeout(())) => Ok(()),
            Err(SendTimeoutError::Closed(())) => bail!("connection died"),
        }
    } else {
        Ok(())
    }
}

fn decode_task(
    mut con: ReadChannel<ClientCtx>,
    mut buf_return: UnboundedReceiver<Vec<From>>,
) -> Receiver<Result<(Vec<From>, bool)>> {
    let (mut send, recv) = mpsc::channel(10);
    task::spawn(async move {
        let mut bufs: Vec<Vec<From>> = Vec::new();
        let mut buf: Vec<From> = Vec::new();
        let r = loop {
            select_biased! {
                b = buf_return.next() => match b {
                    None => break Ok(()),
                    Some(b) => { bufs.push(b); }
                },
                r = con.receive_batch(&mut buf).fuse() => match r {
                    Err(e) => {
                        buf.clear();
                        try_cf!(send.send(Err(e)).await)
                    }
                    Ok(()) => {
                        let next = bufs.pop().unwrap_or_else(Vec::new);
                        let batch = mem::replace(&mut buf, next);
                        let only_updates = batch.iter().all(|v| match v {
                            From::Update(_, _) => true,
                            _ => false
                        });
                        try_cf!(send.send(Ok((batch, only_updates))).await)
                    }
                }
            }
        };
        info!("decode task shutting down {:?}", r);
    });
    recv
}

async fn connection(
    subscriber: SubscriberWeak,
    addr: SocketAddr,
    target_spn: Chars,
    from_sub: UnboundedReceiver<ToCon>,
    auth: Auth,
) -> Result<()> {
    let mut pending: HashMap<Path, SubscribeValRequest> = HashMap::new();
    let mut subscriptions: HashMap<Id, Sub, FxBuildHasher> =
        HashMap::with_hasher(FxBuildHasher::default());
    let mut idle: usize = 0;
    let mut msg_recvd = false;
    let mut from_sub = Batched::new(from_sub, BATCH);
    let mut con = Channel::new(time::timeout(PERIOD, TcpStream::connect(addr)).await??);
    hello_publisher(&mut con, &auth, &target_spn).await?;
    let (read_con, mut write_con) = con.split();
    let (return_batch, read_returned) = mpsc::unbounded();
    let mut batches = decode_task(read_con, read_returned);
    let mut periodic = time::interval_at(Instant::now() + PERIOD, PERIOD).fuse();
    let mut by_receiver: HashMap<ChanWrap, ChanId> = HashMap::new();
    let mut by_chan: HashMap<ChanId, (Sender<Batch>, Batch), FxBuildHasher> =
        HashMap::with_hasher(FxBuildHasher::default());
    let res = 'main: loop {
        select_biased! {
            now = periodic.next() => if let Some(now) = now {
                if !msg_recvd {
                    break 'main Err(anyhow!("hung publisher"));
                } else {
                    msg_recvd = false;
                }
                if subscriptions.len() == 0 && pending.len() == 0 {
                    idle += 1;
                    if idle == 2 { break 'main Ok(()); }
                } else {
                    idle = 0;
                }
                let mut timed_out = Vec::new();
                for (path, req) in pending.iter() {
                    if let Some(deadline) = req.deadline {
                        if deadline < now {
                            timed_out.push(path.clone());
                        }
                    }
                }
                for path in timed_out {
                    if let Some(req) = pending.remove(&path) {
                        let _ = req.finished.send(Err(anyhow!("timed out")));
                    }
                }
                try_cf!(try_flush(&mut write_con).await)
            },
            r = batches.next() => match r {
                Some(Ok((mut batch, true))) => {
                    msg_recvd = true;
                    process_updates_batch(
                        &mut by_chan,
                        &mut batch,
                        &mut subscriptions
                    ).await;
                    try_cf!(return_batch.unbounded_send(batch));
                    try_cf!(try_flush(&mut write_con).await)
                },
                Some(Ok((mut batch, false))) =>
                    if let Some(subscriber) = subscriber.upgrade() {
                        msg_recvd = true;
                        try_cf!(process_batch(
                            &mut batch,
                            &mut subscriptions,
                            &mut pending,
                            &mut write_con,
                            &subscriber,
                            addr).await);
                        try_cf!(return_batch.unbounded_send(batch));
                        try_cf!(try_flush(&mut write_con).await)
                    }
                Some(Err(e)) => break Err(Error::from(e)),
                None => break Err(anyhow!("EOF")),
            },
            msg = from_sub.next() => match msg {
                None => break Err(anyhow!("dropped")),
                Some(BatchItem::EndBatch) => {
                    try_cf!(try_flush(&mut write_con).await)
                }
                Some(BatchItem::InBatch(ToCon::Subscribe(req))) => {
                    let path = req.path.clone();
                    let resolver = req.resolver;
                    let token = req.token.clone();
                    pending.insert(path.clone(), req);
                    try_cf!(write_con.queue_send(&To::Subscribe {
                        path,
                        resolver,
                        token,
                    }))
                }
                Some(BatchItem::InBatch(ToCon::Unsubscribe(id))) => {
                    try_cf!(write_con.queue_send(&To::Unsubscribe(id)))
                }
                Some(BatchItem::InBatch(ToCon::Last(id, tx))) => {
                    if let Some(sub) = subscriptions.get(&id) {
                        let _ = tx.send(sub.last.clone());
                    }
                }
                Some(BatchItem::InBatch(ToCon::Stream { id, sub_id, mut tx, last })) => {
                    if let Some(sub) = subscriptions.get_mut(&id) {
                        sub.streams.retain(|(_, _, c)| {
                            if c.is_closed() {
                                by_receiver.remove(&ChanWrap(c.clone()));
                                false
                            } else {
                                true
                            }
                        });
                        if last {
                            let m = sub.last.clone();
                            let mut b = Batch::new();
                            b.push((sub_id, m));
                            match tx.send(b).await {
                                Err(_) => continue,
                                Ok(()) => ()
                            }
                        }
                        let id = by_receiver.entry(ChanWrap(tx.clone()))
                            .or_insert_with(ChanId::new);
                        sub.streams.push((sub_id, *id, tx));
                    }
                }
            },
        }
    };
    if let Some(subscriber) = subscriber.upgrade() {
        let mut t = subscriber.0.lock();
        t.connections.remove(&addr);
        for (id, sub) in subscriptions {
            unsubscribe(&mut *t, sub, id, addr);
        }
        for (_, req) in pending {
            let _ = req.finished.send(Err(anyhow!("connection died")));
        }
    }
    info!("connection to {:?} shutting down {:?}", addr, res);
    res
}
