//! Browser egress policy. Resolve once, validate every address, connect to that pinned IP.
use crate::{Result, ensure};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};
use url::{Host, Url};

#[derive(Clone)]
pub struct NetworkPolicy {
    pub fixture_origin: Option<Url>,
}
impl NetworkPolicy {
    pub fn new(fixture: Option<&str>) -> Result<Self> {
        let fixture_origin = fixture.map(Url::parse).transpose()?;
        if let Some(url) = &fixture_origin {
            ensure(
                url.scheme() == "http"
                    && url.host_str() == Some("127.0.0.1")
                    && url.port().is_some_and(|p| p > 0)
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.username().is_empty()
                    && url.password().is_none(),
                "invalid explicit browser fixture origin",
            )?;
        }
        Ok(Self { fixture_origin })
    }
    pub fn validate_url(&self, url: &Url) -> Result<()> {
        ensure(
            pulse_core::browser_url(&format!("browser:{url}")).is_some(),
            "browser URL rejected",
        )?;
        let fixture = self
            .fixture_origin
            .as_ref()
            .is_some_and(|f| f.origin() == url.origin());
        if !fixture {
            ensure(
                matches!(url.port_or_known_default(), Some(80 | 443)),
                "public browser ports must be 80 or 443",
            )?;
            if let Some(host) = url.host() {
                match host {
                    Host::Ipv4(ip) => ensure(public_ip(IpAddr::V4(ip)), "private browser address")?,
                    Host::Ipv6(ip) => ensure(public_ip(IpAddr::V6(ip)), "private browser address")?,
                    Host::Domain(name) => ensure(
                        name.trim_end_matches('.') != "localhost"
                            && !name.trim_end_matches('.').ends_with(".localhost")
                            && !name.trim_end_matches('.').ends_with(".local"),
                        "private browser hostname",
                    )?,
                }
            }
        }
        Ok(())
    }
    async fn addresses(&self, url: &Url) -> Result<Vec<SocketAddr>> {
        self.validate_url(url)?;
        let host = url
            .host_str()
            .ok_or_else(|| crate::Error::Rejected("browser host required".into()))?;
        let host = host.trim_matches(['[', ']']);
        let mut addresses: Vec<_> =
            tokio::net::lookup_host((host, url.port_or_known_default().unwrap()))
                .await?
                .take(17)
                .collect();
        ensure(
            !addresses.is_empty() && addresses.len() <= 16,
            "browser DNS result rejected",
        )?;
        let fixture = self
            .fixture_origin
            .as_ref()
            .is_some_and(|f| f.origin() == url.origin());
        ensure(
            addresses.iter().all(|a| {
                if fixture {
                    a.ip() == IpAddr::from([127, 0, 0, 1])
                } else {
                    public_ip(a.ip())
                }
            }),
            "browser DNS contains private address",
        )?;
        addresses.sort_by_key(|a| a.is_ipv6());
        Ok(addresses)
    }
}
pub fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(matches!(a, 0 | 10 | 127 | 224..=255)
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192
                    && (b == 168 || (b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(v4));
            }
            let s = ip.segments();
            s[0] & 0xe000 == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}
pub struct BrowserProxy {
    pub address: SocketAddr,
    task: JoinHandle<()>,
    grant: Arc<Mutex<Option<PostGrant>>>,
    policy: NetworkPolicy,
}
struct PostGrant {
    id: String,
    url: Url,
    body: Vec<u8>,
    until: Instant,
    forwarded: Arc<AtomicBool>,
}
pub(crate) struct PostPermit {
    id: String,
    grant: Arc<Mutex<Option<PostGrant>>>,
    forwarded: Arc<AtomicBool>,
}
impl PostPermit {
    pub fn forwarded(&self) -> bool {
        self.forwarded.load(Ordering::SeqCst)
    }
    pub async fn wait_forwarded(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !self.forwarded() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| crate::Error::Rejected("HTTP form transfer not verified".into()))
    }
}
impl Drop for PostPermit {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.grant.lock()
            && slot.as_ref().is_some_and(|g| g.id == self.id)
        {
            *slot = None;
        }
    }
}
impl Drop for BrowserProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl BrowserProxy {
    pub async fn start(policy: NetworkPolicy) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let grant = Arc::new(Mutex::new(None));
        let slot = grant.clone();
        let live_policy = policy.clone();
        let task = tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(16));
            let count = Arc::new(AtomicUsize::new(0));
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=> {
                        let Ok((mut stream,_))=accepted else {break};
                        let Ok(permit)=permits.clone().try_acquire_owned() else {continue};
                        if count.fetch_add(1,Ordering::Relaxed)>=128 {continue;}
                        let policy=policy.clone();
                        let grant=slot.clone();
                        tasks.spawn(async move {
                            let _permit=permit;
                            if tokio::time::timeout(Duration::from_secs(40),proxy(&mut stream,&policy,&grant)).await.is_err() {let _=stream.shutdown().await;}
                        });
                    }
                    _=tasks.join_next(),if !tasks.is_empty()=>{}
                }
            }
        });
        Ok(Self {
            address,
            task,
            grant,
            policy: live_policy,
        })
    }
    pub(crate) fn allow_post(&self, url: &Url, body: Vec<u8>) -> Result<PostPermit> {
        self.policy.validate_url(url)?;
        ensure(
            url.scheme() == "http" && body.len() <= 8192,
            "invalid HTTP form grant",
        )?;
        let mut url = url.clone();
        url.set_fragment(None);
        let mut slot = self
            .grant
            .lock()
            .map_err(|_| crate::Error::Rejected("proxy grant lock".into()))?;
        ensure(
            slot.as_ref().is_none_or(|g| g.until <= Instant::now()),
            "proxy grant already pending",
        )?;
        let id = uuid::Uuid::new_v4().to_string();
        let forwarded = Arc::new(AtomicBool::new(false));
        *slot = Some(PostGrant {
            id: id.clone(),
            url,
            body,
            until: Instant::now() + Duration::from_secs(10),
            forwarded: forwarded.clone(),
        });
        Ok(PostPermit {
            id,
            grant: self.grant.clone(),
            forwarded,
        })
    }
}
async fn proxy(
    stream: &mut TcpStream,
    policy: &NetworkPolicy,
    grant: &Arc<Mutex<Option<PostGrant>>>,
) -> Result<()> {
    let mut header = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let byte = stream.read_u8().await?;
            header.push(byte);
            ensure(header.len() <= 16 * 1024, "proxy header too large")?;
            if header.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        Ok::<_, crate::Error>(())
    })
    .await
    .map_err(|_| crate::Error::Rejected("proxy header timed out".into()))??;
    let header = std::str::from_utf8(&header)
        .map_err(|_| crate::Error::Rejected("invalid proxy header".into()))?;
    let mut lines = header.split("\r\n");
    let first = lines.next().unwrap_or("");
    let parts: Vec<_> = first.split(' ').collect();
    ensure(
        parts.len() == 3 && parts[2] == "HTTP/1.1",
        "invalid proxy request",
    )?;
    let connect = parts[0] == "CONNECT";
    let target = if connect {
        format!("https://{}/", parts[1])
    } else {
        parts[1].to_owned()
    };
    let url = Url::parse(&target)?;
    if !connect {
        ensure(
            matches!(parts[0], "GET" | "HEAD" | "POST") && url.scheme() == "http",
            "proxy supports read requests only",
        )?;
    }
    let mut body = Vec::new();
    let mut post = None;
    if parts[0] == "POST" {
        let fields: Vec<_> = header
            .split("\r\n")
            .skip(1)
            .filter_map(|l| l.split_once(':'))
            .collect();
        let lengths: Vec<_> = fields
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case("content-length"))
            .collect();
        ensure(
            lengths.len() == 1
                && !fields
                    .iter()
                    .any(|(n, _)| n.eq_ignore_ascii_case("transfer-encoding")),
            "form framing rejected",
        )?;
        let length: usize = lengths[0]
            .1
            .trim()
            .parse()
            .map_err(|_| crate::Error::Rejected("invalid form length".into()))?;
        ensure(
            length <= 8192
                && fields.iter().any(|(n, v)| {
                    n.eq_ignore_ascii_case("content-type")
                        && v.trim() == "application/x-www-form-urlencoded"
                }),
            "form body rejected",
        )?;
        body.resize(length, 0);
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
            .await
            .map_err(|_| crate::Error::Rejected("form body timeout".into()))??;
        let mut slot = grant
            .lock()
            .map_err(|_| crate::Error::Rejected("proxy grant lock".into()))?;
        ensure(
            slot.as_ref()
                .is_some_and(|g| g.url == url && g.body == body && g.until > Instant::now()),
            "no exact form grant",
        )?;
        post = slot.take();
    }
    let addresses = match policy.addresses(&url).await {
        Ok(a) => a,
        Err(e) => {
            let _ = stream
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
            return Err(e);
        }
    };
    let mut remote = None;
    for address in addresses.into_iter().take(4) {
        if let Ok(Ok(socket)) =
            tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(address)).await
        {
            remote = Some(socket);
            break;
        }
    }
    let mut remote =
        remote.ok_or_else(|| crate::Error::Rejected("browser upstream unavailable".into()))?;
    if connect {
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
    } else {
        let path = if let Some(q) = url.query() {
            format!("{}?{q}", url.path())
        } else {
            url.path().to_owned()
        };
        let mut forwarded = format!(
            "{} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            parts[0],
            &url[url::Position::BeforeHost..url::Position::AfterPort]
        );
        for line in lines.filter(|s| !s.is_empty()) {
            let (name, _) = line
                .split_once(':')
                .ok_or_else(|| crate::Error::Rejected("invalid proxy field".into()))?;
            ensure(
                (parts[0] == "POST" || !name.eq_ignore_ascii_case("content-length"))
                    && !name.eq_ignore_ascii_case("transfer-encoding"),
                "proxy read body rejected",
            )?;
            if ![
                "host",
                "connection",
                "proxy-connection",
                "proxy-authorization",
            ]
            .iter()
            .any(|n| name.eq_ignore_ascii_case(n))
            {
                forwarded.push_str(line);
                forwarded.push_str("\r\n");
            }
        }
        forwarded.push_str("\r\n");
        if let Some(grant) = &post {
            ensure(grant.until > Instant::now(), "form grant expired")?;
            grant.forwarded.store(true, Ordering::SeqCst);
        }
        remote.write_all(forwarded.as_bytes()).await?;
        remote.write_all(&body).await?;
    }
    if !connect {
        // A read has no request body. Never forward a second pipelined request.
        let mut response = remote.take(8 * 1024 * 1024);
        tokio::io::copy(&mut response, stream).await?;
        stream.shutdown().await?;
        return Ok(());
    }
    let (sr, mut sw) = stream.split();
    let (rr, mut rw) = remote.split();
    let mut sr = sr.take(8 * 1024 * 1024);
    let mut rr = rr.take(8 * 1024 * 1024);
    tokio::try_join!(
        async {
            let n = tokio::io::copy(&mut sr, &mut rw).await?;
            rw.shutdown().await?;
            Ok::<_, std::io::Error>(n)
        },
        async {
            let n = tokio::io::copy(&mut rr, &mut sw).await?;
            sw.shutdown().await?;
            Ok::<_, std::io::Error>(n)
        }
    )?;
    Ok(())
}

#[cfg(test)]
mod grant_tests {
    use super::*;
    use axum::{Router, routing::post};
    use std::future::IntoFuture;

    async fn send(proxy: &BrowserProxy, url: &Url, body: &str, framing: &str) -> Vec<u8> {
        let mut stream = TcpStream::connect(proxy.address).await.unwrap();
        let head = format!(
            "POST {url} HTTP/1.1\r\nHost: fixture\r\nContent-Type: application/x-www-form-urlencoded\r\n{framing}\r\n{body}"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        let _ =
            tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response)).await;
        response
    }

    #[tokio::test]
    async fn exact_form_grant_is_single_use_and_bad_requests_do_not_consume_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let url: Url = format!("{origin}count").parse().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let called = count.clone();
        let app = Router::new().route(
            "/count",
            post(move |body: String| {
                let called = called.clone();
                async move {
                    assert_eq!(body, "job=one");
                    called.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        );
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        let proxy = BrowserProxy::start(NetworkPolicy::new(Some(&origin)).unwrap())
            .await
            .unwrap();
        let framing = "Content-Length: 7\r\n";
        assert!(send(&proxy, &url, "job=one", framing).await.is_empty());
        let permit = proxy.allow_post(&url, b"job=one".to_vec()).unwrap();
        assert!(proxy.allow_post(&url, vec![]).is_err());
        assert!(send(&proxy, &url, "job=two", framing).await.is_empty());
        let wrong = url.join("other").unwrap();
        assert!(send(&proxy, &wrong, "job=one", framing).await.is_empty());
        assert!(
            send(
                &proxy,
                &url,
                "job=one",
                "Content-Length: 7\r\nContent-Length: 7\r\n"
            )
            .await
            .is_empty()
        );
        assert!(
            send(&proxy, &url, "job=one", "Transfer-Encoding: chunked\r\n")
                .await
                .is_empty()
        );
        assert!(!permit.forwarded());
        assert_eq!(count.load(Ordering::SeqCst), 0);
        let (response, observed) = tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                send(&proxy, &url, "job=one", framing).await
            },
            permit.wait_forwarded()
        );
        observed.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert!(permit.forwarded());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(send(&proxy, &url, "job=one", framing).await.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(permit);
        let withdrawn = proxy.allow_post(&url, b"job=one".to_vec()).unwrap();
        drop(withdrawn);
        assert!(send(&proxy, &url, "job=one", framing).await.is_empty());
        let expired = proxy.allow_post(&url, b"job=one".to_vec()).unwrap();
        proxy.grant.lock().unwrap().as_mut().unwrap().until = Instant::now();
        assert!(send(&proxy, &url, "job=one", framing).await.is_empty());
        assert!(!expired.forwarded());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        server.abort();
    }
}
