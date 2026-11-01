use pulse_link::browser_network::{BrowserProxy, NetworkPolicy, public_ip};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[test]
fn urls_and_all_nonpublic_ranges_are_rejected() {
    let policy = NetworkPolicy::new(None).unwrap();
    for ip in [
        "0.0.0.0",
        "10.1.2.3",
        "127.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.1.1",
        "100.64.1.1",
        "224.0.0.1",
        "192.0.2.1",
        "198.18.1.1",
        "198.51.100.2",
        "203.0.113.2",
        "::1",
        "fe80::1",
        "fc00::1",
        "::ffff:127.0.0.1",
        "64:ff9b::7f00:1",
        "2002:7f00:1::",
        "2001:db8::1",
    ] {
        assert!(!public_ip(ip.parse().unwrap()), "{ip}");
    }
    for ip in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
        assert!(public_ip(ip.parse().unwrap()));
    }
    for url in [
        "http://localhost/",
        "http://localhost./",
        "http://127.0.0.1/",
        "http://2130706433/",
        "http://0x7f000001/",
        "http://10.0.0.1/",
        "file:///etc/passwd",
        "http://example.com:8000/",
        "https://user:pass@example.com/",
        "https://example.com/?api_key=secret",
    ] {
        let url: url::Url = url.parse().unwrap();
        assert!(policy.validate_url(&url).is_err(), "{url}");
    }
    for fixture in [
        "http://localhost:1234/",
        "https://127.0.0.1:1234/",
        "http://127.0.0.1:1234/path",
        "http://127.0.0.1:1234/?token=x",
    ] {
        assert!(NetworkPolicy::new(Some(fixture)).is_err());
    }
}
async fn request(proxy: &BrowserProxy, line: &str) -> String {
    let mut socket = TcpStream::connect(proxy.address).await.unwrap();
    socket.write_all(line.as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        socket.read_to_end(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    String::from_utf8_lossy(&bytes).into()
}
#[tokio::test]
async fn proxy_pins_only_explicit_fixture_and_blocks_private_connect_and_names() {
    let allowed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let denied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", allowed.local_addr().unwrap());
    let sink = tokio::spawn(async move {
        let (mut socket, _) = allowed.accept().await.unwrap();
        let mut bytes = [0; 4096];
        let n = socket.read(&mut bytes).await.unwrap();
        let text = String::from_utf8_lossy(&bytes[..n]);
        assert!(text.starts_with("GET /page?q=1 HTTP/1.1"));
        assert!(!text.contains("Proxy-Authorization"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nmarker")
            .await
            .unwrap();
    });
    let proxy = BrowserProxy::start(NetworkPolicy::new(Some(&origin)).unwrap())
        .await
        .unwrap();
    assert!(
        request(
            &proxy,
            &format!("GET {origin}page?q=1 HTTP/1.1\r\nProxy-Authorization: secret\r\n\r\n")
        )
        .await
        .ends_with("marker")
    );
    sink.await.unwrap();
    for target in [
        format!("http://{}/", denied.local_addr().unwrap()),
        "http://127.0.0.1/".into(),
        "http://localhost/".into(),
        "http://localhost./".into(),
        "http://[::1]/".into(),
    ] {
        assert!(
            request(&proxy, &format!("GET {target} HTTP/1.1\r\n\r\n"))
                .await
                .starts_with("HTTP/1.1 403"),
            "{target}"
        );
    }
    assert!(
        request(&proxy, "CONNECT 127.0.0.1:443 HTTP/1.1\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 403")
    );
    assert!(
        request(&proxy, &format!("POST {origin} HTTP/1.1\r\n\r\n"))
            .await
            .is_empty()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), denied.accept())
            .await
            .is_err()
    );
    let address = proxy.address;
    drop(proxy);
    tokio::task::yield_now().await;
    assert!(TcpStream::connect(address).await.is_err());
}
