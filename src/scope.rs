use url::Url;

/// Canonical, explicitly Human-selected resource; does not grant local network access.
pub fn browser_url(scope: &str) -> Option<Url> {
    let value = scope.strip_prefix("browser:")?;
    let url = Url::parse(value).ok()?;
    (value.len() <= 2048
        && matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && !url.query_pairs().any(|(key, _)| {
            matches!(
                key.to_ascii_lowercase().as_str(),
                "token"
                    | "access_token"
                    | "api_key"
                    | "apikey"
                    | "password"
                    | "authorization"
                    | "secret"
            )
        }))
    .then_some(url)
}
pub fn read_scope(scope: &str) -> bool {
    scope == "pulse:read"
        || input_digest(scope).is_some()
        || browser_url(scope).is_some()
        || interaction_url(scope).is_some()
        || crate::ComputerSelection::from_scope(scope).is_some()
}
pub fn input_digest(scope: &str) -> Option<&str> {
    let digest = scope.strip_prefix("input:")?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    .then_some(digest)
}
pub fn interaction_url(scope: &str) -> Option<Url> {
    browser_url(&format!(
        "browser:{}",
        scope.strip_prefix("browser-interact:")?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn browser_resources_preserve_web_path_query_and_fragment_semantics() {
        let url = browser_url("browser:https://EXAMPLE.com/a//b?q=1#part").unwrap();
        assert_eq!(url.as_str(), "https://example.com/a//b?q=1#part");
        assert_ne!(
            url,
            browser_url("browser:https://example.com/a/b?q=1#part").unwrap()
        );
        for value in [
            "file:///etc/passwd",
            "javascript:alert(1)",
            "https://user@example.com/",
            "https://example.com/?%74oken=x",
            "https://example.com/?PASSWORD=x",
        ] {
            assert!(browser_url(&format!("browser:{value}")).is_none());
        }
        assert!(!read_scope("browser"));
        assert!(read_scope("pulse:read"));
    }
}
