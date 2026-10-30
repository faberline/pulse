use super::*;
#[test]
fn selected_images_are_local_bounded_and_do_not_follow_links() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("image.png");
    image_codec::DynamicImage::new_rgba8(2, 2)
        .save_with_format(&path, image_codec::ImageFormat::Png)
        .unwrap();
    assert!(selected_image(path.to_str().unwrap()).is_ok());
    let link = dir.path().join("link.png");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(selected_image(link.to_str().unwrap()).is_err());
    assert!(selected_image("https://example.com/tracker.png").is_err());
    image_codec::DynamicImage::new_rgba8(4097, 1)
        .save_with_format(&path, image_codec::ImageFormat::Png)
        .unwrap();
    assert!(selected_image(path.to_str().unwrap()).is_err());
    std::fs::write(&path, b"GIF87a\0\0\0\0").unwrap();
    assert!(selected_image(path.to_str().unwrap()).is_err());
}
#[test]
fn desktop_settings_never_accept_a_credential_field() {
    assert!(serde_json::from_value::<ConnectionForm>(serde_json::json!({"server":"https://example.com/","repo":"/tmp/pulse","data_dir":"/tmp/private","dev_loopback":false,"token":"unexpected"})).is_err());
}
#[test]
fn native_table_avoids_infinite_width_markdown_table() {
    let blocks = rich_content("before\n\n| 欄位 | 值 |\n|---|---|\n| A | `B` |\n\nafter");
    assert_eq!(blocks.len(), 3);
    let RichBlock::Table { headers, rows } = &blocks[1] else {
        panic!("native table missing")
    };
    assert_eq!(headers, &["欄位", "值"]);
    assert_eq!(rows, &[vec!["A", "B"]]);
    for block in blocks {
        if let RichBlock::Markdown(items) = block {
            assert!(
                !items
                    .iter()
                    .any(|item| matches!(item, markdown::Item::Table { .. }))
            );
        }
    }
}
#[test]
fn links_require_explicit_safe_https() {
    for url in [
        "javascript:alert(1)",
        "file:///etc/passwd",
        "data:text/plain,x",
        "pulse://run",
        "https://user:pass@example.com",
    ] {
        assert!(!safe_link(url));
    }
    assert!(safe_link("https://example.com/artifact/1"));
}
#[test]
fn markdown_supports_native_code_table_and_image() {
    let items:Vec<_>=markdown::parse("## 中文\n\n```rust\nlet x = 1;\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n![image](https://example.com/image.png)\n\n<script>alert(1)</script>").collect();
    assert!(
        items
            .iter()
            .any(|i| matches!(i, markdown::Item::CodeBlock { .. }))
    );
    assert!(
        items
            .iter()
            .any(|i| matches!(i, markdown::Item::Table { .. }))
    );
    assert!(
        items
            .iter()
            .any(|i| matches!(i, markdown::Item::Image { .. }))
    );
}
