//! NN1a: loop guard for provider endpoints (no network).

use nauti_nodes::providers::self_endpoint::is_self_endpoint;

#[test]
fn loopback_is_always_self() {
    assert!(is_self_endpoint("http://127.0.0.1:8310/v1", &[]));
    assert!(is_self_endpoint("http://localhost:8310/v1", &[]));
    assert!(is_self_endpoint("http://LOCALHOST/v1", &[]));
    assert!(is_self_endpoint("http://[::1]:8310/v1", &[]));
}

#[test]
fn own_listen_address_is_self() {
    assert!(is_self_endpoint("http://10.44.0.13:8310/v1", &["10.44.0.13".into()]));
    assert!(is_self_endpoint("https://t440.lan/v1", &["10.44.0.13".into(), "T440.lan".into()]));
    assert!(is_self_endpoint("10.44.0.13:8310", &["10.44.0.13".into()]));
}

#[test]
fn other_hosts_are_not_self() {
    assert!(!is_self_endpoint("http://10.44.0.10:8080/v1", &["10.44.0.13".into()]));
    assert!(!is_self_endpoint("https://integrate.api.nvidia.com/v1", &["10.44.0.13".into()]));
    // Prefix of an own address is not a match.
    assert!(!is_self_endpoint("http://10.44.0.1:8080/v1", &["10.44.0.13".into()]));
    assert!(!is_self_endpoint("", &["10.44.0.13".into()]));
}
