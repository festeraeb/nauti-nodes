//! Loop guard: tell whether a provider URL points back at this node.
//!
//! On 2026-10-24 Paddler slots pointed at a node router that fell back to
//! Paddler, forming a loop. Callers use `is_self_endpoint` to refuse a
//! FederatedWorker or RemoteInferenceApi target that is this node itself.

/// True when the host part of `url` is this node: "127.0.0.1", "localhost",
/// "::1", or equal (ASCII case-insensitive) to one of `self_addrs`
/// (this node's listen addresses and hostnames).
///
/// The host is the text after "scheme://" (or the whole string when there is
/// no scheme), up to the first '/', with any ":port" removed; an IPv6 host in
/// brackets ("[::1]:8080") is taken without the brackets. An empty host gives
/// false.
pub fn is_self_endpoint(url: &str, self_addrs: &[String]) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split('/').next().unwrap_or("");
    let host = match authority.strip_prefix('[') {
        Some(inner) => inner.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    if host.is_empty() {
        return false;
    }
    ["127.0.0.1", "localhost", "::1"]
        .iter()
        .copied()
        .chain(self_addrs.iter().map(String::as_str))
        .any(|a| a.eq_ignore_ascii_case(host))
}
