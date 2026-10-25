//! NN1b: provider ranking with an "internet first" switch (no network).

use nauti_nodes::providers::{provider_rank, ProviderKind, ProviderPolicy};

#[test]
fn default_policy_is_unchanged() {
    let p = ProviderPolicy::default();
    assert!(p.prefer_local);
    assert!(!p.prefer_remote);
    assert!(!p.allow_remote);
    assert_eq!(provider_rank(ProviderKind::LocalWorker, &p), 0);
    assert_eq!(provider_rank(ProviderKind::FederatedWorker, &p), 1);
    assert_eq!(provider_rank(ProviderKind::DatacenterPool, &p), 2);
    assert_eq!(provider_rank(ProviderKind::RemoteInferenceApi, &p), 3);
}

#[test]
fn local_not_preferred_ranks_local_with_federated() {
    let p = ProviderPolicy { prefer_local: false, ..ProviderPolicy::default() };
    assert_eq!(provider_rank(ProviderKind::LocalWorker, &p), 1);
    assert_eq!(provider_rank(ProviderKind::FederatedWorker, &p), 1);
}

#[test]
fn internet_first_puts_remote_first_and_keeps_the_rest_in_order() {
    let p = ProviderPolicy::from_route_order("internet-first").unwrap();
    assert!(p.prefer_remote && p.allow_remote && p.prefer_local);
    assert_eq!(provider_rank(ProviderKind::RemoteInferenceApi, &p), 0);
    assert_eq!(provider_rank(ProviderKind::LocalWorker, &p), 1);
    assert_eq!(provider_rank(ProviderKind::FederatedWorker, &p), 2);
    assert_eq!(provider_rank(ProviderKind::DatacenterPool, &p), 3);
}

#[test]
fn local_first_allows_remote_but_ranks_it_last() {
    let p = ProviderPolicy::from_route_order("local-first").unwrap();
    assert!(p.allow_remote && p.prefer_local && !p.prefer_remote);
    assert_eq!(provider_rank(ProviderKind::LocalWorker, &p), 0);
    assert_eq!(provider_rank(ProviderKind::RemoteInferenceApi, &p), 3);
}

#[test]
fn route_order_parsing() {
    assert!(ProviderPolicy::from_route_order(" Internet-First ").unwrap().prefer_remote);
    assert!(ProviderPolicy::from_route_order("").is_none());
    assert!(ProviderPolicy::from_route_order("cloud").is_none());
}
