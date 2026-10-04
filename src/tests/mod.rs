mod socket;

use super::*;
use std::net::{Ipv4Addr, Ipv6Addr};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_change_addresses() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let handle = tokio::runtime::Handle::current();

    let peer_id1 = "test_peer1".to_string();
    let peer_id2 = "test_peer2".to_string();

    let (tx, mut rx) = mpsc::channel(10);

    // First Discoverer (the one we're testing)
    let discoverer1 = Discoverer::new("test_service".to_string(), peer_id1.clone())
        .with_addrs(8000, vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))])
        .with_multicast_interfaces_v4(vec![Ipv4Addr::new(127, 0, 0, 1)])
        .with_cadence(Duration::from_secs(1))
        .with_response_rate(1.0);

    let guard1 = discoverer1
        .spawn(&handle)
        .expect("Failed to spawn discoverer1");

    // Second Discoverer (to verify the changes). It uses the same
    // multicast interface as the first one: multicast sent on the
    // loopback interface is only delivered to members on the loopback
    // interface, which also keeps this test independent of the host's
    // default route.
    let discoverer2 = Discoverer::new("test_service".to_string(), peer_id2)
        .with_multicast_interfaces_v4(vec![Ipv4Addr::new(127, 0, 0, 1)])
        .with_callback(move |id, peer| {
            if id == peer_id1 {
                tx.try_send(peer.clone()).ok();
            }
        });

    let _guard2 = discoverer2
        .spawn(&handle)
        .expect("Failed to spawn discoverer2");

    // Wait for initial discovery with a timeout
    let initial_peer = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("Timeout waiting for initial peer")
        .expect("Failed to receive initial peer");
    assert_eq!(initial_peer.addrs().len(), 1);
    assert_eq!(
        initial_peer.addrs()[0],
        (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000)
    );

    // Change addresses
    guard1.add(
        9000,
        vec![IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))],
    );
    guard1.remove_port(8000);

    // Wait for the update to be discovered
    let updated_peer = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(peer) = rx.recv().await {
                if peer.addrs().len() == 1 && peer.addrs()[0].1 == 9000 {
                    return Ok(peer);
                }
            } else {
                return Err("Failed to receive updated peer");
            }
        }
    })
    .await
    .expect("Timeout waiting for updated peer")
    .expect("Failed to receive updated peer");

    assert_eq!(updated_peer.addrs().len(), 1);
    assert_eq!(
        updated_peer.addrs()[0],
        (IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 9000)
    );

    // Stop the discoverers
    drop(guard1);
}
