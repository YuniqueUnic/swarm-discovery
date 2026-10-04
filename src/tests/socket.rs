use crate::{
    socket::{Mode, Sockets},
    IpClass,
};
use hickory_proto::op::{Message, MessageType, OpCode};
use std::{net::Ipv4Addr, time::Duration};

#[tokio::test]
async fn explicit_loopback_interface_is_installed_on_first_socket_set() {
    let sockets = Sockets::new(IpClass::V4Only, vec![Ipv4Addr::LOCALHOST]).unwrap();
    assert!(sockets
        .get_interface_socket_v4(Ipv4Addr::LOCALHOST)
        .is_some());
    assert_eq!(
        sockets.get_all_interface_addresses_v4(),
        vec![Ipv4Addr::LOCALHOST]
    );
}

#[tokio::test]
async fn any_mode_sends_on_ipv4_without_explicit_interfaces() {
    let sockets = Sockets::new(IpClass::V4Only, vec![]).unwrap();
    let listener = sockets.v4().unwrap();
    let message = Message::new(0x5a71, MessageType::Query, OpCode::Query);
    let expected = message.to_vec().unwrap();
    sockets.send_msg(&message, Mode::Any).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = [0; 1472];
        loop {
            let (len, _) = listener.recv_from(&mut bytes).await.unwrap();
            if bytes[..len] == expected {
                break;
            }
        }
    })
    .await
    .expect("Any mode must use the available IPv4 socket");
    assert_eq!(sockets.send_failure_count(IpClass::V4Only), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn failed_ipv4_send_updates_shared_family_and_aggregate_counters() {
    let sockets = Sockets::new(IpClass::V4Only, vec![]).unwrap();
    let socket = sockets.v4().unwrap();
    socket.connect((Ipv4Addr::LOCALHOST, 9)).await.unwrap();
    socket2::SockRef::from(socket.as_ref())
        .shutdown(std::net::Shutdown::Write)
        .unwrap();
    let observer = sockets.clone();
    let message = Message::new(0x5a72, MessageType::Query, OpCode::Query);
    sockets.send_msg(&message, Mode::Any).await;
    assert_eq!(observer.send_failure_count(IpClass::V4Only), 1);
    assert_eq!(observer.send_failure_count(IpClass::V6Only), 0);
    assert_eq!(observer.send_failure_count(IpClass::V4AndV6), 1);
    assert_eq!(observer.send_failure_count(IpClass::Auto), 1);
}
