use crate::{
    receiver::{receiver, ReceiverError},
    sender::{self, sender},
    socket::Sockets,
    updater::updater,
    Discoverer,
};
use acto::{AcTokioRuntime, ActoCell, ActoId, ActoInput, ActoRef};
use hickory_proto::rr::Name;
use std::{
    collections::{HashMap, HashSet},
    mem::replace,
    net::{IpAddr, Ipv4Addr},
};
use tokio_util::sync::CancellationToken;

pub enum Input {
    RemoveAll,
    RemovePort(u16),
    RemoveAddr(IpAddr),
    AddAddr(u16, Vec<IpAddr>),
    SetTxt(String, Option<String>),
    RemoveTxt(String),
    AddInterface(IpAddr),
    RemoveInterface(IpAddr),
    SetInterfacesV4(Vec<Ipv4Addr>),
}

pub async fn guardian(
    mut ctx: ActoCell<Input, AcTokioRuntime, Result<(), ReceiverError>>,
    mut discoverer: Discoverer,
    sockets: Sockets,
    service_name: Name,
    cancel: CancellationToken,
) -> Result<(), String> {
    let callback = replace(&mut discoverer.callback, Box::new(|_, _| {}));
    let tau = discoverer.tau;
    let phi = discoverer.phi;
    let mut children = HashSet::new();
    let worker_cancel = cancel.clone();
    let upd_ref = ctx.supervise(
        ctx.spawn("updater", move |ctx| async move {
            tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => {},
                _ = updater(ctx, tau, phi, callback) => {},
            }
        })
        .map_handle(Ok),
    );
    children.insert(upd_ref.id());

    let sockets2 = sockets.clone();
    let sn = service_name.clone();
    let worker_cancel = cancel.clone();
    let snd_ref = ctx.supervise(
        ctx.spawn("sender", move |ctx| async move {
            tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => {},
                _ = sender(ctx, sockets, upd_ref, discoverer, sn) => {},
            }
        })
        .map_handle(Ok),
    );
    children.insert(snd_ref.id());

    for (name, socket) in [
        ("receiver_v4", sockets2.v4()),
        ("receiver_v6", sockets2.v6()),
    ] {
        if let Some(socket) = socket {
            let service_name = service_name.clone();
            let snd_ref = snd_ref.clone();
            let worker_cancel = cancel.clone();
            let receiver = ctx.spawn_supervised(name, move |ctx| async move {
                tokio::select! {
                    biased;
                    _ = worker_cancel.cancelled() => Ok(()),
                    result = receiver(ctx, service_name, socket, snd_ref) => result,
                }
            });
            children.insert(receiver.id());
        }
    }

    let mut interface_receivers = HashMap::<Ipv4Addr, (ActoId, CancellationToken)>::new();
    let mut retiring = HashSet::new();
    for addr in sockets2.get_all_interface_addresses_v4() {
        let (id, token) =
            spawn_interface_receiver(&mut ctx, &sockets2, &service_name, &snd_ref, &cancel, addr);
        children.insert(id);
        interface_receivers.insert(addr, (id, token));
    }

    let mut draining = false;
    let mut failure = None;
    while !children.is_empty() {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled(), if !draining => {
                draining = true;
                continue;
            }
            msg = ctx.recv() => msg,
        };
        match msg {
            ActoInput::NoMoreSenders => {
                cancel.cancel();
            }
            ActoInput::Supervision { id, name, result } => {
                children.remove(&id);
                let expected = retiring.remove(&id) || draining;
                match result {
                    Ok(Ok(())) if expected => {}
                    other => {
                        tracing::warn!(?id, %name, ?other, "discovery worker stopped; drain remaining workers");
                        failure.get_or_insert_with(|| {
                            format!("discovery worker {name} terminated unexpectedly: {other:?}")
                        });
                        cancel.cancel();
                    }
                }
            }
            ActoInput::Message(_) if draining => {}
            ActoInput::Message(msg) => {
                let desired = match msg {
                    Input::AddInterface(IpAddr::V4(addr)) => {
                        let mut desired: HashSet<_> = interface_receivers.keys().copied().collect();
                        desired.insert(addr);
                        desired
                    }
                    Input::RemoveInterface(IpAddr::V4(addr)) => {
                        let mut desired: HashSet<_> = interface_receivers.keys().copied().collect();
                        desired.remove(&addr);
                        desired
                    }
                    Input::SetInterfacesV4(interfaces) => interfaces.into_iter().collect(),
                    Input::AddInterface(_) | Input::RemoveInterface(_) => continue,
                    msg => {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => {},
                            _ = snd_ref.send_wait(sender::MdnsMsg::Update(msg)) => {},
                        }
                        continue;
                    }
                };
                for addr in interface_receivers
                    .keys()
                    .copied()
                    .filter(|addr| !desired.contains(addr))
                    .collect::<Vec<_>>()
                {
                    let (id, token) = interface_receivers
                        .remove(&addr)
                        .expect("registered receiver");
                    retiring.insert(id);
                    token.cancel();
                    sockets2.remove_interface_v4(addr);
                }
                for addr in desired {
                    if interface_receivers.contains_key(&addr) {
                        continue;
                    }
                    if let Err(error) = sockets2.add_interface_v4(addr) {
                        tracing::warn!(%addr, %error, "failed to add multicast interface");
                        continue;
                    }
                    let (id, token) = spawn_interface_receiver(
                        &mut ctx,
                        &sockets2,
                        &service_name,
                        &snd_ref,
                        &cancel,
                        addr,
                    );
                    children.insert(id);
                    interface_receivers.insert(addr, (id, token));
                }
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

fn spawn_interface_receiver(
    ctx: &mut ActoCell<Input, AcTokioRuntime, Result<(), ReceiverError>>,
    sockets: &Sockets,
    service_name: &Name,
    snd_ref: &ActoRef<sender::MdnsMsg>,
    cancel: &CancellationToken,
    addr: Ipv4Addr,
) -> (ActoId, CancellationToken) {
    let socket = sockets
        .get_interface_socket_v4(addr)
        .expect("interface socket registered");
    let service_name = service_name.clone();
    let snd_ref = snd_ref.clone();
    let token = cancel.child_token();
    let worker_cancel = token.clone();
    let receiver = ctx.spawn_supervised(
        &format!("receiver_interface_{addr}"),
        move |ctx| async move {
            tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => Ok(()),
                result = receiver(ctx, service_name, socket, snd_ref) => result,
            }
        },
    );
    (receiver.id(), token)
}
