// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use s2n_quic_core::{
    event::Subscriber,
    packet::{interceptor::Interceptor, number::PacketNumberSpace},
    varint::VarInt,
};

#[test]
fn optimistic_ack_mitigation() {
    let model = Model::default();
    model.set_delay(Duration::from_millis(50));
    const LEN: usize = 1_000_000;

    let server_subscriber = recorder::PacketSkipped::new();
    let server_events = server_subscriber.events();
    let client_subscriber = recorder::PacketSkipped::new();
    let client_events = server_subscriber.events();
    test(model, |handle| {
        let mut server = Server::builder()
            .with_io(handle.builder().build()?)?
            .with_tls(SERVER_CERTS)?
            .with_event((tracing_events(), server_subscriber))?
            .with_random(Random::with_seed(456))?
            .start()?;

        let addr = server.local_addr()?;
        spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let mut stream = conn.open_bidirectional_stream().await.unwrap();
            stream.send(vec![42; LEN].into()).await.unwrap();
            stream.flush().await.unwrap();
        });

        let client = Client::builder()
            .with_io(handle.builder().build().unwrap())?
            .with_tls(certificates::CERT_PEM)?
            .with_event((tracing_events(), client_subscriber))?
            .with_random(Random::with_seed(456))?
            .start()?;

        primary::spawn(async move {
            let connect = Connect::new(addr).with_server_name("localhost");
            let mut conn = client.connect(connect).await.unwrap();
            let mut stream = conn.accept_bidirectional_stream().await.unwrap().unwrap();

            let mut recv_len = 0;
            while let Some(chunk) = stream.receive().await.unwrap() {
                recv_len += chunk.len();
            }
            assert_eq!(LEN, recv_len);
        });

        Ok(addr)
    })
    .unwrap();

    let server_skip_count = server_events
        .lock()
        .unwrap()
        .iter()
        .filter(|reason| {
            matches!(
                reason,
                events::PacketSkipReason::OptimisticAckMitigation { .. }
            )
        })
        .count();
    let client_skip_count = client_events
        .lock()
        .unwrap()
        .iter()
        .filter(|reason| {
            matches!(
                reason,
                events::PacketSkipReason::OptimisticAckMitigation { .. }
            )
        })
        .count();

    // Verify that both client and server are skipping packets for Optimistic
    // Ack attack mitigation.
    assert_eq!(server_skip_count, 5);
    assert_eq!(client_skip_count, 5);
}

// Mimic an Optimistic Ack attack and confirm the connection is closed.
//
// Use the SkipSubscriber to record the skipped packet_number and then use
// the SkipInterceptor to inject an ACK for that packet.
#[test]
fn detect_optimistic_ack() {
    let model = Model::default();
    model.set_delay(Duration::from_millis(50));
    const LEN: usize = 1_000_000;

    let skip_pn = Arc::new(Mutex::new(None));
    let skip_subscriber = SkipSubscriber {
        skip_packet_number: skip_pn.clone(),
    };
    let skip_interceptor = SkipInterceptor {
        skip_packet_number: skip_pn,
    };
    test(model, |handle| {
        let mut server = Server::builder()
            .with_io(handle.builder().build()?)?
            .with_tls(SERVER_CERTS)?
            .with_event((tracing_events(), skip_subscriber))?
            .with_random(Random::with_seed(456))?
            .with_packet_interceptor(skip_interceptor)?
            .start()?;

        let addr = server.local_addr()?;
        spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let mut stream = conn.open_bidirectional_stream().await.unwrap();
            stream.send(vec![42; LEN].into()).await.unwrap();
            let send_result = stream.flush().await;
            // connection should abort since we inject a skip packet number
            assert!(send_result.is_err());
        });

        let client = Client::builder()
            .with_io(handle.builder().build().unwrap())?
            .with_tls(certificates::CERT_PEM)?
            .with_event(tracing_events())?
            .with_random(Random::with_seed(456))?
            .start()?;

        primary::spawn(async move {
            let connect = Connect::new(addr).with_server_name("localhost");
            let mut conn = client.connect(connect).await.unwrap();
            let mut stream = conn.accept_bidirectional_stream().await.unwrap().unwrap();

            let mut recv_len = 0;

            while let Ok(Some(chunk)) = stream.receive().await {
                recv_len += chunk.len();
            }
            // connection aborts before completing the transfer
            assert_ne!(LEN, recv_len);
        });

        Ok(addr)
    })
    .unwrap();
}

struct SkipSubscriber {
    skip_packet_number: Arc<Mutex<Option<u64>>>,
}

impl Subscriber for SkipSubscriber {
    type ConnectionContext = Arc<Mutex<Option<u64>>>;

    fn create_connection_context(
        &mut self,
        _meta: &s2n_quic_core::event::api::ConnectionMeta,
        _info: &s2n_quic_core::event::api::ConnectionInfo,
    ) -> Self::ConnectionContext {
        self.skip_packet_number.clone()
    }

    fn on_packet_skipped(
        &mut self,
        context: &mut Self::ConnectionContext,
        _meta: &s2n_quic_core::event::api::ConnectionMeta,
        event: &s2n_quic_core::event::api::PacketSkipped,
    ) {
        *context.lock().unwrap() = Some(event.number);
    }
}

struct SkipInterceptor {
    skip_packet_number: Arc<Mutex<Option<u64>>>,
}

impl Interceptor for SkipInterceptor {
    #[inline(always)]
    fn intercept_rx_frame<'a, A: s2n_quic_core::frame::ack::AckRanges, D>(
        &'a mut self,
        frame: s2n_quic_core::frame::Frame<'a, A, D>,
    ) -> s2n_quic_core::frame::Frame<'a, A, D> {
        // use s2n_quic_core::frame::ack::AckRanges;
        // use s2n_quic_core::packet::number::PacketNumberRange;

        let mut transport_range = s2n_quic_transport::ack::ack_ranges::AckRanges::default();
        match frame {
            s2n_quic_core::frame::Frame::Ack(ack_frame) => {
                let ack_ranges = ack_frame.ack_ranges();
                // for aa in ack_range {
                //     let start= aa.start();
                //     let end = aa.end();
                //     let start = PacketNumberSpace::ApplicationData.new_packet_number(*start);
                //     let end = PacketNumberSpace::ApplicationData.new_packet_number(*end);
                //     let pnr = PacketNumberRange::new(start, end);
                //             transport_range.insert_packet_number_range(pnr);
                // }
                let ack = s2n_quic_core::frame::Ack {
                    ack_delay: ack_frame.ack_delay,
                    ack_ranges: transport_range,
                    ecn_counts: ack_frame.ecn_counts,
                };
                s2n_quic_core::frame::Frame::Ack(ack)
            }
            f => f,
        }
    }

    fn intercept_rx_inject_ack(&mut self, space: PacketNumberSpace) -> Option<VarInt> {
        // use s2n_quic_core::frame::ack::AckRanges;
        // use s2n_quic_core::frame::Ack;

        // let mut ack_range = s2n_quic_transport::ack::ack_ranges::AckRanges::default();
        // ack_range
        //     .insert_packet_number(packet_number.space().new_packet_number(pn))
        //     .unwrap();

        // //         // TODO possibly source ack_delay and ecn_counts values from the interceptor
        // let insert_frame = Ack {
        //     ack_delay: VarInt::default(),
        //     ack_ranges: &ack_range,
        //     ecn_counts: None,
        // };

        if let Some(pn) = *self.skip_packet_number.lock().unwrap() {
            assert!(matches!(space, PacketNumberSpace::ApplicationData));
            // clear the packet_number to ack
            *self.skip_packet_number.lock().unwrap() = None;
            Some(VarInt::new(pn).unwrap())
        } else {
            None
        }
    }
}
