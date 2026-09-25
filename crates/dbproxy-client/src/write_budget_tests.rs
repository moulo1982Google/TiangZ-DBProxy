use super::*;
use tokio::io::{AsyncReadExt, duplex};

/// 64 字节流可确定停在部分帧写入；与真实 TCP 的排队/重连用例分别报告。 / A 64-byte stream deterministically stalls a partial frame, separately from the real TCP queue/reconnect tests.
async fn partial_write(cancel: bool) {
    let (writer, mut peer) = duplex(64);
    let writer = Mutex::new(Some(writer));
    let shared = ConnectionShared::new();
    let frame = wire::ClientFrame {
        body: Some(wire::client_frame::Body::Request(wire::RequestEnvelope {
            rpc_id: 1,
            body: Some(wire::request_envelope::Body::SaveSnapshot(
                wire::SaveSnapshotRequest {
                    payload: vec![7; 1024],
                    ..Default::default()
                },
            )),
        })),
    };
    let mut budget = RequestBudget::new(Duration::from_millis(50)).unwrap();
    let mut request = Box::pin(write_request(
        writer.lock().await,
        &shared,
        &frame,
        DEFAULT_MAX_FRAME_BYTES,
        &mut budget,
    ));
    let declared = tokio::select! {
        result = &mut request => panic!("request ended before its frame header: {result:?}"),
        header = peer.read_u32() => header.unwrap() as usize,
    };
    assert!(
        writer.try_lock().is_err(),
        "the frame must still be writing"
    );
    if cancel {
        drop(request);
    } else {
        assert!(matches!(request.await, Err(ClientError::RequestTimeout)));
    }
    assert!(!shared.usable());
    assert!(writer.lock().await.is_none());
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(
        bytes.len() < declared,
        "fixture must interrupt a partial frame"
    );
}

#[tokio::test]
async fn timed_out_partial_write_closes_the_stream() {
    timeout(Duration::from_secs(3), partial_write(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelling_partial_write_closes_the_stream() {
    timeout(Duration::from_secs(3), partial_write(true))
        .await
        .unwrap();
}
