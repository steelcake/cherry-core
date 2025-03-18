use crate::{svm, DataStream, ProviderConfig, Query};
use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use futures_lite::StreamExt;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::mpsc;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks,
};

pub async fn start_stream(cfg: ProviderConfig) -> Result<DataStream> {
    let _url = cfg
        .url
        .as_ref()
        .context("url is required when using yellowstone grpc.")?;

    if !matches!(cfg.query, Query::Svm(_)) {
        return Err(anyhow!(
            "only svm query is supported with yellowstone grpc."
        ));
    }

    let (tx, rx) = mpsc::channel(cfg.buffer_size.unwrap_or(50));

    tokio::spawn(async move {
        if let Err(e) = run_stream(cfg, tx).await {
            log::error!("failed to run stream: {:?}", e);
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    Ok(Box::pin(stream))
}

async fn run_stream(
    cfg: ProviderConfig,
    tx: mpsc::Sender<Result<BTreeMap<String, RecordBatch>>>,
) -> Result<()> {
    let query = match cfg.query {
        Query::Svm(q) => q,
        Query::Evm(_) => {
            return Err(anyhow!(
                "only svm query is supported with yellowstone grpc."
            ))
        }
    };

    let mut client = GeyserGrpcClient::build_from_shared(
        cfg.url
            .context("url is required when using yellowstone grpc.")?,
    )
    .context("create client")?
    .x_token(cfg.bearer_token)
    .context("pass token to client")?
    .connect_timeout(Duration::from_millis(
        cfg.req_timeout_millis.unwrap_or(10_000),
    ))
    .timeout(Duration::from_millis(
        cfg.req_timeout_millis.unwrap_or(10_000),
    ))
    .tls_config(ClientTlsConfig::new().with_native_roots())
    .context("add tls config")?
    .max_decoding_message_size(1 << 30)
    .connect()
    .await
    .context("connect to server")?;

    let mut stream = client
        .subscribe_once(SubscribeRequest {
            blocks: [(
                "data".to_owned(),
                SubscribeRequestFilterBlocks {
                    include_transactions: Some(true),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            commitment: Some(CommitmentLevel::Finalized as i32),
            ..Default::default()
        })
        .await
        .context("subscribe")?;

    while let Some(res) = stream.next().await {
        let update = res.context("get next from stream")?;

        let update = match update.update_oneof {
            Some(up) => up,
            None => continue,
        };

        match update {
            UpdateOneof::Block(block) => {}
            _ => return Err(anyhow!("unexpected update from rpc: {:?}", update)),
        }
    }

    Ok(())
}
