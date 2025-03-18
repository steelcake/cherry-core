use crate::{svm, DataStream, ProviderConfig, Query};
use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use cherry_query::{run_query, Query as GenericQuery};
use cherry_svm_schema::{
    BalancesBuilder, BlocksBuilder, InstructionsBuilder, LogsBuilder, RewardsBuilder,
    TokenBalancesBuilder, TransactionsBuilder,
};
use futures_lite::StreamExt;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::mpsc;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterBlocks, SubscribeUpdateBlock,
    },
    prelude::Reward,
};

use super::common::svm_query_to_generic;

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

    let generic_query = svm_query_to_generic(&query);

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
        let update = match res.context("get next from stream") {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "got error from grpc. Error is: {:?}. Sending to receiver...",
                    e
                );
                if tx.send(Err(e)).await.is_err() {
                    log::debug!("quitting ingest loop since receiver is dropped");
                    break;
                } else {
                    continue;
                }
            }
        };

        let update = match update.update_oneof {
            Some(up) => up,
            None => continue,
        };

        let res = match process_update(&update, &generic_query) {
            Ok(r) => r,
            Err(e) => {
                log::error!("failed to process update coming from grpc. Error is: {:?}. Sending to receiver...", e);
                if tx.send(Err(e)).await.is_err() {
                    log::debug!("quitting ingest loop since receiver is dropped");
                    break;
                } else {
                    continue;
                }
            }
        };

        if tx.send(Ok(res)).await.is_err() {
            log::debug!("quitting ingest loop since receiver is dropped");
            break;
        }
    }

    Ok(())
}

fn process_update(
    update: &UpdateOneof,
    generic_query: &GenericQuery,
) -> Result<BTreeMap<String, RecordBatch>> {
    let data = match update {
        UpdateOneof::Block(block) => block,
        _ => return Err(anyhow!("unexpected update from rpc: {:?}", update)),
    };
    let data = parse_data(&data).context("parse data")?;
    let data = run_query(&data, &generic_query).context("run local query")?;
    Ok(data)
}

fn parse_data(data: &SubscribeUpdateBlock) -> Result<BTreeMap<String, RecordBatch>> {
    let mut blocks = BlocksBuilder::default();
    let mut rewards = RewardsBuilder::default();
    let mut token_balances = TokenBalancesBuilder::default();
    let mut balances = BalancesBuilder::default();
    let mut logs = LogsBuilder::default();
    let mut transactions = TransactionsBuilder::default();
    let mut instructions = InstructionsBuilder::default();

    let block_info = parse_block(&mut blocks, data).context("parse block")?;

    if let Some(rewards_data) = data.rewards.as_ref() {
        parse_rewards(&mut rewards, &block_info, &rewards_data.rewards).context("parse rewards")?;
    }

    let mut data = BTreeMap::new();
    data.insert("blocks".to_owned(), blocks.finish());
    data.insert("rewards".to_owned(), rewards.finish());
    data.insert("token_balances".to_owned(), token_balances.finish());
    data.insert("balances".to_owned(), balances.finish());
    data.insert("logs".to_owned(), logs.finish());
    data.insert("transactions".to_owned(), transactions.finish());
    data.insert("instructions".to_owned(), instructions.finish());

    Ok(data)
}

fn parse_block(blocks: &mut BlocksBuilder, data: &SubscribeUpdateBlock) -> Result<BlockInfo> {
    let block_info = BlockInfo {
        slot: data.slot,
        hash: decode_base58(data.blockhash.as_str()).context("parse blockhash")?,
    };

    blocks.slot.append_value(block_info.slot);
    blocks.hash.append_value(block_info.hash.as_slice());
    blocks.parent_slot.append_value(data.parent_slot);
    blocks.parent_hash.append_value(
        decode_base58(data.parent_blockhash.as_str()).context("parse parent_blockhash")?,
    );
    blocks
        .height
        .append_option(data.block_height.map(|x| x.block_height));
    blocks
        .timestamp
        .append_option(data.block_time.map(|x| x.timestamp));

    Ok(block_info)
}

fn parse_rewards(
    rewards: &mut RewardsBuilder,
    block_info: &BlockInfo,
    data: &[Reward],
) -> Result<()> {
    for r in data.iter() {
        rewards.block_slot.append_value(block_info.slot);
        rewards.block_hash.append_value(block_info.hash.as_slice());
        rewards
            .pubkey
            .append_value(decode_base58(r.pubkey.as_str()).context("parse pubkey")?);
        rewards.lamports.append_value(r.lamports);
        rewards.post_balance.append_value(r.post_balance);
        rewards
            .reward_type
            .append_value(r.reward_type().as_str_name().to_lowercase());
        rewards.commission.append_null(); // TODO: figure out how to parse this
    }

    Ok(())
}

fn decode_base58(v: &str) -> Result<Vec<u8>> {
    bs58::decode(v)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
        .context("decode base58")
}

struct BlockInfo {
    slot: u64,
    hash: Vec<u8>,
}
