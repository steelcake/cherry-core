use crate::{DataStream, ProviderConfig, Query};
use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use cherry_svm_schema::{
    BalancesBuilder, BlocksBuilder, InstructionsBuilder, LogsBuilder, RewardsBuilder,
    TokenBalancesBuilder, TransactionsBuilder,
};
use futures_lite::StreamExt;
use std::str::FromStr;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::mpsc;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::TokenBalance;
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterBlocks, SubscribeUpdateBlock,
    },
    prelude::Reward,
};

pub async fn start_stream(cfg: ProviderConfig, query: crate::Query) -> Result<DataStream> {
    let _url = cfg
        .url
        .as_ref()
        .context("url is required when using yellowstone grpc.")?;

    if !matches!(&query, Query::Svm(_)) {
        return Err(anyhow!(
            "only svm query is supported with yellowstone grpc."
        ));
    }

    let (tx, rx) = mpsc::channel(cfg.buffer_size.unwrap_or(50));

    tokio::spawn(async move {
        if let Err(e) = run_stream(cfg, query, tx).await {
            log::error!("failed to run stream: {:?}", e);
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    Ok(Box::pin(stream))
}

async fn run_stream(
    cfg: ProviderConfig,
    query: crate::Query,
    tx: mpsc::Sender<Result<BTreeMap<String, RecordBatch>>>,
) -> Result<()> {
    let _query = match query {
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
            Some(UpdateOneof::Block(up)) => up,
            _ => continue,
        };

        let res = match process_update(update) {
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

fn process_update(mut data: SubscribeUpdateBlock) -> Result<BTreeMap<String, RecordBatch>> {
    data.transactions.sort_by_key(|tx| tx.index);

    let data = parse_data(&data).context("parse data")?;

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

    parse_transactions(
        &mut token_balances,
        &mut balances,
        &mut logs,
        &mut transactions,
        &mut instructions,
        &block_info,
        data,
    )
    .context("parse tx data")?;

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

fn parse_transactions(
    token_balances: &mut TokenBalancesBuilder,
    balances: &mut BalancesBuilder,
    _logs: &mut LogsBuilder,
    transactions: &mut TransactionsBuilder,
    _instructions: &mut InstructionsBuilder,
    block_info: &BlockInfo,
    data: &SubscribeUpdateBlock,
) -> Result<()> {
    for tx in data.transactions.iter() {
        if tx.is_vote {
            continue;
        }

        let inner = match tx.transaction.as_ref() {
            Some(inner) => inner,
            None => continue,
        };

        let msg = match inner.message.as_ref() {
            Some(msg) => msg,
            None => continue,
        };

        let meta = match tx.meta.as_ref() {
            Some(m) => m,
            None => continue,
        };

        let accounts = msg
            .account_keys
            .iter()
            .chain(meta.loaded_writable_addresses.iter())
            .chain(meta.loaded_readonly_addresses.iter())
            .collect::<Vec<_>>();

        let tx_index = u32::try_from(tx.index).context("tx index to u32")?;

        let mut token_balances_map =
            BTreeMap::<u32, (Option<TokenBalance>, Option<TokenBalance>)>::new();

        for pre in meta.pre_token_balances.iter() {
            match token_balances_map.entry(pre.account_index) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((Some(pre.clone()), None));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().0 = Some(pre.clone());
                }
            }
        }
        for post in meta.post_token_balances.iter() {
            match token_balances_map.entry(post.account_index) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((None, Some(post.clone())));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().1 = Some(post.clone());
                }
            }
        }

        for (&acc_index, (pre, post)) in token_balances_map.iter() {
            let acc_index = usize::try_from(acc_index).context("convert account index to usize")?;
            let acc = accounts.get(acc_index).context("get account")?;

            token_balances.block_slot.append_value(block_info.slot);
            token_balances
                .block_hash
                .append_value(block_info.hash.as_slice());
            token_balances.transaction_index.append_value(tx_index);
            token_balances.account.append_value(acc.as_slice());

            if let Some(pre) = pre.as_ref() {
                token_balances
                    .pre_mint
                    .append_value(decode_base58(pre.mint.as_str()).context("parse pre mint")?);
                token_balances.pre_program_id.append_value(
                    decode_base58(pre.program_id.as_str()).context("parse pre program id")?,
                );
                token_balances
                    .pre_owner
                    .append_value(decode_base58(pre.owner.as_str()).context("parse pre owner")?);
                if let Some(ui_amount) = pre.ui_token_amount.as_ref() {
                    token_balances.pre_decimals.append_value(
                        ui_amount
                            .decimals
                            .try_into()
                            .context("convert pre_decimals")?,
                    );
                    token_balances.pre_amount.append_value(
                        u64::from_str(ui_amount.amount.as_str()).context("parse pre amount")?,
                    );
                } else {
                    token_balances.pre_decimals.append_null();
                    token_balances.pre_amount.append_null();
                }
            } else {
                token_balances.pre_mint.append_null();
                token_balances.pre_program_id.append_null();
                token_balances.pre_owner.append_null();
                token_balances.pre_decimals.append_null();
                token_balances.pre_amount.append_null();
            }

            if let Some(post) = post.as_ref() {
                token_balances
                    .post_mint
                    .append_value(decode_base58(post.mint.as_str()).context("parse post mint")?);
                token_balances.post_program_id.append_value(
                    decode_base58(post.program_id.as_str()).context("parse post program id")?,
                );
                token_balances
                    .post_owner
                    .append_value(decode_base58(post.owner.as_str()).context("parse post owner")?);

                if let Some(ui_amount) = post.ui_token_amount.as_ref() {
                    token_balances.post_decimals.append_value(
                        ui_amount
                            .decimals
                            .try_into()
                            .context("convert post decimals")?,
                    );
                    token_balances.post_amount.append_value(
                        u64::from_str(ui_amount.amount.as_str()).context("parse post amount")?,
                    );
                } else {
                    token_balances.post_decimals.append_null();
                    token_balances.post_amount.append_null();
                }
            } else {
                token_balances.post_mint.append_null();
                token_balances.post_program_id.append_null();
                token_balances.post_owner.append_null();
                token_balances.post_decimals.append_null();
                token_balances.post_amount.append_null();
            }
        }

        if meta.pre_balances.len() != meta.post_balances.len() {
            return Err(anyhow!("length mismatch when parsing balances"));
        }

        for ((pre, post), acc) in meta
            .pre_balances
            .iter()
            .zip(meta.post_balances.iter())
            .zip(accounts.iter())
        {
            balances.block_slot.append_value(block_info.slot);
            balances.block_hash.append_value(block_info.hash.as_slice());
            balances.transaction_index.append_value(tx_index);
            balances.account.append_value(acc.as_slice());
            balances.pre.append_value(*pre);
            balances.post.append_value(*post);
        }

        transactions.block_slot.append_value(block_info.slot);
        transactions
            .block_hash
            .append_value(block_info.hash.as_slice());
        transactions.transaction_index.append_value(tx_index);
        transactions.signature.append_value(tx.signature.as_slice());
        transactions.version.append_null();
        transactions.account_keys.append_null();
        transactions.address_table_lookups.0.append_null();
        transactions.num_readonly_signed_accounts.append_null();
        transactions.num_readonly_unsigned_accounts.append_null();
        transactions.num_required_signatures.append_null();
        transactions.recent_blockhash.append_null();
        transactions.signatures.append_null();
        transactions.err.append_option(
            meta.err
                .as_ref()
                .map(|e| parse_transaction_err(e.err.as_slice()).context("parse tx err"))
                .transpose()?,
        );
        transactions.fee.append_value(meta.fee);
        transactions
            .compute_units_consumed
            .append_option(meta.compute_units_consumed);
        transactions.loaded_readonly_addresses.append_null();
        transactions.loaded_writable_addresses.append_null();
        transactions.fee_payer.append_null();
        transactions.has_dropped_log_messages.append_null();
    }

    Ok(())
}

fn parse_transaction_err(err: &[u8]) -> Result<String> {
    let err = bincode::deserialize::<solana_transaction_error::TransactionError>(err)
        .context("bincode serialize err")?;
    let err = serde_json::to_string(&err).context("json serialize")?;

    Ok(err)
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
