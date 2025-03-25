use super::common::field_selection_to_set;
use crate::{evm, DataStream, ProviderConfig, Query};
use anyhow::{anyhow, Context, Result};
use arrow::array::ListBuilder;
use arrow::array::{builder, new_null_array, Array, BinaryArray, BinaryBuilder, RecordBatch};
use arrow::datatypes::DataType;
use cherry_evm_schema::AccessListBuilder;
use hypersync_client::format::AccessList;
use hypersync_client::net_types::{self as hypersync_nt, JoinMode};
use std::sync::Arc;
use std::{collections::BTreeMap, num::NonZeroU64, time::Duration};
use tokio::sync::mpsc;

pub fn query_to_hypersync(query: &evm::Query) -> Result<hypersync_nt::Query> {
    let mut need_join_default = false;
    let mut need_join_all = false;

    for lg in query.logs.iter() {
        if lg.include_transactions || lg.include_blocks || lg.include_transaction_traces {
            need_join_default = true;
        }

        if lg.include_transaction_logs {
            need_join_all = true;
        }
    }

    for tx in query.transactions.iter() {
        if tx.include_traces || tx.include_blocks {
            need_join_default = true;
        }

        if tx.include_logs {
            need_join_all = true;
        }
    }

    for trc in query.traces.iter() {
        if trc.include_blocks {
            need_join_default = true;
        }

        if trc.include_transaction_logs
            || trc.include_transactions
            || trc.include_transaction_traces
        {
            need_join_all = true;
        }
    }

    let join_mode = if need_join_all {
        JoinMode::JoinAll
    } else if need_join_default {
        JoinMode::Default
    } else {
        JoinMode::JoinNothing
    };

    Ok(hypersync_nt::Query {
        from_block: query.from_block,
        to_block: query.to_block.map(|x| x+1),
        include_all_blocks: query.include_all_blocks,
        logs: query.logs.iter().map(|lg| Ok(hypersync_nt::LogSelection {
            address: lg.address.iter().map(|addr| addr.0.into()).collect::<Vec<_>>(),
            address_filter: None,
            topics: vec![
                lg.topic0.iter().map(|x| x.0.into()).collect(),
                lg.topic1.iter().map(|x| x.0.into()).collect(),
                lg.topic2.iter().map(|x| x.0.into()).collect(),
                lg.topic3.iter().map(|x| x.0.into()).collect(),
            ].as_slice().try_into().unwrap(),
        })).collect::<Result<_>>()?,
        transactions: query.transactions.iter().map(|tx| Ok(hypersync_nt::TransactionSelection {
            from: tx.from_.iter().map(|x| x.0.into()).collect(),
            to: tx.to.iter().map(|x| x.0.into()).collect(),
            sighash: tx.sighash.iter().map(|x| x.0.into()).collect(),
            status: if tx.status.len() == 1 {
                Some(*tx.status.first().unwrap())
            } else if tx.status.is_empty() {
                None
            } else {
                return Err(anyhow!("failed to convert status query to hypersync. Only empty or single element arrays are supported."))
            },
            kind: tx.type_.clone(),
            contract_address: tx.contract_deployment_address.iter().map(|x| x.0.into()).collect(),
            hash: tx.hash.iter().map(|x| x.0.into()).collect(),
            ..Default::default()
        })).collect::<Result<_>>()?,
        traces: query.traces.iter().map(|trc| Ok(hypersync_nt::TraceSelection {
            from: trc.from_.iter().map(|x| x.0.into()).collect(),
            to: trc.to.iter().map(|x| x.0.into()).collect(),
            address: trc.address.iter().map(|x| x.0.into()).collect(),
            call_type: trc.call_type.clone(),
            reward_type: trc.reward_type.clone(),
            kind: trc.type_.clone(),
            sighash: trc.sighash.iter().map(|x| x.0.into()).collect(),
            ..Default::default()
        })).collect::<Result<_>>()?,
        join_mode,
        field_selection: hypersync_nt::FieldSelection {
            block: field_selection_to_set(&query.fields.block),
            transaction: field_selection_to_set(&query.fields.transaction),
            log: field_selection_to_set(&query.fields.log),
            trace: field_selection_to_set(&query.fields.trace),
        },
        ..Default::default()
    })
}

pub async fn start_stream(cfg: ProviderConfig, query: crate::Query) -> Result<DataStream> {
    match query {
        Query::Svm(_) => Err(anyhow!("svm is not supported by hypersync")),
        Query::Evm(query) => {
            let evm_query = query_to_hypersync(&query).context("convert to hypersync query")?;
            let client_config = hypersync_client::ClientConfig {
                url: match cfg.url {
                    Some(url) => Some(url.parse().context("parse url")?),
                    None => None,
                },
                bearer_token: cfg.bearer_token,
                http_req_timeout_millis: match cfg.req_timeout_millis {
                    Some(x) => Some(
                        NonZeroU64::new(x).context("check http_req_timeout_millis isn't zero")?,
                    ),
                    None => None,
                },
                max_num_retries: cfg.max_num_retries,
                retry_backoff_ms: cfg.retry_backoff_ms,
                retry_base_ms: cfg.retry_base_ms,
                retry_ceiling_ms: cfg.retry_ceiling_ms,
            };

            let client =
                hypersync_client::Client::new(client_config).context("init hypersync client")?;
            let client = Arc::new(client);

            let chain_id = client.get_chain_id().await.context("get chain id")?;
            let rollback_offset = make_rollback_offset(chain_id);

            let (tx, rx) = mpsc::channel(cfg.buffer_size.unwrap_or(10));

            let height = client.get_height().await.context("get height")?;

            let mut evm_query = evm_query;
            let original_to_block = evm_query.to_block;
            evm_query.to_block = Some(make_to_block(original_to_block, height, rollback_offset));

            let mut receiver = client
                .clone()
                .stream_arrow(evm_query.clone(), Default::default())
                .await
                .context("start hypersync stream")?;

            tokio::spawn(async move {
                while let Some(res) = receiver.recv().await {
                    let res =
                        res.and_then(|r| map_response(&r.data).context("map data").map(|x| (r, x)));
                    match res {
                        Ok((r, data)) => {
                            evm_query.from_block = r.next_block;
                            if tx.send(Ok(data)).await.is_err() {
                                log::trace!("quitting ingest loop because the receiver is dropped");
                                return;
                            }
                        }
                        Err(e) => {
                            tx.send(Err(e)).await.ok();
                            return;
                        }
                    }
                }

                if let Some(tb) = original_to_block {
                    if evm_query.from_block == tb {
                        return;
                    }
                }

                if cfg.stop_on_head {
                    return;
                }

                let head_poll_interval =
                    Duration::from_millis(cfg.head_poll_interval_millis.unwrap_or(1_000));

                let r = chain_head_stream(
                    &client,
                    original_to_block,
                    &mut evm_query,
                    &tx,
                    rollback_offset,
                    head_poll_interval,
                )
                .await;

                if let Err(e) = r {
                    tx.send(Err(e)).await.ok();
                }
            });

            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

            Ok(Box::pin(stream))
        }
    }
}

async fn chain_head_stream(
    client: &hypersync_client::Client,
    original_to_block: Option<u64>,
    evm_query: &mut hypersync_client::net_types::Query,
    tx: &mpsc::Sender<Result<BTreeMap<String, RecordBatch>>>,
    rollback_offset: u64,
    head_poll_interval: Duration,
) -> Result<()> {
    let mut height = client.get_height().await.context("get height")?;

    loop {
        while height.saturating_sub(rollback_offset) < evm_query.from_block {
            log::debug!(
                "waiting for block: {}. server is at block {}",
                evm_query.from_block,
                height
            );
            tokio::time::sleep(head_poll_interval).await;
            height = client.get_height().await.context("get height")?;
        }

        evm_query.to_block = Some(make_to_block(original_to_block, height, rollback_offset));

        let res = client.get_arrow(evm_query).await.context("run query")?;

        let data = map_response(&res.data).context("map data")?;

        if tx.send(Ok(data)).await.is_err() {
            log::trace!("quitting chain head stream since the receiver is dropped");
            return Ok(());
        }

        evm_query.from_block = res.next_block;
        height = res.archive_height.unwrap_or(0);

        if let Some(tb) = original_to_block {
            if tb == res.next_block {
                return Ok(());
            }
        }
    }
}

fn make_to_block(original_to_block: Option<u64>, height: u64, rollback_offset: u64) -> u64 {
    let safe_height = height.saturating_sub(rollback_offset);
    match original_to_block {
        Some(tb) => tb.min(safe_height + 1),
        None => safe_height + 1,
    }
}

fn make_rollback_offset(_chain_id: u64) -> u64 {
    200
}

fn map_response(
    resp: &hypersync_client::ArrowResponseData,
) -> Result<BTreeMap<String, RecordBatch>> {
    let mut data = BTreeMap::new();

    data.insert(
        "blocks".to_owned(),
        map_blocks(&resp.blocks).context("map blocks")?,
    );
    data.insert(
        "transactions".to_owned(),
        map_transactions(&resp.transactions).context("map transactions")?,
    );
    data.insert("logs".to_owned(), map_logs(&resp.logs).context("map logs")?);
    data.insert(
        "traces".to_owned(),
        map_traces(&resp.traces).context("map traces")?,
    );

    Ok(data)
}

fn map_hypersync_array(
    batch: &RecordBatch,
    name: &str,
    num_rows: usize,
    data_type: &DataType,
) -> Result<Arc<dyn Array>> {
    let arr = match batch.column_by_name(name) {
        Some(arr) => Arc::clone(arr),
        None => new_null_array(data_type, num_rows),
    };
    if arr.data_type() != data_type {
        return Err(anyhow!(
            "expected column {} to be of type {} but got {} instead",
            name,
            data_type,
            arr.data_type()
        ));
    }
    Ok(arr)
}

fn map_hypersync_binary_array_to_u8(
    batch: &RecordBatch,
    name: &str,
    num_rows: usize,
) -> Result<Arc<dyn Array>> {
    let arr = map_hypersync_array(batch, name, num_rows, &DataType::Binary)?;
    let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
    let arr = cherry_cast::u256_column_from_binary(arr)
        .with_context(|| format!("parse u256 values in {} column", name))?;
    let arr: &dyn Array = &arr;
    let arr = arrow::compute::cast(arr, &DataType::UInt8)
        .with_context(|| format!("cast u256 to u8 for column {}", name))?;
    Ok(Arc::new(arr))
}

fn map_hypersync_binary_array_to_decimal256(
    batch: &RecordBatch,
    name: &str,
    num_rows: usize,
) -> Result<Arc<dyn Array>> {
    let arr = map_hypersync_array(batch, name, num_rows, &DataType::Binary)?;
    let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
    let arr = cherry_cast::u256_column_from_binary(arr)
        .with_context(|| format!("parse u256 values in {} column", name))?;
    Ok(Arc::new(arr))
}

fn map_hypersync_u8_binary_array_to_boolean(
    batch: &RecordBatch,
    name: &str,
    num_rows: usize,
) -> Result<Arc<dyn Array>> {
    let src = match batch.column_by_name(name) {
        Some(arr) => Arc::clone(arr),
        None => return Ok(new_null_array(&DataType::Boolean, num_rows)),
    };
    let src = src
        .as_any()
        .downcast_ref::<BinaryArray>()
        .with_context(|| format!("expected {} column to be binary type", name))?;

    let mut arr = builder::BooleanBuilder::with_capacity(src.len());

    for v in src.iter() {
        match v {
            None => arr.append_null(),
            Some(v) => match v {
                [0] => arr.append_value(false),
                [1] => arr.append_value(true),
                _ => {
                    return Err(anyhow!(
                        "column {} has an invalid value {}. All values should be zero or one.",
                        name,
                        faster_hex::hex_string(v),
                    ))
                }
            },
        }
    }
    Ok(Arc::new(arr.finish()))
}

fn map_hypersync_binary_array_to_access_list_elem(
    batch: &RecordBatch,
    name: &str,
    num_rows: usize,
) -> Result<Arc<dyn Array>> {
    let arr = map_hypersync_array(batch, name, num_rows, &DataType::Binary)?;
    let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut access_list_builder = AccessListBuilder::default();

    // Process each element in the array
    for opt_value in arr.iter() {
        match opt_value {
            None => access_list_builder.0.append_null(),
            Some(bytes) => {
                let access_list_wrapper = bincode::deserialize::<Vec<AccessList>>(bytes)
                    .context("deserialize access list failed")?;
                let access_list_items_builder = access_list_builder.0.values();
                for item in access_list_wrapper {
                    access_list_items_builder
                        .field_builder::<builder::BinaryBuilder>(0)
                        .unwrap()
                        .append_option(item.address);

                    {
                        let b = access_list_items_builder
                            .field_builder::<builder::ListBuilder<builder::BinaryBuilder>>(1)
                            .unwrap();

                        let v = item.storage_keys;
                        let mut keys = vec![];
                        if let Some(v) = v {
                            for x in v {
                                keys.push(Some(x.to_vec()));
                            }
                            b.append_value(keys);
                        } else {
                            b.append_null();
                        }
                    }
                    access_list_items_builder.append(true);
                }
                access_list_builder.0.append(true);
            }
        }
    }
    let access_list_array = access_list_builder.0.finish();
    let access_list_array = Arc::new(access_list_array);
    // Create and return the struct array
    Ok(access_list_array)
}

fn map_hypersync_binary_array_to_list_hashes(
    batch: &RecordBatch,
    name: &str,
    num_rows: usize,
) -> Result<Arc<dyn Array>> {
    let arr = map_hypersync_array(batch, name, num_rows, &DataType::Binary)?;
    let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut hashes_list_builder = ListBuilder::<BinaryBuilder>::new(BinaryBuilder::default());
    for opt_value in arr.iter() {
        match opt_value {
            None => hashes_list_builder.append_null(),
            Some(bytes) => {
                if bytes.len() % 32 != 0 {
                    return Err(anyhow!("invalid blob versioned hash: input length {} is not a multiple of 32 bytes", bytes.len()));
                }

                let values: Vec<_> = bytes
                    .chunks_exact(32)
                    .map(|chunk| Some(chunk.to_vec()))
                    .collect();

                if values.is_empty() {
                    hashes_list_builder.append_null();
                } else {
                    hashes_list_builder.append_value(values);
                }
            }
        }
    }
    Ok(Arc::new(hashes_list_builder.finish()))
}

fn map_blocks(blocks: &[hypersync_client::ArrowBatch]) -> Result<RecordBatch> {
    let mut batches = Vec::with_capacity(blocks.len());

    let schema = Arc::new(cherry_evm_schema::blocks_schema());

    for batch in blocks.iter() {
        let batch = polars_arrow_to_arrow_rs(batch);
        let num_rows = batch.num_rows();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                map_hypersync_array(&batch, "number", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "parent_hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "nonce", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "sha3_uncles", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "logs_bloom", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "transactions_root", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "state_root", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "receipts_root", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "miner", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "difficulty", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "total_difficulty", num_rows)?,
                map_hypersync_array(&batch, "extra_data", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "size", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_limit", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_used", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "timestamp", num_rows)?,
                new_null_array(
                    schema.column_with_name("uncles").unwrap().1.data_type(),
                    num_rows,
                ),
                map_hypersync_binary_array_to_decimal256(&batch, "base_fee_per_gas", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "blob_gas_used", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "excess_blob_gas", num_rows)?,
                map_hypersync_array(
                    &batch,
                    "parent_beacon_block_root",
                    num_rows,
                    &DataType::Binary,
                )?,
                map_hypersync_array(&batch, "withdrawals_root", num_rows, &DataType::Binary)?,
                new_null_array(
                    schema
                        .column_with_name("withdrawals")
                        .unwrap()
                        .1
                        .data_type(),
                    num_rows,
                ),
                map_hypersync_array(&batch, "l1_block_number", num_rows, &DataType::UInt64)?,
                map_hypersync_binary_array_to_decimal256(&batch, "send_count", num_rows)?,
                map_hypersync_array(&batch, "send_root", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "mix_hash", num_rows, &DataType::Binary)?,
            ],
        )
        .context("map hypersync columns to common format")?;
        batches.push(batch);
    }

    arrow::compute::concat_batches(&schema, batches.iter()).context("concat batches")
}

fn map_transactions(transactions: &[hypersync_client::ArrowBatch]) -> Result<RecordBatch> {
    let mut batches = Vec::with_capacity(transactions.len());

    let schema = Arc::new(cherry_evm_schema::transactions_schema());

    for batch in transactions.iter() {
        let batch = polars_arrow_to_arrow_rs(batch);
        let num_rows = batch.num_rows();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                map_hypersync_array(&batch, "block_hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "block_number", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "from", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_price", num_rows)?,
                map_hypersync_array(&batch, "hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "input", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "nonce", num_rows)?,
                map_hypersync_array(&batch, "to", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "transaction_index", num_rows, &DataType::UInt64)?,
                map_hypersync_binary_array_to_decimal256(&batch, "value", num_rows)?,
                map_hypersync_binary_array_to_u8(&batch, "v", num_rows)?,
                map_hypersync_array(&batch, "r", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "s", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(
                    &batch,
                    "max_priority_fee_per_gas",
                    num_rows,
                )?,
                map_hypersync_binary_array_to_decimal256(&batch, "max_fee_per_gas", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "chain_id", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "cumulative_gas_used", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "effective_gas_price", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_used", num_rows)?,
                map_hypersync_array(&batch, "contract_address", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "logs_bloom", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "type", num_rows, &DataType::UInt8)?,
                map_hypersync_array(&batch, "root", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "status", num_rows, &DataType::UInt8)?,
                map_hypersync_array(&batch, "sighash", num_rows, &DataType::Binary)?,
                map_hypersync_u8_binary_array_to_boolean(&batch, "y_parity", num_rows)?,
                map_hypersync_binary_array_to_access_list_elem(&batch, "access_list", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_fee", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_gas_price", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_gas_used", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_fee_scalar", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_used_for_l1", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "max_fee_per_blob_gas", num_rows)?,
                map_hypersync_binary_array_to_list_hashes(
                    &batch,
                    "blob_versioned_hashes",
                    num_rows,
                )?,
                map_hypersync_binary_array_to_decimal256(&batch, "deposit_nonce", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "blob_gas_price", num_rows)?,
                map_hypersync_binary_array_to_decimal256(
                    &batch,
                    "deposit_receipt_version",
                    num_rows,
                )?,
                map_hypersync_binary_array_to_decimal256(&batch, "blob_gas_used", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_base_fee_scalar", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_blob_base_fee", num_rows)?,
                map_hypersync_binary_array_to_decimal256(
                    &batch,
                    "l1_blob_base_fee_scalar",
                    num_rows,
                )?,
                arrow::compute::cast_with_options(
                    &map_hypersync_binary_array_to_decimal256(&batch, "l1_block_number", num_rows)?,
                    &DataType::UInt64,
                    &arrow::compute::CastOptions {
                        safe: true,
                        ..Default::default()
                    },
                )
                .context("cast l1_block_number column from decimal256 to uint64")?,
                map_hypersync_binary_array_to_decimal256(&batch, "mint", num_rows)?,
                map_hypersync_array(&batch, "source_hash", num_rows, &DataType::Binary)?,
            ],
        )
        .context("map hypersync columns to common format")?;
        batches.push(batch);
    }

    arrow::compute::concat_batches(&schema, batches.iter()).context("concat batches")
}

fn map_logs(logs: &[hypersync_client::ArrowBatch]) -> Result<RecordBatch> {
    let mut batches = Vec::with_capacity(logs.len());

    let schema = Arc::new(cherry_evm_schema::logs_schema());

    for batch in logs.iter() {
        let batch = polars_arrow_to_arrow_rs(batch);
        let num_rows = batch.num_rows();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                map_hypersync_array(&batch, "removed", num_rows, &DataType::Boolean)?,
                map_hypersync_array(&batch, "log_index", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "transaction_index", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "transaction_hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "block_hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "block_number", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "address", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "data", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "topic0", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "topic1", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "topic2", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "topic3", num_rows, &DataType::Binary)?,
            ],
        )
        .context("map hypersync columns to common format")?;
        batches.push(batch);
    }

    arrow::compute::concat_batches(&schema, batches.iter()).context("concat batches")
}

fn map_traces(traces: &[hypersync_client::ArrowBatch]) -> Result<RecordBatch> {
    let mut batches = Vec::with_capacity(traces.len());

    let schema = Arc::new(cherry_evm_schema::traces_schema());

    for batch in traces.iter() {
        let batch = polars_arrow_to_arrow_rs(batch);
        let num_rows = batch.num_rows();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                map_hypersync_array(&batch, "from", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "to", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "call_type", num_rows, &DataType::Utf8)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas", num_rows)?,
                map_hypersync_array(&batch, "input", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "init", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "value", num_rows)?,
                map_hypersync_array(&batch, "author", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "reward_type", num_rows, &DataType::Utf8)?,
                map_hypersync_array(&batch, "block_hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "block_number", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "address", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "code", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_used", num_rows)?,
                map_hypersync_array(&batch, "output", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "subtraces", num_rows, &DataType::UInt64)?,
                new_null_array(
                    schema
                        .column_with_name("trace_address")
                        .unwrap()
                        .1
                        .data_type(),
                    num_rows,
                ),
                map_hypersync_array(&batch, "transaction_hash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "transaction_position", num_rows, &DataType::UInt64)?,
                map_hypersync_array(&batch, "type", num_rows, &DataType::Utf8)?,
                map_hypersync_array(&batch, "error", num_rows, &DataType::Utf8)?,
                map_hypersync_array(&batch, "sighash", num_rows, &DataType::Binary)?,
                map_hypersync_array(&batch, "action_address", num_rows, &DataType::Binary)?,
                map_hypersync_binary_array_to_decimal256(&batch, "balance", num_rows)?,
                map_hypersync_array(&batch, "refund_address", num_rows, &DataType::Binary)?,
            ],
        )
        .context("map hypersync columns to common format")?;
        batches.push(batch);
    }

    arrow::compute::concat_batches(&schema, batches.iter()).context("concat batches")
}

fn polars_arrow_to_arrow_rs(
    batch: &hypersync_client::ArrowBatch,
) -> arrow::record_batch::RecordBatch {
    let data_type = polars_arrow::datatypes::ArrowDataType::Struct(batch.schema.fields.clone());
    let arr = polars_arrow::array::StructArray::new(
        data_type.clone(),
        batch.chunk.columns().to_vec(),
        None,
    );

    let arr: arrow::ffi::FFI_ArrowArray =
        unsafe { std::mem::transmute(polars_arrow::ffi::export_array_to_c(Box::new(arr))) };
    let schema: arrow::ffi::FFI_ArrowSchema = unsafe {
        std::mem::transmute(polars_arrow::ffi::export_field_to_c(
            &polars_arrow::datatypes::Field::new("", data_type, false),
        ))
    };

    let mut arr_data = unsafe { arrow::ffi::from_ffi(arr, &schema).unwrap() };

    arr_data.align_buffers();

    let arr = arrow::array::StructArray::from(arr_data);

    arrow::record_batch::RecordBatch::from(arr)
}
