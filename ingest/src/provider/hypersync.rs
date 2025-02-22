use crate::{evm, DataStream, ProviderConfig, Query};
use anyhow::{anyhow, Context, Result};
use arrow::array::{builder, new_null_array, Array, BinaryArray, RecordBatch};
use arrow::datatypes::DataType;
use futures_lite::StreamExt as _;
use hypersync_client::net_types as hypersync_nt;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

pub fn query_to_hypersync(query: &evm::Query) -> Result<hypersync_nt::Query> {
    Ok(hypersync_nt::Query {
        from_block: query.from_block,
        to_block: query.to_block.map(|x| x+1),
        include_all_blocks: query.include_all_blocks,
        logs: query.logs.iter().map(|lg| Ok(hypersync_nt::LogSelection {
            address: lg.address.iter().map(|addr| addr.0.into()).collect::<Vec<_>>(),
            address_filter: None,
            topics: vec![
                lg.topic0.iter().map(|x| Ok(x.0.into())).chain(lg.event_signatures.iter().map(|sig| {
                    Ok(cherry_evm_decode::signature_to_topic0(sig).context("map signature to topic0")?.into())
                })).collect::<Result<Vec<_>>>()?,
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
        join_mode: hypersync_nt::JoinMode::Default,
        field_selection: hypersync_nt::FieldSelection {
            block: field_selection_to_vec(&query.fields.block),
            transaction: field_selection_to_vec(&query.fields.transaction),
            log: field_selection_to_vec(&query.fields.log),
            trace: field_selection_to_vec(&query.fields.trace),
        },
        ..Default::default()
    })
}

fn field_selection_to_vec<S: serde::Serialize>(field_selection: &S) -> BTreeSet<String> {
    let json = serde_json::to_value(field_selection).unwrap();
    let json = json.as_object().unwrap();

    let mut output = BTreeSet::new();

    for (key, value) in json.iter() {
        if value.as_bool().unwrap() {
            output.insert(key.clone());
        }
    }

    output
}

pub async fn start_stream(cfg: ProviderConfig) -> Result<DataStream> {
    match cfg.query {
        Query::Svm(_) => Err(anyhow!("svm is not supported by hypersync")),
        Query::Evm(evm_query) => {
            let evm_query = query_to_hypersync(&evm_query).context("convert to hypersync query")?;

            let client_config = hypersync_client::ClientConfig {
                url: match cfg.url {
                    Some(url) => Some(url.parse().context("parse url")?),
                    None => None,
                },
                bearer_token: cfg.bearer_token,
                http_req_timeout_millis: match cfg.http_req_timeout_millis {
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

            let receiver = client
                .stream_arrow(evm_query, Default::default())
                .await
                .context("start hypersync stream")?;

            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

            let stream = stream.map(|v| {
                v.and_then(|res| {
                    let mut data = BTreeMap::new();

                    data.insert(
                        "blocks".to_owned(),
                        map_blocks(&res.data.blocks).context("map blocks")?,
                    );
                    data.insert(
                        "transactions".to_owned(),
                        map_transactions(&res.data.transactions).context("map transactions")?,
                    );
                    data.insert(
                        "logs".to_owned(),
                        map_logs(&res.data.logs).context("map logs")?,
                    );
                    data.insert(
                        "traces".to_owned(),
                        map_traces(&res.data.traces).context("map traces")?,
                    );

                    Ok(data)
                })
            });

            Ok(Box::pin(stream))
        }
    }
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
                map_hypersync_binary_array_to_decimal256(&batch, "v", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "r", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "s", num_rows)?,
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
                new_null_array(
                    schema
                        .column_with_name("access_list")
                        .unwrap()
                        .1
                        .data_type(),
                    num_rows,
                ),
                map_hypersync_binary_array_to_decimal256(&batch, "l1_fee", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_gas_price", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_gas_used", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "l1_fee_scalar", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "gas_used_for_l1", num_rows)?,
                map_hypersync_binary_array_to_decimal256(&batch, "max_fee_per_blob_gas", num_rows)?,
                new_null_array(
                    schema
                        .column_with_name("blob_versioned_hashes")
                        .unwrap()
                        .1
                        .data_type(),
                    num_rows,
                ),
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
