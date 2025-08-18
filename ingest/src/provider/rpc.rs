use crate::{evm, DataStream, ProviderConfig, Query};
use anyhow::{anyhow, Context, Result};
use arrow::array::builder;
use arrow::array::{BinaryBuilder, BooleanBuilder, RecordBatch, UInt64Builder};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use cherry_evm_schema::{BlocksBuilder, LogsBuilder, TransactionsBuilder};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use alloy_primitives::{hex, Address, B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use serde::de::DeserializeOwned;

async fn rpc_call<T, P>(
    provider: &P,
    method: &'static str,
    params: serde_json::Value,
    cfg: &ProviderConfig,
) -> Result<T>
where
    T: DeserializeOwned + Send + Sync + Unpin + 'static + std::fmt::Debug,
    P: Provider,
{
    let max_retries = cfg.max_num_retries.unwrap_or(3);
    let base = cfg.retry_base_ms.unwrap_or(200);
    let ceiling = cfg.retry_ceiling_ms.unwrap_or(5_000);
    let mut delay = base;
    let mut attempt = 0;
    loop {
        match provider.client().request(method, params.clone()).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                attempt += 1;
                if attempt > max_retries {
                    return Err(anyhow!(
                        "rpc {} failed after {} retries: {}",
                        method,
                        max_retries,
                        e
                    ));
                }
                log::warn!("rpc {} failed (attempt {}): {}", method, attempt, e);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                delay = (delay.saturating_mul(2)).min(ceiling);
            }
        }
    }
}

// Helpers
fn u64_to_dec(v: u64) -> arrow::datatypes::i256 {
    let be = U256::from(v).to_be_bytes::<32>();
    arrow::datatypes::i256::from_be_bytes(be)
}

fn u256_to_dec(v: U256) -> arrow::datatypes::i256 {
    arrow::datatypes::i256::from_be_bytes(v.to_be_bytes::<32>())
}

fn b256_to_bin_opt(h: Option<B256>, b: &mut BinaryBuilder) {
    match h {
        Some(x) => b.append_value(x.as_slice()),
        None => b.append_null(),
    }
}

fn addr_opt_to_bin(a: Option<Address>, b: &mut BinaryBuilder) {
    match a {
        Some(x) => b.append_value(x.as_slice()),
        None => b.append_null(),
    }
}

pub async fn start_stream(cfg: ProviderConfig, query: crate::Query) -> Result<DataStream> {
    match query {
        Query::Svm(_) => Err(anyhow!("svm is not supported by rpc")),
        Query::Evm(query) => start_stream_evm(cfg, query).await,
    }
}

async fn start_stream_evm(cfg: ProviderConfig, query: evm::Query) -> Result<DataStream> {
    let url = cfg
        .url
        .clone()
        .context("rpc provider requires url in ProviderConfig")?;
    let provider = ProviderBuilder::new().on_http(url.parse().context("parse rpc url")?);

    // We stream in a single background task, batching by a reasonable chunk.
    let (tx, rx) =
        mpsc::channel::<Result<BTreeMap<String, RecordBatch>>>(cfg.buffer_size.unwrap_or(1));

    let include_all_blocks = query.include_all_blocks;
    let from_block = query.from_block;
    let to_block = query.to_block.unwrap_or(from_block);
    let log_filters = query.logs.clone();
    let tx_fields = query.fields.transaction;

    // Determine if receipts are actually needed based on requested fields
    let need_receipts = tx_fields.cumulative_gas_used
        || tx_fields.effective_gas_price
        || tx_fields.gas_used
        || tx_fields.contract_address
        || tx_fields.logs_bloom
        || tx_fields.root
        || tx_fields.status;

    // Determine if logs are needed
    let lf = query.fields.log;
    let need_logs = !log_filters.is_empty()
        || lf.removed
        || lf.log_index
        || lf.transaction_index
        || lf.transaction_hash
        || lf.block_hash
        || lf.block_number
        || lf.address
        || lf.data
        || lf.topic0
        || lf.topic1
        || lf.topic2
        || lf.topic3;

    tokio::spawn(async move {
        let chunk_size: u64 = 20;
        let mut start = from_block;
        while start <= to_block {
            let end = (start + chunk_size - 1).min(to_block);

            let res = fetch_batch(
                &provider,
                start,
                end,
                include_all_blocks,
                &log_filters,
                &cfg,
                need_receipts,
                need_logs,
            )
            .await;
            match res {
                Ok(data) => {
                    if tx.send(Ok(data)).await.is_err() {
                        log::trace!("rpc stream consumer dropped");
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }

            start = end + 1;
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(stream))
}

async fn fetch_batch<P: Provider>(
    provider: &P,
    from_block: u64,
    to_block: u64,
    include_all_blocks: bool,
    log_filters: &[evm::LogRequest],
    cfg: &ProviderConfig,
    need_receipts: bool,
    need_logs: bool,
) -> Result<BTreeMap<String, RecordBatch>> {
    // Fetch blocks with full transactions
    // alloy_provider supports get_block_by_number on RootProvider. We'll attempt to fetch sequentially with light concurrency.
    let block_numbers: Vec<u64> = (from_block..=to_block).collect();

    // Fetch blocks sequentially for simplicity and robustness
    let mut block_results = Vec::with_capacity(block_numbers.len());
    for n in block_numbers {
        // Using raw call for compatibility across alloy versions
        // eth_getBlockByNumber params: ["0x..", true]
        let tag = format!("0x{:x}", n);
        let block_value: serde_json::Value = rpc_call(
            provider,
            "eth_getBlockByNumber",
            serde_json::json!([tag, true]),
            cfg,
        )
        .await
        .context("eth_getBlockByNumber")?;
        block_results.push(block_value);
    }

    // Convert blocks and collect transactions
    let (blocks_batch, transactions, tx_receipt_hashes): (
        RecordBatch,
        Vec<serde_json::Value>,
        Vec<B256>,
    ) = convert_blocks_and_transactions(&block_results, include_all_blocks)?;

    // Fetch receipts for all txs we saw
    let mut receipts: Vec<serde_json::Value> = Vec::with_capacity(tx_receipt_hashes.len());
    if need_receipts {
        for h in tx_receipt_hashes.iter() {
            let tx_hash_hex = format!("0x{}", hex::encode(h.as_slice()));
            let receipt: serde_json::Value = rpc_call(
                provider,
                "eth_getTransactionReceipt",
                serde_json::json!([tx_hash_hex]),
                cfg,
            )
            .await
            .context("eth_getTransactionReceipt")?;
            receipts.push(receipt);
        }
    }

    let transactions_batch = convert_transactions(&transactions, &receipts)?;

    // Fetch logs
    let logs_batch = if need_logs {
        fetch_and_convert_logs(provider, from_block, to_block, log_filters, cfg).await?
    } else {
        // Return empty logs batch
        let schema = Arc::new(cherry_evm_schema::logs_schema());
        ArrowRecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanBuilder::new().finish()),
                Arc::new(UInt64Builder::new().finish()),
                Arc::new(UInt64Builder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(UInt64Builder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
                Arc::new(BinaryBuilder::new().finish()),
            ],
        )
        .unwrap()
    };

    // Currently traces not supported via generic RPC; return empty batch built by schema builder
    let empty_traces = cherry_evm_schema::TracesBuilder::default().finish();

    let mut out = BTreeMap::new();
    out.insert("blocks".to_string(), blocks_batch);
    out.insert("transactions".to_string(), transactions_batch);
    out.insert("logs".to_string(), logs_batch);
    out.insert("traces".to_string(), empty_traces);
    Ok(out)
}

fn hex_to_bytes_opt(v: &serde_json::Value) -> Option<Vec<u8>> {
    v.as_str().and_then(|s| {
        let s = s.strip_prefix("0x").unwrap_or(s);
        hex::decode(s).ok()
    })
}

fn hex_to_u64_opt(v: &serde_json::Value) -> Option<u64> {
    v.as_str()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
}

fn hex_to_u256_opt(v: &serde_json::Value) -> Option<U256> {
    v.as_str()
        .and_then(|s| U256::from_str_radix(s.trim_start_matches("0x"), 16).ok())
}

fn convert_blocks_and_transactions(
    blocks: &[serde_json::Value],
    include_all_blocks: bool,
) -> Result<(RecordBatch, Vec<serde_json::Value>, Vec<B256>)> {
    let mut bb = BlocksBuilder::default();
    let mut all_txs = Vec::new();
    let mut tx_hashes = Vec::new();

    for b in blocks {
        if b.is_null() {
            continue;
        }

        // Only append block if asked, but to keep behavior consistent with other providers, we include blocks when include_all_blocks=true.
        if include_all_blocks {
            // Minimal fields we can reliably fill; rest as nulls
            // number
            if let Some(n) = hex_to_u64_opt(&b["number"]) {
                bb.number.append_value(n);
            } else {
                bb.number.append_null();
            }

            // hashes
            match hex_to_bytes_opt(&b["hash"]) {
                Some(h) => bb.hash.append_value(&h),
                None => bb.hash.append_null(),
            }
            match hex_to_bytes_opt(&b["parentHash"]) {
                Some(h) => bb.parent_hash.append_value(&h),
                None => bb.parent_hash.append_null(),
            }

            // bloom, roots, miner
            match hex_to_bytes_opt(&b["logsBloom"]) {
                Some(v) => bb.logs_bloom.append_value(&v),
                None => bb.logs_bloom.append_null(),
            }
            match hex_to_bytes_opt(&b["transactionsRoot"]) {
                Some(v) => bb.transactions_root.append_value(&v),
                None => bb.transactions_root.append_null(),
            }
            match hex_to_bytes_opt(&b["stateRoot"]) {
                Some(v) => bb.state_root.append_value(&v),
                None => bb.state_root.append_null(),
            }
            match hex_to_bytes_opt(&b["receiptsRoot"]) {
                Some(v) => bb.receipts_root.append_value(&v),
                None => bb.receipts_root.append_null(),
            }
            match hex_to_bytes_opt(&b["miner"]) {
                Some(v) => bb.miner.append_value(&v),
                None => bb.miner.append_null(),
            }

            // numeric fields (Decimal256)
            match hex_to_u256_opt(&b["gasLimit"]) {
                Some(v) => bb.gas_limit.append_value(u256_to_dec(v)),
                None => bb.gas_limit.append_null(),
            }
            match hex_to_u256_opt(&b["gasUsed"]) {
                Some(v) => bb.gas_used.append_value(u256_to_dec(v)),
                None => bb.gas_used.append_null(),
            }
            match hex_to_u256_opt(&b["timestamp"]) {
                Some(v) => bb.timestamp.append_value(u256_to_dec(v)),
                None => bb.timestamp.append_null(),
            }

            // fill remaining optional fields as nulls
            bb.nonce.append_null();
            bb.sha3_uncles.append_null();
            bb.difficulty.append_null();
            bb.total_difficulty.append_null();
            bb.extra_data.append_null();
            bb.size.append_null();
            bb.uncles.append_null();
            bb.base_fee_per_gas.append_null();
            bb.blob_gas_used.append_null();
            bb.excess_blob_gas.append_null();
            bb.parent_beacon_block_root.append_null();
            bb.withdrawals_root.append_null();
            bb.withdrawals.0.append_null();
            bb.l1_block_number.append_null();
            bb.send_count.append_null();
            bb.send_root.append_null();
            bb.mix_hash.append_null();
        }

        // Collect transactions array entries
        if let Some(txs) = b.get("transactions").and_then(|v| v.as_array()) {
            for t in txs.iter() {
                if !t.is_null() {
                    if let Some(h) = t.get("hash").and_then(|x| x.as_str()) {
                        if let Ok(bytes) = hex::decode(h.trim_start_matches("0x")) {
                            if bytes.len() == 32 {
                                tx_hashes.push(B256::from_slice(&bytes));
                            }
                        }
                    }
                    all_txs.push(t.clone());
                }
            }
        }
    }

    Ok((bb.finish(), all_txs, tx_hashes))
}

fn convert_transactions(
    txs: &[serde_json::Value],
    receipts: &[serde_json::Value],
) -> Result<RecordBatch> {
    let mut tb = TransactionsBuilder::default();

    // Build a simple index by tx hash -> receipt
    use std::collections::HashMap;
    let mut receipt_map: HashMap<Vec<u8>, &serde_json::Value> = HashMap::new();
    for r in receipts {
        if let Some(h) = r.get("transactionHash").and_then(|x| hex_to_bytes_opt(x)) {
            receipt_map.insert(h, r);
        }
    }

    for t in txs {
        // hashes
        match hex_to_bytes_opt(&t["hash"]) {
            Some(v) => tb.hash.append_value(&v),
            None => tb.hash.append_null(),
        }
        match hex_to_bytes_opt(&t["from"]) {
            Some(v) => tb.from.append_value(&v),
            None => tb.from.append_null(),
        }
        match hex_to_bytes_opt(&t["to"]) {
            Some(v) => tb.to.append_value(&v),
            None => tb.to.append_null(),
        }

        // block refs
        match hex_to_bytes_opt(&t["blockHash"]) {
            Some(v) => tb.block_hash.append_value(&v),
            None => tb.block_hash.append_null(),
        }
        match hex_to_u64_opt(&t["blockNumber"]) {
            Some(v) => tb.block_number.append_value(v),
            None => tb.block_number.append_null(),
        }
        match hex_to_u64_opt(&t["transactionIndex"]) {
            Some(v) => tb.transaction_index.append_value(v),
            None => tb.transaction_index.append_null(),
        }

        // numeric
        match hex_to_u256_opt(&t["gas"]) {
            Some(v) => tb.gas.append_value(u256_to_dec(v)),
            None => tb.gas.append_null(),
        }
        match hex_to_u256_opt(&t["gasPrice"]) {
            Some(v) => tb.gas_price.append_value(u256_to_dec(v)),
            None => tb.gas_price.append_null(),
        }
        match hex_to_u256_opt(&t["value"]) {
            Some(v) => tb.value.append_value(u256_to_dec(v)),
            None => tb.value.append_null(),
        }
        match hex_to_u256_opt(&t["nonce"]) {
            Some(v) => tb.nonce.append_value(u256_to_dec(v)),
            None => tb.nonce.append_null(),
        }

        // input
        match hex_to_bytes_opt(&t["input"]) {
            Some(v) => tb.input.append_value(&v),
            None => tb.input.append_null(),
        }

        // Optional signature fields
        match hex_to_bytes_opt(&t["r"]) {
            Some(v) => tb.r.append_value(&v),
            None => tb.r.append_null(),
        }
        match hex_to_bytes_opt(&t["s"]) {
            Some(v) => tb.s.append_value(&v),
            None => tb.s.append_null(),
        }
        match hex_to_u64_opt(&t["v"]) {
            Some(v) => tb.v.append_value(v as u8),
            None => tb.v.append_null(),
        }

        // EIP-1559 fee fields (may be null on legacy)
        match hex_to_u256_opt(&t["maxPriorityFeePerGas"]) {
            Some(v) => tb.max_priority_fee_per_gas.append_value(u256_to_dec(v)),
            None => tb.max_priority_fee_per_gas.append_null(),
        }
        match hex_to_u256_opt(&t["maxFeePerGas"]) {
            Some(v) => tb.max_fee_per_gas.append_value(u256_to_dec(v)),
            None => tb.max_fee_per_gas.append_null(),
        }

        // Blob-related fields
        tb.max_fee_per_blob_gas.append_null();
        tb.blob_versioned_hashes.append_null();
        tb.deposit_nonce.append_null();
        tb.blob_gas_price.append_null();
        tb.deposit_receipt_version.append_null();
        tb.blob_gas_used.append_null();
        tb.l1_base_fee_scalar.append_null();
        tb.l1_blob_base_fee.append_null();
        tb.l1_blob_base_fee_scalar.append_null();
        tb.l1_block_number.append_null();
        tb.mint.append_null();
        tb.source_hash.append_null();

        // type
        match hex_to_u64_opt(&t["type"]) {
            Some(v) => tb.type_.append_value(v as u8),
            None => tb.type_.append_null(),
        }

        // Optional access list, yParity
        tb.access_list.0.append_null();
        tb.y_parity.append_null();

        // Receipt-augmented fields
        if let Some(h) = hex_to_bytes_opt(&t["hash"]) {
            if let Some(r) = receipt_map.get(&h) {
                match hex_to_bytes_opt(&r["contractAddress"]) {
                    Some(v) => tb.contract_address.append_value(&v),
                    None => tb.contract_address.append_null(),
                }
                match hex_to_bytes_opt(&r["logsBloom"]) {
                    Some(v) => tb.logs_bloom.append_value(&v),
                    None => tb.logs_bloom.append_null(),
                }
                match hex_to_u256_opt(&r["cumulativeGasUsed"]) {
                    Some(v) => tb.cumulative_gas_used.append_value(u256_to_dec(v)),
                    None => tb.cumulative_gas_used.append_null(),
                }
                match hex_to_u256_opt(&r["effectiveGasPrice"]) {
                    Some(v) => tb.effective_gas_price.append_value(u256_to_dec(v)),
                    None => tb.effective_gas_price.append_null(),
                }
                match hex_to_u256_opt(&r["gasUsed"]) {
                    Some(v) => tb.gas_used.append_value(u256_to_dec(v)),
                    None => tb.gas_used.append_null(),
                }
                match hex_to_bytes_opt(&r["root"]) {
                    Some(v) => tb.root.append_value(&v),
                    None => tb.root.append_null(),
                }
                match hex_to_u64_opt(&r["status"]) {
                    Some(v) => tb.status.append_value(v as u8),
                    None => tb.status.append_null(),
                }
            } else {
                // No receipt found, append nulls for all receipt fields
                tb.contract_address.append_null();
                tb.logs_bloom.append_null();
                tb.cumulative_gas_used.append_null();
                tb.effective_gas_price.append_null();
                tb.gas_used.append_null();
                tb.root.append_null();
                tb.status.append_null();
            }
        } else {
            // No tx hash? append nulls for all receipt fields
            tb.contract_address.append_null();
            tb.logs_bloom.append_null();
            tb.cumulative_gas_used.append_null();
            tb.effective_gas_price.append_null();
            tb.gas_used.append_null();
            tb.root.append_null();
            tb.status.append_null();
        }

        // Fields we don't populate via generic RPC (L2 specifics)
        tb.chain_id.append_null();
        tb.sighash.append_null();
        tb.l1_fee.append_null();
        tb.l1_gas_price.append_null();
        tb.l1_gas_used.append_null();
        tb.l1_fee_scalar.append_null();
        tb.gas_used_for_l1.append_null();
    }

    Ok(tb.finish())
}

async fn fetch_and_convert_logs<P: Provider>(
    provider: &P,
    from_block: u64,
    to_block: u64,
    log_filters: &[evm::LogRequest],
    cfg: &ProviderConfig,
) -> Result<RecordBatch> {
    // If filters provided, use them; else query all logs in range.
    // We'll call raw eth_getLogs to avoid version-specific type path issues.
    let mut all_logs: Vec<serde_json::Value> = Vec::new();

    // Build minimal filter object(s)
    if log_filters.is_empty() {
        let params = serde_json::json!({
            "fromBlock": format!("0x{:x}", from_block),
            "toBlock": format!("0x{:x}", to_block)
        });
        let logs: Vec<serde_json::Value> =
            rpc_call(provider, "eth_getLogs", serde_json::json!([params]), cfg)
                .await
                .context("eth_getLogs (no filters)")?;
        all_logs.extend(logs);
    } else {
        for q in log_filters {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "fromBlock".to_string(),
                serde_json::Value::String(format!("0x{:x}", from_block)),
            );
            obj.insert(
                "toBlock".to_string(),
                serde_json::Value::String(format!("0x{:x}", to_block)),
            );
            if !q.address.is_empty() {
                let addrs: Vec<String> = q
                    .address
                    .iter()
                    .map(|a| format!("0x{}", hex::encode(a.0)))
                    .collect();
                obj.insert(
                    "address".to_string(),
                    serde_json::Value::Array(
                        addrs.into_iter().map(serde_json::Value::String).collect(),
                    ),
                );
            }
            // topics structure: [topic0s, topic1s, topic2s, topic3s]
            let mut topics: Vec<serde_json::Value> = Vec::new();
            for ts in [&q.topic0, &q.topic1, &q.topic2, &q.topic3] {
                if ts.is_empty() {
                    topics.push(serde_json::Value::Null);
                } else {
                    let arr: Vec<serde_json::Value> = ts
                        .iter()
                        .map(|t| serde_json::Value::String(format!("0x{}", hex::encode(t.0))))
                        .collect();
                    topics.push(serde_json::Value::Array(arr));
                }
            }
            obj.insert("topics".to_string(), serde_json::Value::Array(topics));

            let logs: Vec<serde_json::Value> = rpc_call(
                provider,
                "eth_getLogs",
                serde_json::json!([serde_json::Value::Object(obj)]),
                cfg,
            )
            .await
            .context("eth_getLogs (filtered)")?;
            all_logs.extend(logs);
        }
    }

    // Convert logs
    let mut lb = LogsBuilder::default();
    for l in all_logs {
        // removed
        if let Some(b) = l.get("removed").and_then(|x| x.as_bool()) {
            lb.removed.append_value(b);
        } else {
            lb.removed.append_null();
        }
        match hex_to_u64_opt(&l["logIndex"]) {
            Some(v) => lb.log_index.append_value(v),
            None => lb.log_index.append_null(),
        }
        match hex_to_u64_opt(&l["transactionIndex"]) {
            Some(v) => lb.transaction_index.append_value(v),
            None => lb.transaction_index.append_null(),
        }
        match hex_to_bytes_opt(&l["transactionHash"]) {
            Some(v) => lb.transaction_hash.append_value(&v),
            None => lb.transaction_hash.append_null(),
        }
        match hex_to_bytes_opt(&l["blockHash"]) {
            Some(v) => lb.block_hash.append_value(&v),
            None => lb.block_hash.append_null(),
        }
        match hex_to_u64_opt(&l["blockNumber"]) {
            Some(v) => lb.block_number.append_value(v),
            None => lb.block_number.append_null(),
        }
        match hex_to_bytes_opt(&l["address"]) {
            Some(v) => lb.address.append_value(&v),
            None => lb.address.append_null(),
        }
        match hex_to_bytes_opt(&l["data"]) {
            Some(v) => lb.data.append_value(&v),
            None => lb.data.append_null(),
        }
        // topics 0..=3
        if let Some(arr) = l.get("topics").and_then(|x| x.as_array()) {
            for (i, dst) in [
                &mut lb.topic0,
                &mut lb.topic1,
                &mut lb.topic2,
                &mut lb.topic3,
            ]
            .into_iter()
            .enumerate()
            {
                if let Some(val) = arr.get(i) {
                    match hex_to_bytes_opt(val) {
                        Some(v) => dst.append_value(&v),
                        None => dst.append_null(),
                    }
                } else {
                    dst.append_null();
                }
            }
        } else {
            lb.topic0.append_null();
            lb.topic1.append_null();
            lb.topic2.append_null();
            lb.topic3.append_null();
        }
    }

    Ok(lb.finish())
}
