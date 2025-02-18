use crate::{evm, DataStream, Format, StreamConfig};
use anyhow::{Context, Result};
use futures_lite::StreamExt;
use std::collections::BTreeMap;

use std::sync::Arc;

pub fn query_to_sqd(query: &evm::Query) -> Result<sqd_portal_client::evm::Query> {
    let hex_encode = |addr: &[u8]| format!("0x{}", faster_hex::hex_string(addr));

    let mut logs: Vec<_> = Vec::with_capacity(query.logs.len());

    for lg in query.logs.iter() {
        let mut topic0 = Vec::with_capacity(lg.topic0.len() + lg.event_signatures.len());

        topic0.extend_from_slice(lg.topic0.as_slice());

        for sig in lg.event_signatures.iter() {
            let t0 = cherry_evm_decode::signature_to_topic0(sig)
                .context("convert event signature to topic0")?;
            topic0.push(evm::Topic(t0));
        }

        let topic0 = topic0
            .into_iter()
            .map(|x| hex_encode(x.0.as_slice()))
            .collect::<Vec<_>>();

        logs.push(sqd_portal_client::evm::LogRequest {
            address: lg
                .address
                .iter()
                .map(|x| hex_encode(x.0.as_slice()))
                .collect(),
            topic0,
            topic1: lg
                .topic1
                .iter()
                .map(|x| hex_encode(x.0.as_slice()))
                .collect(),
            topic2: lg
                .topic2
                .iter()
                .map(|x| hex_encode(x.0.as_slice()))
                .collect(),
            topic3: lg
                .topic3
                .iter()
                .map(|x| hex_encode(x.0.as_slice()))
                .collect(),
            transaction: lg.include_transactions,
            transaction_logs: lg.include_transaction_logs,
            transaction_traces: lg.include_transaction_traces,
        });
    }

    Ok(sqd_portal_client::evm::Query {
        type_: Default::default(),
        from_block: query.from_block,
        to_block: query.to_block,
        include_all_blocks: query.include_all_blocks,
        transactions: query
            .transactions
            .iter()
            .map(|tx| sqd_portal_client::evm::TransactionRequest {
                from: tx
                    .from_
                    .iter()
                    .map(|x| hex_encode(x.0.as_slice()))
                    .collect(),
                to: tx.to.iter().map(|x| hex_encode(x.0.as_slice())).collect(),
                sighash: tx
                    .sighash
                    .iter()
                    .map(|x| hex_encode(x.0.as_slice()))
                    .collect(),
                logs: tx.include_logs,
                traces: tx.include_traces,
                state_diffs: false,
            })
            .collect(),
        logs,
        traces: query
            .traces
            .iter()
            .map(|t| sqd_portal_client::evm::TraceRequest {
                type_: t.type_.clone(),
                create_from: t.from_.iter().map(|x| hex_encode(x.0.as_slice())).collect(),
                call_from: t.from_.iter().map(|x| hex_encode(x.0.as_slice())).collect(),
                call_to: t.to.iter().map(|x| hex_encode(x.0.as_slice())).collect(),
                call_sighash: t
                    .sighash
                    .iter()
                    .map(|x| hex_encode(x.0.as_slice()))
                    .collect(),
                suicide_refund_address: t
                    .address
                    .iter()
                    .map(|x| hex_encode(x.0.as_slice()))
                    .collect(),
                reward_author: t
                    .author
                    .iter()
                    .map(|x| hex_encode(x.0.as_slice()))
                    .collect(),
                transaction: t.include_transactions,
                transaction_logs: t.include_transaction_logs,
                subtraces: t.include_transaction_traces,
                parents: t.include_transaction_traces,
            })
            .collect(),
        state_diffs: Vec::new(),
        fields: sqd_portal_client::evm::Fields {
            block: sqd_portal_client::evm::BlockFields {
                number: query.fields.block.number,
                hash: query.fields.block.hash,
                parent_hash: query.fields.block.parent_hash,
                timestamp: query.fields.block.timestamp,
                transactions_root: query.fields.block.transactions_root,
                receipts_root: query.fields.block.receipts_root,
                state_root: query.fields.block.state_root,
                logs_bloom: query.fields.block.logs_bloom,
                sha3_uncles: query.fields.block.sha3_uncles,
                extra_data: query.fields.block.extra_data,
                miner: query.fields.block.miner,
                nonce: query.fields.block.nonce,
                mix_hash: query.fields.block.mix_hash,
                size: query.fields.block.size,
                gas_limit: query.fields.block.gas_limit,
                gas_used: query.fields.block.gas_used,
                difficulty: query.fields.block.difficulty,
                total_difficulty: query.fields.block.total_difficulty,
                base_fee_per_gas: query.fields.block.base_fee_per_gas,
                blob_gas_used: query.fields.block.blob_gas_used,
                excess_blob_gas: query.fields.block.excess_blob_gas,
                l1_block_number: query.fields.block.l1_block_number,
            },
            transaction: sqd_portal_client::evm::TransactionFields {
                transaction_index: query.fields.transaction.transaction_index,
                hash: query.fields.transaction.hash,
                nonce: query.fields.transaction.nonce,
                from: query.fields.transaction.from_,
                to: query.fields.transaction.to,
                input: query.fields.transaction.input,
                value: query.fields.transaction.value,
                gas: query.fields.transaction.gas,
                gas_price: query.fields.transaction.gas_price,
                max_fee_per_gas: query.fields.transaction.max_fee_per_gas,
                max_priority_fee_per_gas: query.fields.transaction.max_priority_fee_per_gas,
                v: query.fields.transaction.v,
                r: query.fields.transaction.r,
                s: query.fields.transaction.s,
                y_parity: query.fields.transaction.y_parity,
                chain_id: query.fields.transaction.chain_id,
                sighash: query.fields.transaction.sighash,
                contract_address: query.fields.transaction.contract_address,
                gas_used: query.fields.transaction.gas_used,
                cumulative_gas_used: query.fields.transaction.cumulative_gas_used,
                effective_gas_price: query.fields.transaction.effective_gas_price,
                type_: query.fields.transaction.type_,
                status: query.fields.transaction.status,
                max_fee_per_blob_gas: query.fields.transaction.max_fee_per_blob_gas,
                blob_versioned_hashes: query.fields.transaction.blob_versioned_hashes,
                l1_fee: query.fields.transaction.l1_fee,
                l1_fee_scalar: query.fields.transaction.l1_fee_scalar,
                l1_gas_price: query.fields.transaction.l1_gas_price,
                l1_gas_used: false,
                l1_blob_base_fee: query.fields.transaction.l1_blob_base_fee,
                l1_blob_base_fee_scalar: query.fields.transaction.l1_blob_base_fee_scalar,
                l1_base_fee_scalar: query.fields.transaction.l1_base_fee_scalar,
            },
            log: sqd_portal_client::evm::LogFields {
                log_index: query.fields.log.log_index,
                transaction_index: query.fields.log.transaction_index,
                transaction_hash: query.fields.log.transaction_hash,
                address: query.fields.log.address,
                data: query.fields.log.data,
                topics: query.fields.log.topic0
                    || query.fields.log.topic1
                    || query.fields.log.topic2
                    || query.fields.log.topic3,
            },
            trace: sqd_portal_client::evm::TraceFields {
                transaction_index: query.fields.trace.transaction_position,
                trace_address: query.fields.trace.trace_address,
                subtraces: query.fields.trace.subtraces,
                type_: query.fields.trace.type_,
                error: query.fields.trace.error,
                revert_reason: query.fields.trace.error,
                create_from: query.fields.trace.from_,
                create_value: query.fields.trace.value,
                create_gas: query.fields.trace.gas,
                create_init: query.fields.trace.init,
                create_result_gas_used: query.fields.trace.gas_used,
                create_result_code: query.fields.trace.code,
                create_result_address: query.fields.trace.address,
                call_from: query.fields.trace.from_,
                call_to: query.fields.trace.to,
                call_value: query.fields.trace.value,
                call_gas: query.fields.trace.gas,
                call_input: query.fields.trace.input,
                call_sighash: query.fields.trace.sighash,
                call_type: query.fields.trace.type_,
                call_call_type: query.fields.trace.call_type,
                call_result_gas_used: query.fields.trace.gas_used,
                call_result_output: query.fields.trace.output,
                suicide_address: query.fields.trace.address,
                suicide_refund_address: query.fields.trace.refund_address,
                suicide_balance: query.fields.trace.balance,
                reward_author: query.fields.trace.author,
                reward_value: query.fields.trace.value,
                reward_type: query.fields.trace.author,
            },
        },
    })
}

pub fn start_stream(cfg: StreamConfig) -> Result<DataStream> {
    match cfg.format {
        Format::Evm(evm_query) => {
            let evm_query = query_to_sqd(&evm_query).context("convert to sqd query")?;

            let url = cfg
                .provider
                .url
                .context("url is required when using sqd")?
                .parse()
                .context("parse url")?;

            let mut client_config = sqd_portal_client::ClientConfig::default();

            if let Some(v) = cfg.provider.max_num_retries {
                client_config.max_num_retries = v;
            }
            if let Some(v) = cfg.provider.retry_backoff_ms {
                client_config.retry_backoff_ms = v;
            }
            if let Some(v) = cfg.provider.retry_base_ms {
                client_config.retry_base_ms = v;
            }
            if let Some(v) = cfg.provider.retry_ceiling_ms {
                client_config.retry_ceiling_ms = v;
            }
            if let Some(v) = cfg.provider.http_req_timeout_millis {
                client_config.http_req_timeout_millis = v;
            }

            let client = sqd_portal_client::Client::new(url, client_config);
            let client = Arc::new(client);

            let receiver = client.evm_arrow_finalized_stream(
                evm_query,
                sqd_portal_client::StreamConfig {
                    stop_on_head: true,
                    ..Default::default()
                },
            );

            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

            let stream = stream.map(|v| {
                v.map(|v| {
                    let mut data = BTreeMap::new();

                    data.insert("blocks".to_owned(), v.blocks);
                    data.insert("transactions".to_owned(), v.transactions);
                    data.insert("logs".to_owned(), v.logs);
                    data.insert("traces".to_owned(), v.traces);

                    data
                })
            });

            Ok(Box::pin(stream))
        }
    }
}
