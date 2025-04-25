use crate::{evm, svm, DataStream, ProviderConfig, Query};
use anyhow::{Context, Result};
use futures_lite::StreamExt;
use std::collections::BTreeMap;

use std::sync::Arc;

fn svm_query_to_sqd(query: &svm::Query) -> Result<sqd_portal_client::svm::Query> {
    let base58_encode = |addr: &[u8]| {
        bs58::encode(addr)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    };
    let hex_encode = |addr: &[u8]| format!("0x{}", faster_hex::hex_string(addr));

    Ok(sqd_portal_client::svm::Query {
        type_: Default::default(),
        from_block: query.from_block,
        to_block: query.to_block,
        include_all_blocks: query.include_all_blocks,
        fields: sqd_portal_client::svm::Fields {
            instruction: sqd_portal_client::svm::InstructionFields {
                transaction_index: query.fields.instruction.transaction_index,
                instruction_address: query.fields.instruction.instruction_address,
                program_id: query.fields.instruction.program_id,
                accounts: query.fields.instruction.a0
                    || query.fields.instruction.a1
                    || query.fields.instruction.a2
                    || query.fields.instruction.a3
                    || query.fields.instruction.a4
                    || query.fields.instruction.a5
                    || query.fields.instruction.a6
                    || query.fields.instruction.a7
                    || query.fields.instruction.a8
                    || query.fields.instruction.a9
                    || query.fields.instruction.rest_of_accounts,
                data: query.fields.instruction.data,
                d1: query.fields.instruction.d1,
                d2: query.fields.instruction.d2,
                d4: query.fields.instruction.d4,
                d8: query.fields.instruction.d8,
                error: query.fields.instruction.error,
                compute_units_consumed: query.fields.instruction.compute_units_consumed,
                is_committed: query.fields.instruction.is_committed,
                has_dropped_log_messages: query.fields.instruction.has_dropped_log_messages,
            },
            transaction: sqd_portal_client::svm::TransactionFields {
                transaction_index: query.fields.transaction.transaction_index,
                version: query.fields.transaction.version,
                account_keys: query.fields.transaction.account_keys,
                address_table_lookups: query.fields.transaction.address_table_lookups,
                num_readonly_signed_accounts: query.fields.transaction.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: query
                    .fields
                    .transaction
                    .num_readonly_unsigned_accounts,
                num_required_signatures: query.fields.transaction.num_required_signatures,
                recent_blockhash: query.fields.transaction.recent_blockhash,
                signatures: query.fields.transaction.signatures
                    || query.fields.transaction.signature,
                err: query.fields.transaction.err,
                fee: query.fields.transaction.fee,
                compute_units_consumed: query.fields.transaction.compute_units_consumed,
                loaded_addresses: query.fields.transaction.loaded_readonly_addresses
                    || query.fields.transaction.loaded_writable_addresses,
                fee_payer: query.fields.transaction.fee_payer,
                has_dropped_log_messages: query.fields.transaction.has_dropped_log_messages,
            },
            log: sqd_portal_client::svm::LogFields {
                transaction_index: query.fields.log.transaction_index,
                log_index: query.fields.log.log_index,
                instruction_address: query.fields.log.instruction_address,
                program_id: query.fields.log.program_id,
                kind: query.fields.log.kind,
                message: query.fields.log.message,
            },
            balance: sqd_portal_client::svm::BalanceFields {
                transaction_index: query.fields.balance.transaction_index,
                account: query.fields.balance.account,
                pre: query.fields.balance.pre,
                post: query.fields.balance.post,
            },
            token_balance: sqd_portal_client::svm::TokenBalanceFields {
                transaction_index: query.fields.token_balance.transaction_index,
                account: query.fields.token_balance.account,
                pre_mint: query.fields.token_balance.pre_mint,
                post_mint: query.fields.token_balance.post_mint,
                pre_decimals: query.fields.token_balance.pre_decimals,
                post_decimals: query.fields.token_balance.post_decimals,
                pre_program_id: query.fields.token_balance.pre_program_id,
                post_program_id: query.fields.token_balance.post_program_id,
                pre_owner: query.fields.token_balance.pre_owner,
                post_owner: query.fields.token_balance.post_owner,
                pre_amount: query.fields.token_balance.pre_amount,
                post_amount: query.fields.token_balance.post_amount,
            },
            reward: sqd_portal_client::svm::RewardFields {
                pubkey: query.fields.reward.pubkey,
                lamports: query.fields.reward.lamports,
                post_balance: query.fields.reward.post_balance,
                reward_type: query.fields.reward.reward_type,
                commission: query.fields.reward.commission,
            },
            block: sqd_portal_client::svm::BlockFields {
                number: query.fields.block.slot
                    || query.fields.instruction.block_slot
                    || query.fields.transaction.block_slot
                    || query.fields.log.block_slot
                    || query.fields.balance.block_slot
                    || query.fields.token_balance.block_slot
                    || query.fields.reward.block_slot,
                hash: query.fields.block.hash
                    || query.fields.instruction.block_hash
                    || query.fields.transaction.block_hash
                    || query.fields.log.block_hash
                    || query.fields.balance.block_hash
                    || query.fields.token_balance.block_hash
                    || query.fields.reward.block_hash,
                timestamp: query.fields.block.timestamp,
                parent_hash: query.fields.block.parent_hash,
                parent_number: query.fields.block.parent_slot,
            },
        },
        instructions: query
            .instructions
            .iter()
            .map(|inst| {
                let mut d1: Vec<String> =
                    inst.d1.iter().map(|v| hex_encode(v.0.as_slice())).collect();
                let mut d2: Vec<String> =
                    inst.d2.iter().map(|v| hex_encode(v.0.as_slice())).collect();
                let mut d3: Vec<String> =
                    inst.d3.iter().map(|v| hex_encode(v.0.as_slice())).collect();
                let mut d4: Vec<String> =
                    inst.d4.iter().map(|v| hex_encode(v.0.as_slice())).collect();
                let mut d8: Vec<String> =
                    inst.d8.iter().map(|v| hex_encode(v.0.as_slice())).collect();
                inst.discriminator.iter().for_each(|d| {
                    match d.0.len() {
                        0 => {}
                        1 => {
                            d1.push(hex_encode(d.0.as_slice()));
                        }
                        2 => {
                            d2.push(hex_encode(d.0.as_slice()));
                        }
                        3 => {
                            d3.push(hex_encode(d.0.as_slice()));
                        }
                        4 => {
                            d4.push(hex_encode(d.0.as_slice()));
                        }
                        5..=7 => {
                            let slice = &d.0[..4.min(d.0.len())];
                            d4.push(hex_encode(slice));
                        }
                        _ => {
                            let slice = &d.0[..8.min(d.0.len())];
                            d8.push(hex_encode(slice));
                        }
                    };
                });

                sqd_portal_client::svm::InstructionRequest {
                    program_id: inst
                        .program_id
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a0: inst
                        .a0
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a1: inst
                        .a1
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a2: inst
                        .a2
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a3: inst
                        .a3
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a4: inst
                        .a4
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a5: inst
                        .a5
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a6: inst
                        .a6
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a7: inst
                        .a7
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a8: inst
                        .a8
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    a9: inst
                        .a9
                        .iter()
                        .map(|v| base58_encode(v.0.as_slice()))
                        .collect(),
                    d1,
                    d2,
                    d3,
                    d4,
                    d8,
                    is_committed: inst.is_committed,
                    inner_instructions: inst.include_inner_instructions,
                    logs: inst.include_logs,
                    transaction: inst.include_transactions,
                    transaction_token_balances: inst.include_transaction_token_balances,
                }
            })
            .collect(),
        transactions: query
            .transactions
            .iter()
            .map(|tx| sqd_portal_client::svm::TransactionRequest {
                fee_payer: tx
                    .fee_payer
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                instructions: tx.include_instructions,
                logs: tx.include_logs,
            })
            .collect(),
        logs: query
            .logs
            .iter()
            .map(|lg| sqd_portal_client::svm::LogRequest {
                kind: lg.kind.iter().map(|v| v.as_str().to_owned()).collect(),
                program_id: lg
                    .program_id
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                transaction: lg.include_transactions,
                instruction: lg.include_instructions,
            })
            .collect(),
        balances: query
            .balances
            .iter()
            .map(|bl| sqd_portal_client::svm::BalanceRequest {
                account: bl
                    .account
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                transaction: bl.include_transactions,
                transaction_instructions: bl.include_transaction_instructions,
            })
            .collect(),
        token_balances: query
            .token_balances
            .iter()
            .map(|tb| sqd_portal_client::svm::TokenBalanceRequest {
                account: tb
                    .account
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                pre_program_id: tb
                    .pre_program_id
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                post_program_id: tb
                    .post_program_id
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                pre_mint: tb
                    .pre_mint
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                post_mint: tb
                    .post_mint
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                pre_owner: tb
                    .pre_owner
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                post_owner: tb
                    .post_owner
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
                transaction: tb.include_transactions,
                transaction_instructions: tb.include_transaction_instructions,
            })
            .collect(),
        rewards: query
            .rewards
            .iter()
            .map(|r| sqd_portal_client::svm::RewardRequest {
                pubkey: r
                    .pubkey
                    .iter()
                    .map(|v| base58_encode(v.0.as_slice()))
                    .collect(),
            })
            .collect(),
    })
}

fn evm_query_to_sqd(query: &evm::Query) -> Result<sqd_portal_client::evm::Query> {
    let hex_encode = |addr: &[u8]| format!("0x{}", faster_hex::hex_string(addr));

    let mut logs: Vec<_> = Vec::with_capacity(query.logs.len());

    for lg in query.logs.iter() {
        logs.push(sqd_portal_client::evm::LogRequest {
            address: lg
                .address
                .iter()
                .map(|x| hex_encode(x.0.as_slice()))
                .collect(),
            topic0: lg
                .topic0
                .iter()
                .map(|x| hex_encode(x.0.as_slice()))
                .collect(),
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
                number: query.fields.block.number
                    || query.fields.transaction.block_number
                    || query.fields.log.block_number
                    || query.fields.trace.block_number,
                hash: query.fields.block.hash
                    || query.fields.transaction.block_hash
                    || query.fields.log.block_hash
                    || query.fields.trace.block_hash,
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

pub fn start_stream(cfg: ProviderConfig, query: crate::Query) -> Result<DataStream> {
    let url = cfg
        .url
        .context("url is required when using sqd")?
        .parse()
        .context("parse url")?;

    let mut client_config = sqd_portal_client::ClientConfig::default();

    if let Some(v) = cfg.max_num_retries {
        client_config.max_num_retries = v;
    }
    if let Some(v) = cfg.retry_backoff_ms {
        client_config.retry_backoff_ms = v;
    }
    if let Some(v) = cfg.retry_base_ms {
        client_config.retry_base_ms = v;
    }
    if let Some(v) = cfg.retry_ceiling_ms {
        client_config.retry_ceiling_ms = v;
    }
    if let Some(v) = cfg.req_timeout_millis {
        client_config.http_req_timeout_millis = v;
    }

    let mut stream_config = sqd_portal_client::StreamConfig::default();
    stream_config.stop_on_head = cfg.stop_on_head;

    if let Some(head_poll_interval_millis) = cfg.head_poll_interval_millis {
        stream_config.head_poll_interval_millis = head_poll_interval_millis;
    }

    if let Some(buffer_size) = cfg.buffer_size {
        stream_config.buffer_size = buffer_size;
    }

    let client = sqd_portal_client::Client::new(url, client_config);
    let client = Arc::new(client);
    match query {
        Query::Svm(query) => {
            let sqd_query = svm_query_to_sqd(&query).context("convert to sqd query")?;

            let receiver = client.svm_arrow_finalized_stream(sqd_query, stream_config);

            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

            let stream = stream.map(move |v| {
                v.map(|v| {
                    let mut data = BTreeMap::new();

                    data.insert("blocks".to_owned(), v.blocks);
                    data.insert("rewards".to_owned(), v.rewards);
                    data.insert("token_balances".to_owned(), v.token_balances);
                    data.insert("balances".to_owned(), v.balances);
                    data.insert("logs".to_owned(), v.logs);
                    data.insert("transactions".to_owned(), v.transactions);
                    data.insert("instructions".to_owned(), v.instructions);

                    data
                })
            });

            Ok(Box::pin(stream))
        }
        Query::Evm(query) => {
            let sqd_query = evm_query_to_sqd(&query).context("convert to sqd query")?;

            let receiver = client.evm_arrow_finalized_stream(sqd_query, stream_config);

            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

            let stream = stream.map(move |v| {
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
