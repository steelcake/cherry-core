mod issues_collector;

pub use issues_collector::{DataContext, IssueCollector, IssueCollectorConfig, ReportFormat};

use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    Array, AsArray, BinaryArray, Decimal256Array, GenericByteArray, GenericListArray, ListArray,
    PrimitiveArray, StructArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{i256, Decimal256Type, GenericBinaryType, UInt8Type};
use arrow::{datatypes::UInt64Type, record_batch::RecordBatch};

use alloy_consensus::proofs::{calculate_receipt_root, calculate_transaction_root};
use alloy_consensus::{
    Eip658Value, Receipt, ReceiptEnvelope, SignableTransaction, TxEip1559, TxEip2930, TxEip4844,
    TxEip4844Variant, TxEnvelope, TxLegacy,
};
use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_primitives::{
    Address, Bloom, Bytes, FixedBytes, Log, PrimitiveSignature, TxKind, Uint, B256, U256,
};

struct LogArray<'a> {
    block_number: &'a PrimitiveArray<UInt64Type>,
    tx_index: &'a PrimitiveArray<UInt64Type>,
    log_index: &'a PrimitiveArray<UInt64Type>,
    address: &'a GenericByteArray<GenericBinaryType<i32>>,
    topic0: &'a GenericByteArray<GenericBinaryType<i32>>,
    topic1: &'a GenericByteArray<GenericBinaryType<i32>>,
    topic2: &'a GenericByteArray<GenericBinaryType<i32>>,
    topic3: &'a GenericByteArray<GenericBinaryType<i32>>,
    data: &'a GenericByteArray<GenericBinaryType<i32>>,
}

struct TransactionsArray<'a> {
    block_number: &'a PrimitiveArray<UInt64Type>,
    gas_limit: &'a PrimitiveArray<Decimal256Type>,
    gas_price: &'a PrimitiveArray<Decimal256Type>,
    hash: &'a GenericByteArray<GenericBinaryType<i32>>,
    input: &'a GenericByteArray<GenericBinaryType<i32>>,
    nonce: &'a PrimitiveArray<Decimal256Type>,
    to: &'a GenericByteArray<GenericBinaryType<i32>>,
    tx_index: &'a PrimitiveArray<UInt64Type>,
    value: &'a PrimitiveArray<Decimal256Type>,
    v: &'a PrimitiveArray<UInt8Type>,
    r: &'a GenericByteArray<GenericBinaryType<i32>>,
    s: &'a GenericByteArray<GenericBinaryType<i32>>,
    max_priority_fee_per_gas: &'a PrimitiveArray<Decimal256Type>,
    max_fee_per_gas: &'a PrimitiveArray<Decimal256Type>,
    chain_id: &'a PrimitiveArray<Decimal256Type>,
    cumulative_gas_used: &'a PrimitiveArray<Decimal256Type>,
    contract_address: &'a GenericByteArray<GenericBinaryType<i32>>,
    logs_bloom: &'a GenericByteArray<GenericBinaryType<i32>>,
    tx_type: &'a PrimitiveArray<UInt8Type>,
    status: &'a PrimitiveArray<UInt8Type>,
    sighash: &'a GenericByteArray<GenericBinaryType<i32>>,
    access_list: &'a GenericListArray<i32>,
    max_fee_per_blob_gas: &'a PrimitiveArray<Decimal256Type>,
    blob_versioned_hashes: &'a GenericListArray<i32>,
}

struct BlockArray<'a> {
    number: &'a PrimitiveArray<UInt64Type>,
    receipts_root: &'a GenericByteArray<GenericBinaryType<i32>>,
    transactions_root: &'a GenericByteArray<GenericBinaryType<i32>>,
}

/// Checks that:
///
/// - Everything is ordered by (block_number, tx_index/log_index)
///
/// - No gaps in (block_number, tx_index/log_index)
///
/// - block_hash/tx_hash matches with block_number/(block_number, tx_index)
///
/// - parent hash matches with previous block's hash
///
pub fn validate_block_data(
    blocks: &RecordBatch,
    transactions: &RecordBatch,
    logs: &RecordBatch,
    traces: &RecordBatch,
) -> Result<()> {
    let block_numbers = blocks
        .column_by_name("number")
        .context("get block number column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get block number column as u64")?;

    if block_numbers.null_count() > 0 {
        return Err(anyhow!("block.number column can't have nulls"));
    }

    let first_block_num = block_numbers
        .iter()
        .next()
        .map(Option::unwrap)
        .unwrap_or_default();
    let mut current_bn = first_block_num;
    for bn in block_numbers.iter().skip(1) {
        let bn = bn.unwrap();
        if current_bn + 1 != bn {
            return Err(anyhow!(
                "block.number column is not consistent. {} != {}",
                current_bn + 1,
                bn
            ));
        }
        current_bn = bn;
    }

    let block_hashes = blocks
        .column_by_name("hash")
        .context("get block hash column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get block hash as binary array")?;

    let block_parent_hashes = blocks
        .column_by_name("parent_hash")
        .context("get block parent_hash column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get block parent_hash as binary array")?;

    let get_block_hash = |block_num: u64| -> Option<&[u8]> {
        let pos = usize::try_from(block_num.checked_sub(first_block_num)?).unwrap();
        if pos < block_hashes.len() {
            Some(block_hashes.value(pos))
        } else {
            None
        }
    };

    if block_hashes.null_count() > 0 {
        return Err(anyhow!("block.hash column can't have nulls"));
    }

    if block_parent_hashes.null_count() > 0 {
        return Err(anyhow!("block.parent_has column can't have nulls"));
    }

    for (expected_parent_hash, parent_hash) in
        block_hashes.iter().zip(block_parent_hashes.iter().skip(1))
    {
        let expected_parent_hash = expected_parent_hash.unwrap();
        let parent_hash = parent_hash.unwrap();
        if expected_parent_hash != parent_hash {
            return Err(anyhow!(
                "bad parent hash found. expected {}, found {}",
                faster_hex::hex_string(expected_parent_hash),
                faster_hex::hex_string(parent_hash)
            ));
        }
    }

    validate_block_hashes(get_block_hash, transactions).context("validate tx block hashes")?;
    validate_block_hashes(get_block_hash, logs).context("validate log block hashes")?;
    validate_block_hashes(get_block_hash, traces).context("validate trace block hashes")?;

    // Validate tx ordering and check tx hashes of other tables

    let mut tx_hash_mapping = vec![Vec::<[u8; 32]>::with_capacity(200); block_numbers.len()];

    let tx_hashes = transactions
        .column_by_name("hash")
        .context("get tx hash col")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("tx hash col as binary")?;
    let tx_block_nums = transactions
        .column_by_name("block_number")
        .context("get tx block num col")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get tx block num col as u64")?;
    let tx_indices = transactions
        .column_by_name("transaction_index")
        .context("get tx index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get tx index col as u64")?;

    if tx_hashes.null_count() > 0 {
        return Err(anyhow!("tx hash column can't have nulls"));
    }
    if tx_block_nums.null_count() > 0 {
        return Err(anyhow!("tx block number column can't have nulls"));
    }
    if tx_indices.null_count() > 0 {
        return Err(anyhow!("tx index column can't have nulls"));
    }

    let mut expected_tx_index = 0;
    let mut current_block_num = first_block_num;

    for ((tx_hash, tx_bn), tx_idx) in tx_hashes
        .iter()
        .zip(tx_block_nums.iter())
        .zip(tx_indices.iter())
    {
        let tx_hash = tx_hash.unwrap();
        let tx_bn = tx_bn.unwrap();
        let tx_idx = tx_idx.unwrap();

        if tx_bn != current_block_num {
            if tx_bn < current_block_num {
                return Err(anyhow!(
                    "found wrong ordering in tx block numbers after block num {}",
                    current_block_num
                ));
            }

            current_block_num = tx_bn;
            expected_tx_index = 0;
        }

        if tx_idx != expected_tx_index {
            return Err(anyhow!(
                "found unexpected tx index at the start of block {}",
                current_block_num
            ));
        }
        expected_tx_index += 1;

        let block_pos = tx_bn
            .checked_sub(first_block_num)
            .with_context(|| format!("unexpected block num {} in transactions", tx_bn))?;
        let mappings = tx_hash_mapping
            .get_mut(usize::try_from(block_pos).unwrap())
            .unwrap();

        assert_eq!(mappings.len(), usize::try_from(tx_idx).unwrap());

        if tx_hash.len() != 32 {
            return Err(anyhow!("found bad tx hash at {},{}", tx_bn, tx_idx));
        }

        mappings.push(tx_hash.try_into().unwrap());
    }

    validate_transaction_hashes(first_block_num, &tx_hash_mapping, logs, "transaction_index")
        .context("check tx hashes in logs")?;
    validate_transaction_hashes(
        first_block_num,
        &tx_hash_mapping,
        traces,
        "transaction_position",
    )
    .context("check tx hashes in traces")?;

    // VALIDATE LOG ORDERING

    let log_block_nums = logs
        .column_by_name("block_number")
        .context("get log block num col")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get log block num col as u64")?;
    let log_indices = logs
        .column_by_name("log_index")
        .context("get log index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get log index col as u64")?;

    if log_block_nums.null_count() > 0 {
        return Err(anyhow!("log block number column can't have nulls"));
    }
    if log_indices.null_count() > 0 {
        return Err(anyhow!("log index column can't have nulls"));
    }

    let mut expected_log_index = 0;
    let mut current_block_num = first_block_num;

    for (log_idx, log_bn) in log_indices.iter().zip(log_block_nums.iter()) {
        let log_idx = log_idx.unwrap();
        let log_bn = log_bn.unwrap();

        if log_bn != current_block_num {
            if log_bn < current_block_num {
                return Err(anyhow!(
                    "found wrong ordering in log block numbers after block num {}",
                    current_block_num
                ));
            }

            expected_log_index = 0;
            current_block_num = log_bn;
        }

        if log_idx != expected_log_index {
            return Err(anyhow!(
                "found unexpected log index, expected {},{} but got {} for index",
                log_bn,
                expected_log_index,
                log_idx
            ));
        }
        expected_log_index += 1;
    }

    // VALIDATE TRACE ORDERING

    let trace_block_nums = traces
        .column_by_name("block_number")
        .context("get trace block num col")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get trace block num col as u64")?;
    let trace_tx_indices = traces
        .column_by_name("transaction_position")
        .context("get trace tx index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get trace tx index col as u64")?;

    if trace_block_nums.null_count() > 0 {
        return Err(anyhow!("log block number column can't have nulls"));
    }

    let mut current_tx_pos = 0;
    let mut current_block_num = first_block_num;

    for (trace_bn, trace_tx_pos) in trace_block_nums.iter().zip(trace_tx_indices.iter()) {
        let prev_bn = current_block_num;

        let trace_bn = trace_bn.unwrap();

        if trace_bn != current_block_num {
            if trace_bn < current_block_num {
                return Err(anyhow!(
                    "found wrong ordering in trace block numbers after block num {}",
                    current_block_num
                ));
            }

            current_tx_pos = 0;
            current_block_num = trace_bn;
        }

        let tx_pos = match trace_tx_pos {
            Some(x) => x,
            // This can be None for block reward traces and maybe for other traces that don't associate to blocks for some reason
            None => continue,
        };

        if tx_pos < current_tx_pos {
            return Err(anyhow!(
                "found bad tx position ordering after {},{}",
                prev_bn,
                current_tx_pos
            ));
        }
        current_tx_pos = tx_pos;
    }

    Ok(())
}

fn validate_block_hashes<'a, F: Fn(u64) -> Option<&'a [u8]>>(
    get_block_hash: F,
    data: &RecordBatch,
) -> Result<()> {
    let block_hashes = data
        .column_by_name("block_hash")
        .context("get block hash column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("block hash col as binary")?;
    let block_numbers = data
        .column_by_name("block_number")
        .context("get block number column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("block number as u64")?;

    if block_hashes.null_count() > 0 {
        return Err(anyhow!("block hash column can't have nulls"));
    }

    if block_numbers.null_count() > 0 {
        return Err(anyhow!("block number column can't have nulls"));
    }

    for (bn, hash) in block_numbers.iter().zip(block_hashes.iter()) {
        let bn = bn.unwrap();
        let hash = hash.unwrap();

        let expected = match get_block_hash(bn) {
            Some(h) => h,
            None => {
                return Err(anyhow!("couldn't find expected hash for block {}", bn));
            }
        };

        if expected != hash {
            return Err(anyhow!(
                "block hash mismatch at block {}. expected {} got {}",
                bn,
                faster_hex::hex_string(expected),
                faster_hex::hex_string(hash)
            ));
        }
    }

    Ok(())
}

fn validate_transaction_hashes(
    first_block_num: u64,
    expected_tx_hashes: &[Vec<[u8; 32]>],
    data: &RecordBatch,
    tx_index_col_name: &str,
) -> Result<()> {
    let tx_indices = data
        .column_by_name(tx_index_col_name)
        .context("get tx index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get tx index col as u64")?;
    let block_numbers = data
        .column_by_name("block_number")
        .context("get block number column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("block number as u64")?;
    let tx_hashes = data
        .column_by_name("transaction_hash")
        .context("get tx hash column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx hash col as binary")?;

    if block_numbers.null_count() > 0 {
        return Err(anyhow!("block number column can't have nulls"));
    }

    for ((tx_idx, tx_hash), bn) in tx_indices
        .iter()
        .zip(tx_hashes.iter())
        .zip(block_numbers.iter())
    {
        // Skip entries that don't associate to transactions, e.g. block reward traces.
        if let Some(tx_idx) = tx_idx {
            let bn = bn.unwrap();
            let tx_hash = match tx_hash {
                Some(h) => h,
                None => {
                    return Err(anyhow!("tx hash no found for tx {},{}", bn, tx_idx));
                }
            };

            let block_i = match bn.checked_sub(first_block_num) {
                Some(i) => usize::try_from(i).unwrap(),
                None => return Err(anyhow!("bad block num: {}", bn)),
            };

            let expected_tx_hash = expected_tx_hashes
                .get(block_i)
                .with_context(|| format!("block {} not found in given data", bn))?
                .get(usize::try_from(tx_idx).unwrap())
                .with_context(|| format!("tx hash data for tx {},{} not found", bn, tx_idx))?;

            if expected_tx_hash != tx_hash {
                return Err(anyhow!(
                    "tx hash mismatch for tx {},{}. Expected {}, Found {}",
                    bn,
                    tx_idx,
                    faster_hex::hex_string(expected_tx_hash),
                    faster_hex::hex_string(tx_hash)
                ));
            }
        }
    }

    Ok(())
}

#[rustfmt::skip]
pub fn validate_root_hashes(
    blocks: &RecordBatch,
    logs: &RecordBatch,
    transactions: &RecordBatch,
    issues_collector: &mut IssueCollector,
) -> Result<()> {
    let ic = issues_collector;

    // CREATE A LOG MAPPING

    let log_array = extract_log_cols_as_arrays(logs)?;

    // get first log block num and tx idx
    let mut current_block_num = log_array.block_number.value(0);
    let mut current_tx_idx = log_array.log_index.value(0);
    // initialize a vec to store all logs for a tx
    let mut tx_logs = Vec::<Log>::with_capacity(20);
    // initialize a map to store logs by block num and tx idx
    let mut logs_by_block_num_and_tx_idx = BTreeMap::<(u64, u64), Vec<Log>>::new();

    let log_iterators = log_array
        .block_number
        .iter()
        .zip(log_array.log_index.iter())
        .zip(log_array.tx_index.iter())
        .zip(log_array.address.iter())
        .zip(log_array.topic0.iter())
        .zip(log_array.topic1.iter())
        .zip(log_array.topic2.iter())
        .zip(log_array.topic3.iter())
        .zip(log_array.data.iter());

    // iterate over logs rows
    for (
        (
            (
                (
                    ((((block_nums_opt, log_idx_opt), tx_idx_opt), address_opt), topic0_opt),
                    topic1_opt,
                ),
                topic2_opt,
            ),
            topic3_opt,
        ),
        data_opt,
    ) in log_iterators
    {
        // cast values to expected types
        let block_num = block_nums_opt.unwrap(); // Block number can't be None because we are using it as key in the logs_by_block_num_and_tx_idx mapping
        let tx_idx = tx_idx_opt.unwrap(); // Tx index can't be None because we are using it as key in the logs_by_block_num_and_tx_idx mapping
        let log_idx = log_idx_opt.unwrap_or_else(|| {
            ic.report_with_context(
                "log_idx is None",
                DataContext::new(
                    "Logs".to_string(),
                    format!("Block_num {}, Tx_idx {}", block_num, tx_idx),
                ),
                99999,
            )
        });
        ic.set_context(DataContext::new(
            "Logs".to_string(),
            format!(
                "Block_num {}, Tx_idx {}, Log_idx {}",
                block_num, tx_idx, log_idx
            ),
        ));
        let address = address_opt
            .unwrap_or_else(|| ic.report("address is None", &[0; 20]))
            .try_into()
            .unwrap_or_else(|_| ic.report("address is invalid", Address::ZERO));
        let topic0: FixedBytes<32> = topic0_opt
            .unwrap_or_else(|| ic.report("topic0 is None", &[0; 32]))
            .try_into()
            .unwrap_or_else(|_| ic.report("topic0 is invalid", FixedBytes::<32>::new([0; 32])));
        let topic1: Option<FixedBytes<32>> = topic1_opt.and_then(|t| {
            t.try_into()
                .ok()
                .or_else(|| ic.report("topic1 is invalid", None))
        });
        let topic2: Option<FixedBytes<32>> = topic2_opt.and_then(|t| {
            t.try_into()
                .ok()
                .or_else(|| ic.report("topic2 is invalid", None))
        });
        let topic3: Option<FixedBytes<32>> = topic3_opt.and_then(|t| {
            t.try_into()
                .ok()
                .or_else(|| ic.report("topic3 is invalid", None))
        });
        let log_data = data_opt.unwrap_or_else(|| ic.report("log_data is None", &[0; 0]));
        let log_data = Bytes::copy_from_slice(log_data);

        // create a vec of topics with None values removed
        let topics: Vec<_> = [Some(topic0), topic1, topic2, topic3]
            .into_iter()
            .flatten()
            .collect();

        // if the block num or tx idx has changed, store the previous tx logs in the mapping, clear the logs vec and update the current block num and tx idx
        if block_num != current_block_num || tx_idx != current_tx_idx {
            if !tx_logs.is_empty() {
                logs_by_block_num_and_tx_idx
                    .insert((current_block_num, current_tx_idx), tx_logs.clone());
                tx_logs.clear();
            }
            current_block_num = block_num;
            current_tx_idx = tx_idx;
        }

        // create a log object and add it to the tx logs vec
        let log = Log::new(address, topics, log_data).expect("log is invalid");
        tx_logs.push(log);
    }
    // store the last tx logs in the mapping
    logs_by_block_num_and_tx_idx.insert((current_block_num, current_tx_idx), tx_logs);
    ic.set_context(DataContext::default());

    // CREATE A TRANSACTION MAPPING
    let tx_array = extract_transaction_cols_as_arrays(transactions)?;
    let mut current_block_num = tx_array.block_number.value(0);
    // initialize a map to store transaction root by block num
    let mut transactions_root_by_block_num_mapping = BTreeMap::<u64, FixedBytes<32>>::new();
    // initialize a map to store receipts by block num
    let mut receipts_root_by_block_num_mapping = BTreeMap::<u64, FixedBytes<32>>::new();
    // initialize a vec to store tx envelopes for a tx
    let mut block_tx_envelopes = Vec::<TxEnvelope>::with_capacity(200);
    // initialize a vec to store receipts for a tx
    let mut block_tx_receipts = Vec::<ReceiptEnvelope>::with_capacity(200);
    // initialize an empty vec of logs, used if the tx failed or doesn't have logs
    let empty_logs = Vec::<Log>::new();

    let tx_iterators = tx_array
        .block_number
        .iter()
        .zip(tx_array.gas_limit.iter())
        .zip(tx_array.gas_price.iter())
        .zip(tx_array.hash.iter())
        .zip(tx_array.input.iter())
        .zip(tx_array.nonce.iter())
        .zip(tx_array.to.iter())
        .zip(tx_array.tx_index.iter())
        .zip(tx_array.value.iter())
        .zip(tx_array.v.iter())
        .zip(tx_array.r.iter())
        .zip(tx_array.s.iter())
        .zip(tx_array.max_priority_fee_per_gas.iter())
        .zip(tx_array.max_fee_per_gas.iter())
        .zip(tx_array.chain_id.iter())
        .zip(tx_array.cumulative_gas_used.iter())
        .zip(tx_array.contract_address.iter())
        .zip(tx_array.logs_bloom.iter())
        .zip(tx_array.tx_type.iter())
        .zip(tx_array.status.iter())
        .zip(tx_array.sighash.iter())
        .zip(tx_array.access_list.iter())
        .zip(tx_array.max_fee_per_blob_gas.iter())
        .zip(tx_array.blob_versioned_hashes.iter());

    // iterate over transactions rows
    for (((((((((((((((((((((((
        tx_block_nums_opt
        , tx_gas_limit_opt)
        , tx_gas_price_opt)
        , tx_hash_opt)
        , tx_input_opt)
        , tx_nonce_opt)
        , tx_to_opt)
        , tx_tx_idx_opt)
        , tx_value_opt)
        , tx_v_opt)
        , tx_r_opt)
        , tx_s_opt)
        , tx_max_priority_fee_per_gas_opt)
        , tx_max_fee_per_gas_opt)
        , tx_chain_id_opt)
        , tx_cumulative_gas_used_opt)
        , tx_contract_address_opt)
        , tx_logs_bloom_opt)
        , tx_type_opt) 
        , tx_status_opt)
        , tx_sighash_opt)
        , tx_access_list_opt)
        , tx_max_fee_per_blob_gas_opt)
        , tx_blob_versioned_hashes_opt) in tx_iterators {
        // create contingent row context
        let cont_row_ctx = match (tx_block_nums_opt, tx_tx_idx_opt) {
            (Some(block_num), Some(tx_idx)) => {
                format!("Block_num {}, Tx_idx {}", block_num, tx_idx)
            }
            _ => "Undefined".to_string(),
        };

        // Try to unwrap and cast tx_hash to a FixedBytes<32>, report if issue and set context to the contingent row
        let expected_hash: FixedBytes<32> = match tx_hash_opt {
            //try to unwrap tx_hash
            None => {
                ic.set_context(DataContext::new("Transactions".to_string(), cont_row_ctx));
                ic.report("tx_hash is None", FixedBytes::<32>::new([0; 32]))
            }
            Some(hash) => match hash.try_into() {
                //try cast to FixedBytes<32>
                Ok(hash) => {
                    ic.set_context(DataContext::new(
                        "Transactions".to_string(),
                        format!("Tx_hash {}", hash),
                    ));
                    hash
                }
                Err(_) => {
                    ic.set_context(DataContext::new("Transactions".to_string(), cont_row_ctx));
                    ic.report("tx_hash is invalid", FixedBytes::<32>::new([0; 32]))
                }
            },
        };

        // cast values to expected types
        let block_num = tx_block_nums_opt.unwrap(); // Block number can't be None because we are using it as key in the root_by_block_num_mapping mapping
        let gas_limit = u64::try_from(
            tx_gas_limit_opt
                .unwrap_or_else(|| ic.report("gas_limit is None", i256::ZERO))
                .as_i128(),
        )
        .unwrap();
        let gas_price = u128::try_from(
            tx_gas_price_opt
                .unwrap_or_else(|| ic.report("gas_price is None", i256::ZERO))
                .as_i128(),
        )
        .unwrap();
        let input = tx_input_opt.unwrap_or_else(|| ic.report("input is None", &[0; 0]));
        let input = Bytes::copy_from_slice(input);
        let nonce = u64::try_from(
            tx_nonce_opt
                .unwrap_or_else(|| ic.report("nonce is None", i256::ZERO))
                .as_i128(),
        )
        .unwrap();
        let to: Option<Address> = tx_to_opt.and_then(|a| {
            a.try_into()
                .ok()
                .or_else(|| ic.report("to is invalid", None))
        });
        let tx_idx = tx_tx_idx_opt.unwrap_or_else(|| ic.report("tx_idx is None", 0));
        let value = U256::try_from(
            tx_value_opt
                .unwrap_or_else(|| ic.report("value is None", i256::ZERO))
                .as_i128(),
        )
        .unwrap();
        let chain_id = tx_chain_id_opt.and_then(|id| {
            u64::try_from(id.as_i128())
                .ok()
                .or_else(|| ic.report("chain_id is invalid", None))
        });
        // EIP-155: The recovery identifier boollean is v - 27 for legacy transactions and v = chainId * 2 + 35 for EIP-155 transactions.
        let r_id: u8 = (chain_id.unwrap_or(1) * 2 + 35)
            .try_into()
            .expect("invalid chain_id, produced signiture v is out of range");
        let v = tx_v_opt.unwrap_or_else(|| ic.report("v is None", 0));
        let v = if v == 0 || v == 27 || v == r_id {
            false
        } else if v == 1 || v == 28 || v == r_id + 1 {
            true
        } else {
            return Err(anyhow!("invalid v"));
        };
        let r: Uint<256, 4> =
            U256::try_from_be_slice(tx_r_opt.unwrap_or_else(|| ic.report("r is None", &[0; 32])))
                .expect("invalid r");
        let s: Uint<256, 4> =
            U256::try_from_be_slice(tx_s_opt.unwrap_or_else(|| ic.report("s is None", &[0; 32])))
                .expect("invalid s");
        let max_priority_fee_per_gas: Option<u128> =
            tx_max_priority_fee_per_gas_opt.and_then(|value| {
                value
                    .as_i128()
                    .try_into()
                    .ok()
                    .or_else(|| ic.report("max_priority_fee_per_gas is invalid", None))
            });
        let max_fee_per_gas: Option<u128> = tx_max_fee_per_gas_opt.and_then(|value| {
            value
                .as_i128()
                .try_into()
                .ok()
                .or_else(|| ic.report("max_fee_per_gas is invalid", None))
        });
        let cumulative_gas_used = u64::try_from(
            tx_cumulative_gas_used_opt
                .unwrap_or_else(|| ic.report("cumulative_gas_used is None", i256::ZERO))
                .as_i128(),
        )
        .unwrap();
        let contract_address: Option<Address> = tx_contract_address_opt.and_then(|a| {
            a.try_into()
                .ok()
                .or_else(|| ic.report("contract_address is invalid", None))
        });
        let logs_bloom =
            tx_logs_bloom_opt.unwrap_or_else(|| ic.report("logs_bloom is None", &[0; 256]));
        let status = tx_status_opt.unwrap_or_else(|| ic.report("status is None", 0));
        let expected_sighash = tx_sighash_opt;
        let access_list: Option<AccessList> = tx_access_list_opt.map(|array| {
            let access_list_items = array.as_struct_opt().expect("access list is not a struct");
            convert_arrow_array_into_access_list(access_list_items)
                .expect("access list is invalid")
        });
        let max_fee_per_blob_gas: Option<u128> = tx_max_fee_per_blob_gas_opt.and_then(|value| {
            value
                .as_i128()
                .try_into()
                .ok()
                .or_else(|| ic.report("max_fee_per_blob_gas is invalid", None))
        });
        let blob_versioned_hashes: Option<Vec<FixedBytes<32>>> =
            tx_blob_versioned_hashes_opt.map(|array| {
                let binary_array = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("blob_versioned_hashes must be a BinaryArray");
                convert_binary_array_32_to_fixed_hashes(binary_array)
            });
        let tx_type = tx_type_opt.unwrap_or(if access_list.is_some() {
            if max_priority_fee_per_gas.is_some() {
                if max_fee_per_blob_gas.is_some() {
                    3
                } else {
                    2
                }
            } else {
                1
            }
        } else {
            0
        });

        // if the block num has changed, store the previous tx receipts and tx envelopes in the mapping, clear the receipts vec and update the current block num
        if block_num != current_block_num {
            if !block_tx_receipts.is_empty() {
                let receipt_root = calculate_receipt_root(&block_tx_receipts);
                let transactions_root = calculate_transaction_root(&block_tx_envelopes);
                receipts_root_by_block_num_mapping.insert(current_block_num, receipt_root);
                transactions_root_by_block_num_mapping.insert(current_block_num, transactions_root);
                block_tx_receipts.clear();
                block_tx_envelopes.clear();
            }
            current_block_num = block_num;
        }

        // validate sighash
        match expected_sighash {
            Some(expected_sighash) => {
                let sighash: [u8; 4] = input[..4].try_into().unwrap_or_else(|_| {
                    ic.report(
                        "input must be at least 4 bytes long for a tx with a sighash",
                        [0; 4],
                    )
                });
                if sighash != expected_sighash {
                    ic.report(
                        format!(
                            "sighash mismatch. Expected:\n{:?},\nFound:\n{:?}",
                            expected_sighash, sighash
                        )
                        .as_str(),
                        (),
                    );
                }
            }
            None => {
                if input.len() > 4 {
                    ic.report("sighash is None, with a non-zero input", ());
                }
            }
        }

        // create alloy's tx_kind object
        let tx_kind = match contract_address {
            None => TxKind::Call(to.unwrap_or_else(|| {
                ic.report(
                    "to is None, while contract_address is also None",
                    Address::ZERO,
                )
            })),
            Some(_) => TxKind::Create,
        };
        let primitive_sig = PrimitiveSignature::new(r, s, v);

        // create alloy's tx_envelope object (to accept all tx types)
        let tx_envelope = match tx_type {
            0 => {
                let tx = TxLegacy {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    to: tx_kind,
                    value,
                    input,
                };
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Legacy(signed_tx)
            }
            1 => {
                let tx = TxEip2930 {
                    chain_id: chain_id.unwrap_or_else(|| {
                        ic.report("chain_id is None, for a Eip2930 transaction", 0)
                    }),
                    nonce,
                    gas_price,
                    gas_limit,
                    to: tx_kind,
                    value,
                    access_list: access_list.unwrap_or_else(|| {
                        ic.report(
                            "access list is None, for a Eip2930 transaction",
                            AccessList::default(),
                        )
                    }),
                    input,
                };
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Eip2930(signed_tx)
            }
            2 => {
                let tx = TxEip1559 {
                    chain_id: chain_id.unwrap_or_else(|| {
                        ic.report("chain_id is None, for a Eip1559 transaction", 0)
                    }),
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas.unwrap_or_else(|| {
                        ic.report("max fee per gas is None, for a Eip1559 transaction", 0)
                    }),
                    max_priority_fee_per_gas: max_priority_fee_per_gas.unwrap_or_else(|| {
                        ic.report(
                            "max priority fee per gas is None, for a Eip1559 transaction",
                            0,
                        )
                    }),
                    to: tx_kind,
                    value,
                    access_list: access_list.unwrap_or_else(|| {
                        ic.report(
                            "access list is None, for a Eip1559 transaction",
                            AccessList::default(),
                        )
                    }),
                    input,
                };
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Eip1559(signed_tx)
            }
            3 => {
                let tx = TxEip4844Variant::TxEip4844(TxEip4844 {
                    chain_id: chain_id.unwrap_or_else(|| {
                        ic.report("chain_id is None, for a Eip4844 transaction", 0)
                    }),
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas.unwrap_or_else(|| {
                        ic.report("max fee per gas is None, for a Eip4844 transaction", 0)
                    }),
                    max_priority_fee_per_gas: max_priority_fee_per_gas.unwrap_or_else(|| {
                        ic.report(
                            "max priority fee per gas is None, for a Eip4844 transaction",
                            0,
                        )
                    }),
                    to: to.unwrap_or_else(|| {
                        ic.report("to is None, for a Eip4844 transaction", Address::ZERO)
                    }),
                    value,
                    access_list: access_list.unwrap_or_else(|| {
                        ic.report(
                            "access list is None, for a Eip4844 transaction",
                            AccessList::default(),
                        )
                    }),
                    blob_versioned_hashes: blob_versioned_hashes.unwrap_or_else(|| {
                        ic.report(
                            "blob versioned hashes is None, for a Eip4844 transaction",
                            Vec::<FixedBytes<32>>::new(),
                        )
                    }),
                    max_fee_per_blob_gas: max_fee_per_blob_gas.unwrap_or_else(|| {
                        ic.report("max fee per blob gas is None, for a Eip4844 transaction", 0)
                    }),
                    input,
                });
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Eip4844(signed_tx)
            }
            // 4 => TypedTransaction::Eip7702(TxEip7702{
            //     chain_id,
            //     nonce,
            //     gas_limit,
            //     max_fee_per_gas,
            //     max_priority_fee_per_gas,
            //     to,
            //     value,
            //     access_list,
            //     authorization_list,
            //     input,
            // }),
            _ => return Err(anyhow!("Invalid tx type: {}", tx_type)),
        };

        //validate tx hash
        let calculated_tx_hash = tx_envelope.tx_hash();
        if calculated_tx_hash != &expected_hash {
            ic.report(
                format!(
                    "Calculated tx hash mismatch. Expected: {:?}, Found: {:?}",
                    expected_hash, calculated_tx_hash
                )
                .as_str(),
                (),
            );
        }
        block_tx_envelopes.push(tx_envelope);

        // get the logs for the tx, if the tx failed or doesn't have logs, use an empty vec
        let (eip658value, tx_logs) = match status {
            0 => (Eip658Value::Eip658(false), &Vec::<Log>::new()),
            1 => (
                Eip658Value::Eip658(true),
                logs_by_block_num_and_tx_idx
                    .get(&(block_num, tx_idx))
                    .unwrap_or(&empty_logs),
            ),
            _ => return Err(anyhow!("Invalid tx status: {}", status)), // Other chains may have different status values
        };

        // create a receipt object
        let receipt = Receipt {
            status: eip658value,
            cumulative_gas_used,
            logs: tx_logs.to_vec(),
        };

        // calculate the receipt bloom with the receipt object
        let receiptwithbloom = receipt.with_bloom();
        // create an expected bloom object from the logs_bloom column value
        let expected_bloom = Bloom::new(
            logs_bloom
                .try_into()
                .unwrap_or_else(|_| ic.report("logs bloom must be 256 bytes", [0; 256])),
        );
        // validate logs bloom
        if receiptwithbloom.logs_bloom != expected_bloom {
            ic.report(
                format!(
                    "Calculated logs bloom mismatch.\nExpected {:?},\nFound: {:?}",
                    expected_bloom, receiptwithbloom.logs_bloom
                )
                .as_str(),
                (),
            );
        }
        // create a receipt envelope object from the receipt_with_bloom object
        let receipt_envelope = match tx_type {
            0 => ReceiptEnvelope::Legacy(receiptwithbloom),
            1 => ReceiptEnvelope::Eip2930(receiptwithbloom),
            2 => ReceiptEnvelope::Eip1559(receiptwithbloom),
            3 => ReceiptEnvelope::Eip4844(receiptwithbloom),
            4 => ReceiptEnvelope::Eip7702(receiptwithbloom),
            _ => return Err(anyhow!("Invalid tx type: {}", tx_type)),
        };
        // add the receipt envelope to the block tx receipts vec
        block_tx_receipts.push(receipt_envelope);
    }

    // calculate the transactions root for the last block
    let transactions_root = calculate_transaction_root(&block_tx_envelopes);
    // calculate the receipt root for the last block, and store it in the mapping
    let receipt_root = calculate_receipt_root(&block_tx_receipts);
    transactions_root_by_block_num_mapping.insert(current_block_num, transactions_root);
    receipts_root_by_block_num_mapping.insert(current_block_num, receipt_root);
    ic.set_context(DataContext::default());

    // COMPARE TRANSACTION AND RECEIPTS ROOT WITH EXPECTED VALUES

    // extract the block numbers, receipts roots and transactions roots from the blocks table
    let block_array = extract_block_cols_as_arrays(blocks)?;

    // create a map of block numbers to receipts roots
    let mut expected_transactions_and_receipts_root_by_block_num_mapping =
        BTreeMap::<u64, (FixedBytes<32>, FixedBytes<32>)>::new();

    // iterate over the block numbers and receipts roots
    for ((block_num_opt, block_receipts_root_opt), block_transactions_root_opt) in block_array
        .number
        .iter()
        .zip(block_array.receipts_root.iter())
        .zip(block_array.transactions_root.iter())
    {
        // cast the values to the expected types
        let block_num = block_num_opt.unwrap();
        ic.set_context(DataContext::new(
            "Blocks".to_string(),
            format!("Block_num {}", block_num),
        ));
        let receipts_root = block_receipts_root_opt
            .unwrap_or_else(|| ic.report("receipts root is None", &[0; 32]))
            .try_into()
            .unwrap_or_else(|_| ic.report("receipts root is invalid", FixedBytes::ZERO));
        let transactions_root = block_transactions_root_opt
            .unwrap_or_else(|| ic.report("transactions root is None", &[0; 32]))
            .try_into()
            .unwrap_or_else(|_| ic.report("transactions root is invalid", FixedBytes::ZERO));
        // insert the values into the maps
        expected_transactions_and_receipts_root_by_block_num_mapping
            .insert(block_num, (receipts_root, transactions_root));
    }

    for (block_num, (expected_receipts_root, expected_transactions_root)) in
        expected_transactions_and_receipts_root_by_block_num_mapping.iter()
    {
        // null root is the root of an empty block
        let null_root = <FixedBytes<32> as alloy_primitives::hex::FromHex>::from_hex(
            "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        )
        .unwrap();
        if expected_receipts_root == expected_transactions_root
            && expected_receipts_root == &null_root
        {
            continue;
        }
        let calculated_receipts_root = receipts_root_by_block_num_mapping
            .get(block_num)
            .unwrap_or_else(|| {
                ic.report(
                    "There is no calculated receipts root for this block",
                    &FixedBytes::ZERO,
                )
            });
        let calculated_transactions_root = transactions_root_by_block_num_mapping
            .get(block_num)
            .unwrap_or_else(|| {
                ic.report(
                    "There is no calculated transactions root for this block",
                    &FixedBytes::ZERO,
                )
            });
        if expected_receipts_root != calculated_receipts_root {
            ic.report(
                format!(
                    "Receipts root mismatch. Expected: {:?}, Found: {:?}",
                    expected_receipts_root, calculated_receipts_root
                )
                .as_str(),
                (),
            );
        };
        if expected_transactions_root != calculated_transactions_root {
            ic.report(
                format!(
                    "Transactions root mismatch. Expected: {:?}, Found: {:?}",
                    expected_transactions_root, calculated_transactions_root
                )
                .as_str(),
                (),
            );
        }
    }

    Ok(())
}

fn extract_log_cols_as_arrays(logs: &RecordBatch) -> Result<LogArray> {
    let log_block_nums = logs
        .column_by_name("block_number")
        .context("get log block num col")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get log block num col as u64")?;

    let log_log_idx = logs
        .column_by_name("log_index")
        .context("get log log_index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get log log_index col as u64")?;

    let log_tx_idx = logs
        .column_by_name("transaction_index")
        .context("get tx index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get tx index col as u64")?;

    let log_address = logs
        .column_by_name("address")
        .context("get address column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get address as binary")?;

    let log_topic0 = logs
        .column_by_name("topic0")
        .context("get topic0 column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get topic0 as binary")?;

    let log_topic1 = logs
        .column_by_name("topic1")
        .context("get topic1 column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get topic1 as binary")?;

    let log_topic2 = logs
        .column_by_name("topic2")
        .context("get topic2 column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get topic2 as binary")?;

    let log_topic3 = logs
        .column_by_name("topic3")
        .context("get topic3 column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get topic3 as binary")?;

    let log_data = logs
        .column_by_name("data")
        .context("get data column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get data as binary")?;

    let log_array = LogArray {
        block_number: log_block_nums,
        tx_index: log_tx_idx,
        log_index: log_log_idx,
        address: log_address,
        topic0: log_topic0,
        topic1: log_topic1,
        topic2: log_topic2,
        topic3: log_topic3,
        data: log_data,
    };

    // Return the extracted data
    Ok(log_array)
}

fn extract_transaction_cols_as_arrays(transactions: &RecordBatch) -> Result<TransactionsArray> {
    let tx_block_nums = transactions
        .column_by_name("block_number")
        .context("get tx block num col")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get tx block num col as u64")?;

    let tx_gas_limit = transactions
        .column_by_name("gas")
        .context("get tx gas limit column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx gas limit col as decimal256")?;

    let tx_gas_price = transactions
        .column_by_name("gas_price")
        .context("get tx gas price column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx gas price col as decimal256")?;

    let tx_hash = transactions
        .column_by_name("hash")
        .context("get tx hash column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx hash col as binary")?;

    let tx_input = transactions
        .column_by_name("input")
        .context("get tx input column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx input col as binary")?;

    let tx_nonce = transactions
        .column_by_name("nonce")
        .context("get tx nonce column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx nonce col as binary")?;

    let tx_to = transactions
        .column_by_name("to")
        .context("get tx to column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx to col as binary")?;

    let tx_tx_idx = transactions
        .column_by_name("transaction_index")
        .context("get tx index column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get tx index col as u64")?;

    let tx_value = transactions
        .column_by_name("value")
        .context("get tx value column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx value col as decimal256")?;

    let tx_v = transactions
        .column_by_name("v")
        .context("get tx v column")?
        .as_any()
        .downcast_ref::<UInt8Array>()
        .context("get tx v col as u8")?;

    let tx_r = transactions
        .column_by_name("r")
        .context("get tx r column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx r col as binary")?;

    let tx_s = transactions
        .column_by_name("s")
        .context("get tx s column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx s col as binary")?;

    let tx_max_priority_fee_per_gas = transactions
        .column_by_name("max_priority_fee_per_gas")
        .context("get tx max priority fee per gas column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx max priority fee per gas col as decimal256")?;

    let tx_max_fee_per_gas = transactions
        .column_by_name("max_fee_per_gas")
        .context("get tx max fee per gas column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx max fee per gas col as decimal256")?;

    let tx_chain_id = transactions
        .column_by_name("chain_id")
        .context("get tx chain id column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx chain id col as decimal256")?;

    let tx_cumulative_gas_used = transactions
        .column_by_name("cumulative_gas_used")
        .context("get tx cumulative gas used column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx cumulative gas used col as decimal256")?;

    let tx_contract_address = transactions
        .column_by_name("contract_address")
        .context("get tx contract address column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx contract address col as binary")?;

    let tx_logs_bloom: &GenericByteArray<GenericBinaryType<i32>> = transactions
        .column_by_name("logs_bloom")
        .context("get tx logs bloom column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx logs bloom col as binary")?;

    let tx_type = transactions
        .column_by_name("type")
        .context("get tx type column")?
        .as_any()
        .downcast_ref::<UInt8Array>()
        .context("get tx type col as u8")?;

    let tx_status = transactions
        .column_by_name("status")
        .context("get tx status column")?
        .as_any()
        .downcast_ref::<UInt8Array>()
        .context("get tx status col as u8")?;

    let tx_sighash = transactions
        .column_by_name("sighash")
        .context("get tx sig hash column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx sig hash col as binary")?;

    let tx_access_list = transactions
        .column_by_name("access_list")
        .context("get tx access list column")?
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()
        .context("get tx access list col as binary")?;

    let tx_max_fee_per_blob_gas = transactions
        .column_by_name("max_fee_per_blob_gas")
        .context("get tx max fee per blob gas column")?
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .context("get tx max fee per blob gas col as decimal256")?;

    let tx_blob_versioned_hashes = transactions
        .column_by_name("blob_versioned_hashes")
        .context("get tx blob versioned hashes column")?
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()
        .context("get tx blob versioned hashes col as binary")?;

    let tx_array = TransactionsArray {
        block_number: tx_block_nums,
        gas_limit: tx_gas_limit,
        gas_price: tx_gas_price,
        hash: tx_hash,
        input: tx_input,
        nonce: tx_nonce,
        to: tx_to,
        tx_index: tx_tx_idx,
        value: tx_value,
        v: tx_v,
        r: tx_r,
        s: tx_s,
        max_priority_fee_per_gas: tx_max_priority_fee_per_gas,
        max_fee_per_gas: tx_max_fee_per_gas,
        chain_id: tx_chain_id,
        cumulative_gas_used: tx_cumulative_gas_used,
        contract_address: tx_contract_address,
        logs_bloom: tx_logs_bloom,
        tx_type,
        status: tx_status,
        sighash: tx_sighash,
        access_list: tx_access_list,
        max_fee_per_blob_gas: tx_max_fee_per_blob_gas,
        blob_versioned_hashes: tx_blob_versioned_hashes,
    };

    Ok(tx_array)
}

fn extract_block_cols_as_arrays(blocks: &RecordBatch) -> Result<BlockArray> {
    let block_numbers = blocks
        .column_by_name("number")
        .context("get block number column")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("get block number column as u64")?;

    let block_receipts_root = blocks
        .column_by_name("receipts_root")
        .context("get block receipts_root column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get block receipts_root as binary")?;

    let block_transactions_root = blocks
        .column_by_name("transactions_root")
        .context("get block transactions_root column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get block transactions_root as binary")?;

    let block_array = BlockArray {
        number: block_numbers,
        receipts_root: block_receipts_root,
        transactions_root: block_transactions_root,
    };

    Ok(block_array)
}

fn convert_arrow_array_into_access_list(array: &StructArray) -> Result<AccessList> {
    let mut items = Vec::with_capacity(array.len());

    // Extract the child arrays
    let address_array = array
        .column_by_name("address")
        .context("Missing 'address' field")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("'address' field is not a BinaryArray")?;

    let storage_keys_array = array
        .column_by_name("storage_keys")
        .context("Missing 'storage_keys' field")?
        .as_any()
        .downcast_ref::<ListArray>()
        .context("'storage_keys' field is not a ListArray")?;

    let storage_keys_values = storage_keys_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("Storage keys values are not a BinaryArray")?;

    let storage_keys_offsets = storage_keys_array.offsets();

    // Convert each row to an AccessListItem
    for i in 0..array.len() {
        if array.is_null(i) {
            continue;
        }

        // Skip if either is null - they must be both valid or both null
        if address_array.is_null(i) || storage_keys_array.is_null(i) {
            continue;
        }

        // Get address - convert binary to Address (20 bytes)
        let bytes = address_array.value(i);
        if bytes.len() != 20 {
            return Err(anyhow::anyhow!("Invalid address length: {}", bytes.len()));
        }
        let mut addr = [0u8; 20];
        addr.copy_from_slice(bytes);
        let address = Address::new(addr);

        // Get storage keys - convert each binary to B256 (32 bytes)
        let mut storage_keys = Vec::new();
        let start_offset = *storage_keys_offsets.get(i).expect("start offset is null") as usize;
        let end_offset = *storage_keys_offsets.get(i + 1).expect("end offset is null") as usize;

        for j in start_offset..end_offset {
            let bytes = storage_keys_values.value(j);

            // Make sure we have the correct length for B256
            if bytes.len() != 32 {
                return Err(anyhow::anyhow!("Invalid B256 length: {}", bytes.len()));
            }

            let mut b256 = [0u8; 32];
            b256.copy_from_slice(bytes);
            storage_keys.push(B256::new(b256));
        }

        items.push(AccessListItem {
            address,
            storage_keys,
        });
    }

    Ok(AccessList(items))
}

fn convert_binary_array_32_to_fixed_hashes(binary_array: &BinaryArray) -> Vec<FixedBytes<32>> {
    binary_array
        .iter()
        .map(|bytes| {
            let bytes = bytes.expect("blob versioned hash cannot be null");
            let mut hash = [0u8; 32];
            hash.copy_from_slice(bytes);
            FixedBytes::<32>::new(hash)
        })
        .collect()
}
