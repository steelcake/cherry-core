use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, BinaryArray, GenericByteArray, PrimitiveArray, UInt64Array, UInt8Array};
use arrow::datatypes::{GenericBinaryType, UInt8Type};
use arrow::{datatypes::UInt64Type, record_batch::RecordBatch};

use alloy_primitives::{Address, Bloom, Bytes, FixedBytes, Log, PrimitiveSignature, TxKind, Uint, B256, U256};
use alloy_consensus::{Eip658Value, Receipt, ReceiptEnvelope, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEnvelope, TxLegacy, SignableTransaction};
use alloy_consensus::proofs::{calculate_transaction_root, calculate_receipt_root};
use alloy_eips::eip2930::{AccessList, AccessListItem};
use serde::Deserialize;

struct LogArray<'a> {
    block_number: &'a PrimitiveArray<UInt64Type>,
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
    gas_limit: PrimitiveArray<UInt64Type>,
    gas_price: &'a GenericByteArray<GenericBinaryType<i32>>,
    hash: &'a GenericByteArray<GenericBinaryType<i32>>,
    input: &'a GenericByteArray<GenericBinaryType<i32>>,
    nonce: PrimitiveArray<UInt64Type>,
    to: &'a GenericByteArray<GenericBinaryType<i32>>,
    tx_index: &'a PrimitiveArray<UInt64Type>,
    value: &'a GenericByteArray<GenericBinaryType<i32>>,
    v: &'a GenericByteArray<GenericBinaryType<i32>>,
    r: &'a GenericByteArray<GenericBinaryType<i32>>,
    s: &'a GenericByteArray<GenericBinaryType<i32>>,
    max_priority_fee_per_gas: &'a GenericByteArray<GenericBinaryType<i32>>,
    max_fee_per_gas: &'a GenericByteArray<GenericBinaryType<i32>>,
    chain_id: PrimitiveArray<UInt64Type>,
    cumulative_gas_used: PrimitiveArray<UInt64Type>,
    contract_address: &'a GenericByteArray<GenericBinaryType<i32>>,
    logs_bloom: &'a GenericByteArray<GenericBinaryType<i32>>,
    tx_type: &'a PrimitiveArray<UInt8Type>,
    status: &'a PrimitiveArray<UInt8Type>,
    sighash: &'a GenericByteArray<GenericBinaryType<i32>>,
    access_list: &'a GenericByteArray<GenericBinaryType<i32>>,
    max_fee_per_blob_gas: &'a GenericByteArray<GenericBinaryType<i32>>,
    blob_versioned_hashes: &'a GenericByteArray<GenericBinaryType<i32>>,
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

// AccessListWrapper is a wrapper around the AccessList type that allows it to be deserialized from a byte array.
#[derive(Deserialize)]
pub struct AccessListWrapper(pub Vec<AccessListItemWrapper>);

impl Into<AccessList> for AccessListWrapper {
    fn into(self) -> AccessList {
        AccessList(self.0.into_iter().map(|item| item.into()).collect())
    }
}

// AccessListItemWrapper is a wrapper around the AccessListItem type that allows it to be deserialized from a byte array.
#[derive(Deserialize)]
pub struct AccessListItemWrapper {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

impl Into<AccessListItem> for AccessListItemWrapper {
    fn into(self) -> AccessListItem {
        AccessListItem {
            address: self.address,
            storage_keys: self.storage_keys,
        }
    }
}

pub fn validate_root_hashes(
    blocks: &RecordBatch,
    logs: &RecordBatch,
    transactions: &RecordBatch,
) -> Result<()> {

    // CREATE A LOG MAPPING

    let log_array = extract_log_cols_as_arrays(logs)?;

    // get first log block num and tx idx
    let mut current_block_num = log_array.block_number.value(0);
    let mut current_tx_idx = log_array.log_index.value(0);
    // initialize a vec to store all logs for a tx
    let mut tx_logs = Vec::<Log>::with_capacity(20);
    // initialize a map to store logs by block num and tx idx   
    let mut logs_by_block_num_and_tx_idx = BTreeMap::<(u64, u64), Vec<Log>>::new();

    let log_iterators = log_array.block_number.iter()
        .zip(log_array.log_index.iter())
        .zip(log_array.address.iter())
        .zip(log_array.topic0.iter())
        .zip(log_array.topic1.iter())
        .zip(log_array.topic2.iter())
        .zip(log_array.topic3.iter())
        .zip(log_array.data.iter());
    
    // iterate over logs rows
    for (((((((block_nums_opt, tx_idx_opt), address_opt), topic0_opt), topic1_opt), topic2_opt), topic3_opt), data_opt) in log_iterators {
        
        // cast values to expected types
        let block_num = block_nums_opt.unwrap();
        let tx_idx = tx_idx_opt.unwrap();
        let address = match address_opt.unwrap().try_into() {
            Ok(a) => a,
            Err(_) => return Err(anyhow!("address is invalid")),
        };
        // topics can be null
        let topic0: Option<FixedBytes<32>> = topic0_opt.map(|t| t.try_into().expect("topic0 is invalid"));
        let topic1: Option<FixedBytes<32>> = topic1_opt.map(|t| t.try_into().expect("topic1 is invalid"));
        let topic2: Option<FixedBytes<32>> = topic2_opt.map(|t| t.try_into().expect("topic2 is invalid"));
        let topic3: Option<FixedBytes<32>> = topic3_opt.map(|t| t.try_into().expect("topic3 is invalid"));
        // create a vec of topics with None values removed
        let topics: Vec<_> = [topic0, topic1, topic2, topic3]
            .into_iter()
            .flatten()
            .collect();

        let log_data = data_opt.unwrap_or_default();
        let log_data = Bytes::copy_from_slice(log_data);
        
        // if the block num or tx idx has changed, store the previous tx logs in the mapping, clear the logs vec and update the current block num and tx idx
        if block_num != current_block_num || tx_idx != current_tx_idx {
            if !tx_logs.is_empty() {
                logs_by_block_num_and_tx_idx.insert((current_block_num, current_tx_idx), tx_logs.clone());
                tx_logs.clear();
            }
            current_block_num = block_num;
            current_tx_idx = tx_idx;
        }
        
        // create a log object and add it to the tx logs vec
        let log = Log::new(address, topics, log_data).expect("log is invalid");
        tx_logs.push(log);
    };
    // store the last tx logs in the mapping
    logs_by_block_num_and_tx_idx.insert((current_block_num, current_tx_idx), tx_logs); 

    // CREATE A TRANSACTION MAPPING

    let tx_array = extract_transaction_cols_as_arrays(transactions)?;  
    // get first tx block num
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
        .block_number.iter()
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

        // cast values to expected types
        let block_num = tx_block_nums_opt.unwrap();
        let gas_limit = tx_gas_limit_opt.unwrap();
        let gas_price = U256::try_from_be_slice(tx_gas_price_opt.unwrap()).expect("invalid gas price");
        let gas_price = gas_price.try_into().unwrap();
        let expected_hash: FixedBytes<32> = tx_hash_opt.unwrap().try_into().unwrap();
        let input = tx_input_opt.unwrap();
        let input = Bytes::copy_from_slice(input);
        let nonce = tx_nonce_opt.unwrap();
        let to:Option<Address> = tx_to_opt.map(|a| a.try_into().unwrap());
        let tx_idx = tx_tx_idx_opt.unwrap();
        let value = U256::try_from_be_slice(tx_value_opt.unwrap()).expect("invalid value");
        let v = match tx_v_opt.unwrap(){
            [0] | [27] | [37] => false,
            [1] | [28] | [38] => true,
            _ => return Err(anyhow!("invalid v")),
        };
        let r: Uint<256, 4> = U256::try_from_be_slice(tx_r_opt.unwrap()).expect("invalid r");
        let s: Uint<256, 4> = U256::try_from_be_slice(tx_s_opt.unwrap()).expect("invalid s");
        let max_priority_fee_per_gas: Option<u128> = tx_max_priority_fee_per_gas_opt.map(|bytes| {
            let u256 = U256::try_from_be_slice(bytes).expect("invalid max priority fee per gas");
            u256.try_into().unwrap()
        });
        let max_fee_per_gas: Option<u128> = tx_max_fee_per_gas_opt.map(|bytes| {
            let u256 = U256::try_from_be_slice(bytes).expect("invalid max fee per gas");
            u256.try_into().unwrap()
        });
        let chain_id = tx_chain_id_opt; 
        let cumulative_gas_used = tx_cumulative_gas_used_opt.unwrap();
        let contract_address: Option<Address> = tx_contract_address_opt.map(|a| a.try_into().unwrap());
        let logs_bloom = tx_logs_bloom_opt.unwrap();
        let tx_type = tx_type_opt.unwrap();
        let status = tx_status_opt.unwrap();
        let expected_sighash = tx_sighash_opt;
        let access_list: Option<AccessListWrapper> = tx_access_list_opt.map(|bytes| bincode::deserialize(bytes).unwrap_or_else(|_|{
            println!("failed to deserialize access list, block {}, tx_hash {}", block_num, expected_hash);
            return AccessListWrapper(vec![]);
        }));
        let access_list: Option<AccessList> = access_list.map(|list| list.into());
        let max_fee_per_blob_gas: Option<u128> = tx_max_fee_per_blob_gas_opt.map(|bytes| {
            let u256 = U256::try_from_be_slice(bytes).expect("invalid max fee per blob gas");
            u256.try_into().unwrap()
        });
        let blob_versioned_hashes: Option<Vec<FixedBytes<32>>> = tx_blob_versioned_hashes_opt.map(|bytes| {
            bytes.chunks(32).map(|chunk| FixedBytes::from_slice(chunk)).collect()
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
                let sighash: [u8; 4] = input[..4].try_into().expect("input must be at least 4 bytes long for a tx with a sighash");
                if sighash != expected_sighash {
                    println!("sighash mismatch at block {}, tx_idx {}.\nExpected:\n{:?},\nFound:\n{:?}", block_num, tx_idx, expected_sighash, sighash);
                }
            },
            None => {
                if input.len() > 0 {
                    println!("sighash is None, How to know if this tx has UTF-8 input?");
                }
            }
        }

        // create tx objects
        let tx_kind = match contract_address {
            None => TxKind::Call(to.expect("to is None")),
            Some(_) => TxKind::Create
        };
        let primitive_sig = PrimitiveSignature::new(r, s, v);

        // create transaction as envelope (to accept all tx types)
        let tx_envelope = match tx_type {
            0 => {
                let tx= TxLegacy{
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
            },
            1 => {
                let tx= TxEip2930{
                    chain_id: chain_id.expect("chain_id is None, for a Eip2930 transaction"),
                    nonce,
                    gas_price,
                    gas_limit,
                    to: tx_kind,
                    value,
                    access_list: access_list.expect("access list is None, for a Eip2930 transaction"),
                    input,
                };
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Eip2930(signed_tx)
            },
            2 => {
                let tx= TxEip1559{
                    chain_id: chain_id.expect("chain_id is None, for a Eip1559 transaction"),
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas.expect("max fee per gas is None, for a Eip1559 transaction"),
                    max_priority_fee_per_gas: max_priority_fee_per_gas.expect("max priority fee per gas is None, for a Eip1559 transaction"),
                    to: tx_kind,
                    value,
                    access_list: access_list.expect("access list is None, for a Eip1559 transaction"),
                    input,
                };
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Eip1559(signed_tx)
            },
            3 => {
                let tx= TxEip4844Variant::TxEip4844(TxEip4844{
                    chain_id: chain_id.expect("chain_id is None, for a Eip4844 transaction"),
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas.expect("max fee per gas is None, for a Eip4844 transaction"),
                    max_priority_fee_per_gas: max_priority_fee_per_gas.expect("max priority fee per gas is None, for a Eip4844 transaction"),
                    to: to.expect("to is None, for a Eip4844 transaction"),
                    value,
                    access_list: access_list.expect("access list is None, for a Eip4844 transaction"),
                    blob_versioned_hashes: blob_versioned_hashes.expect("blob versioned hashes is None, for a Eip4844 transaction"),
                    max_fee_per_blob_gas: max_fee_per_blob_gas.expect("max fee per blob gas is None, for a Eip4844 transaction"),
                    input,
                });
                let signed_tx = tx.into_signed(primitive_sig);
                TxEnvelope::Eip4844(signed_tx)
            },
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
            println!("Tx hash mismatch at block {}, tx_idx {}.\nExpected:\n{:?},\nFound:\n{:?}", block_num, tx_idx, expected_hash, calculated_tx_hash);
            // return Err(anyhow!("Tx hash mismatch at block {}, tx_idx {}.\nExpected:\n{:?},\nFound:\n{:?}", block_num, tx_idx, expected_hash, calculated_tx_hash));
        }
        // add the tx envelope to the block tx envelopes vec
        block_tx_envelopes.push(tx_envelope);

        // get the logs for the tx, if the tx failed or doesn't have logs, use an empty vec
        let (eip658value, tx_logs) = match status {
            0 => (Eip658Value::Eip658(false), &Vec::<Log>::new()),
            1 => (Eip658Value::Eip658(true), logs_by_block_num_and_tx_idx.get(&(block_num, tx_idx)).unwrap_or(&empty_logs)),
            _ => return Err(anyhow!("Invalid tx status: {}", status)), // Other chains may have different status values
        };  

        // create a receipt object
        let receipt = Receipt {
            status: eip658value,
            cumulative_gas_used: cumulative_gas_used,
            logs: tx_logs.to_vec(),
        };

        // calculate the receipt bloom with the receipt object
        let receiptwithbloom = receipt.with_bloom();
        // create an expected bloom object from the logs_bloom column value
        let expected_bloom = Bloom::new(logs_bloom.try_into().expect("logs bloom must be 256 bytes"));
        // validate logs bloom
        if receiptwithbloom.logs_bloom != expected_bloom {
            println!("Logs bloom mismatch at block {}, tx_idx {}.\nExpected:\n{},\nFound:\n{:?}", block_num, tx_idx, expected_bloom, receiptwithbloom.logs_bloom);
            // return Err(anyhow!("Logs bloom mismatch at block {}, tx_idx {}.\nExpected:\n{},\nFound:\n{:?}", block_num, tx_idx, expected_bloom, receiptwithbloom.logs_bloom));
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
    };

    // calculate the transactions root for the last block
    let transactions_root = calculate_transaction_root(&block_tx_envelopes);
    // calculate the receipt root for the last block, and store it in the mapping
    let receipt_root = calculate_receipt_root(&block_tx_receipts);
    transactions_root_by_block_num_mapping.insert(current_block_num, transactions_root);
    receipts_root_by_block_num_mapping.insert(current_block_num, receipt_root);

    // COMPARE TRANSACTION AND RECEIPTS ROOT WITH EXPECTED VALUES

    // extract the block numbers, receipts roots and transactions roots from the blocks table
    let block_array = extract_block_cols_as_arrays(blocks)?;

    // create a map of block numbers to receipts roots
    let mut expected_transactions_and_receipts_root_by_block_num_mapping = BTreeMap::<u64, (FixedBytes<32>, FixedBytes<32>)>::new();

    // iterate over the block numbers and receipts roots
    for ((block_num_opt, block_receipts_root_opt), block_transactions_root_opt) in block_array.number.iter().zip(block_array.receipts_root.iter()).zip(block_array.transactions_root.iter()) {
        // cast the values to the expected types
        let block_num = block_num_opt.unwrap();
        let receipts_root = block_receipts_root_opt.unwrap().try_into().unwrap();
        let transactions_root = block_transactions_root_opt.unwrap().try_into().unwrap();
        // insert the values into the maps
        expected_transactions_and_receipts_root_by_block_num_mapping.insert(block_num, (receipts_root, transactions_root));
    }
    
    for (block_num, (expected_receipts_root, expected_transactions_root)) in expected_transactions_and_receipts_root_by_block_num_mapping.iter() {
        let calculated_receipts_root = receipts_root_by_block_num_mapping.get(block_num).unwrap();
        let calculated_transactions_root = transactions_root_by_block_num_mapping.get(block_num).unwrap();
        if expected_receipts_root != calculated_receipts_root {
            println!("Receipts root mismatch at block {}.\nExpected:\n{},\nFound:\n{:?}", block_num, expected_receipts_root, calculated_receipts_root);
            // return Err(anyhow!("Receipts root mismatch at block {}.\nExpected:\n{},\nFound:\n{:?}", block_num, expected, calculated));
        };
        if expected_transactions_root != calculated_transactions_root {
            println!("Transactions root mismatch at block {}.\nExpected:\n{},\nFound:\n{:?}", block_num, expected_transactions_root, calculated_transactions_root);
            // return Err(anyhow!("Transactions root mismatch at block {}.\nExpected:\n{},\nFound:\n{:?}", block_num, expected, calculated));
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
        log_index: log_tx_idx,
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
        .context("get tx gas column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx gas col as binary")?;

    // Convert BinaryArray to UInt64Array
    let tx_gas_limit = UInt64Array::from_iter(
        tx_gas_limit.iter().map(|opt_bytes| {
            opt_bytes.map(|bytes| {
                let u256 = alloy_primitives::U256::try_from_be_slice(bytes)
                        .expect("failed to parse cumulative_gas_used as u256");
                    u64::try_from(u256)
                        .expect("cumulative_gas_used too large for u64")
                })
            })
        );

    let tx_gas_price = transactions
        .column_by_name("gas_price")
        .context("get tx gas price column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx gas price col as binary")?;

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
        .downcast_ref::<BinaryArray>()
        .context("get tx nonce col as binary")?;

    // Convert BinaryArray to UInt64Array
    let tx_nonce = UInt64Array::from_iter(
        tx_nonce.iter().map(|opt_bytes| {
            opt_bytes.map(|bytes| {
                let u256 = alloy_primitives::U256::try_from_be_slice(bytes)
                    .expect("failed to parse nonce as u256");
                u64::try_from(u256)
                    .expect("nonce too large for u64")
            })
        })
    );
    

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
        .downcast_ref::<BinaryArray>()
        .context("get tx value col as binary")?;

    let tx_v = transactions
        .column_by_name("v")
        .context("get tx v column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx v col as binary")?;

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
        .downcast_ref::<BinaryArray>()
        .context("get tx max priority fee per gas col as binary")?;

    let tx_max_fee_per_gas = transactions
        .column_by_name("max_fee_per_gas")
        .context("get tx max fee per gas column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx max fee per gas col as binary")?;

    let tx_chain_id = transactions
        .column_by_name("chain_id")
        .context("get tx chain id column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx chain id col as binary")?;

    // Convert BinaryArray to UInt64Array
    let tx_chain_id = UInt64Array::from_iter(
        tx_chain_id.iter().map(|opt_bytes| {
            opt_bytes.map(|bytes| {
                let u256 = alloy_primitives::U256::try_from_be_slice(bytes)
                    .expect("failed to parse chain_id as u256");
                u64::try_from(u256)
                    .expect("chain_id too large for u64")
            })
        })
    );


    let tx_cumulative_gas_used = transactions
        .column_by_name("cumulative_gas_used")
        .context("get tx cumulative gas used column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx cumulative gas used col as binary")?;

    // Convert BinaryArray to UInt64Array
    let tx_cumulative_gas_used = UInt64Array::from_iter(
        tx_cumulative_gas_used.iter().map(|opt_bytes| {
            opt_bytes.map(|bytes| {
                let u256 = alloy_primitives::U256::try_from_be_slice(bytes)
                    .expect("failed to parse cumulative_gas_used as u256");
                u64::try_from(u256)
                    .expect("cumulative_gas_used too large for u64")
            })
        })
    );

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
        .downcast_ref::<BinaryArray>()
        .context("get tx access list col as binary")?;

    let tx_max_fee_per_blob_gas = transactions
        .column_by_name("max_fee_per_blob_gas")
        .context("get tx max fee per blob gas column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get tx max fee per blob gas col as binary")?;
    
    let tx_blob_versioned_hashes = transactions
        .column_by_name("blob_versioned_hashes")
        .context("get tx blob versioned hashes column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
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
        tx_type: tx_type,
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
