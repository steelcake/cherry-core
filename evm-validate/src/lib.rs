use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{Array, BinaryArray, UInt64Array},
    record_batch::RecordBatch,
};

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
