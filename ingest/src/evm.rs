use std::collections::BTreeSet;

use anyhow::{anyhow, Context, Result};
use hypersync_client::net_types as hypersync_nt;
use serde::Serialize;

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    pub transactions: Vec<TransactionRequest>,
    pub logs: Vec<LogRequest>,
    pub traces: Vec<TraceRequest>,
    pub fields: Fields,
}

impl Query {
    pub fn to_sqd(&self) -> Result<sqd_portal_client::evm::Query> {
        let hex_encode = |addr: &[u8]| format!("0x{}", faster_hex::hex_string(addr));

        let mut logs: Vec<_> = Vec::with_capacity(self.logs.len());

        for lg in self.logs.iter() {
            let mut topic0 = Vec::with_capacity(lg.topic0.len() + lg.event_signatures.len());

            topic0.extend_from_slice(lg.topic0.as_slice());

            for sig in lg.event_signatures.iter() {
                let t0 = cherry_evm_decode::signature_to_topic0(sig)
                    .context("convert event signature to topic0")?;
                topic0.push(Topic(t0));
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
            from_block: self.from_block,
            to_block: self.to_block.map(|x| x + 1),
            include_all_blocks: self.include_all_blocks,
            transactions: self
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
            traces: self
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
                    number: self.fields.block.number,
                    hash: self.fields.block.hash,
                    parent_hash: self.fields.block.parent_hash,
                    timestamp: self.fields.block.timestamp,
                    transactions_root: self.fields.block.transactions_root,
                    receipts_root: self.fields.block.receipts_root,
                    state_root: self.fields.block.state_root,
                    logs_bloom: self.fields.block.logs_bloom,
                    sha3_uncles: self.fields.block.sha3_uncles,
                    extra_data: self.fields.block.extra_data,
                    miner: self.fields.block.miner,
                    nonce: self.fields.block.nonce,
                    mix_hash: self.fields.block.mix_hash,
                    size: self.fields.block.size,
                    gas_limit: self.fields.block.gas_limit,
                    gas_used: self.fields.block.gas_used,
                    difficulty: self.fields.block.difficulty,
                    total_difficulty: self.fields.block.total_difficulty,
                    base_fee_per_gas: self.fields.block.base_fee_per_gas,
                    blob_gas_used: self.fields.block.blob_gas_used,
                    excess_blob_gas: self.fields.block.excess_blob_gas,
                    l1_block_number: self.fields.block.l1_block_number,
                },
                transaction: sqd_portal_client::evm::TransactionFields {
                    transaction_index: self.fields.transaction.transaction_index,
                    hash: self.fields.transaction.hash,
                    nonce: self.fields.transaction.nonce,
                    from: self.fields.transaction.from_,
                    to: self.fields.transaction.to,
                    input: self.fields.transaction.input,
                    value: self.fields.transaction.value,
                    gas: self.fields.transaction.gas,
                    gas_price: self.fields.transaction.gas_price,
                    max_fee_per_gas: self.fields.transaction.max_fee_per_gas,
                    max_priority_fee_per_gas: self.fields.transaction.max_priority_fee_per_gas,
                    v: self.fields.transaction.v,
                    r: self.fields.transaction.r,
                    s: self.fields.transaction.s,
                    y_parity: self.fields.transaction.y_parity,
                    chain_id: self.fields.transaction.chain_id,
                    sighash: self.fields.transaction.sighash,
                    contract_address: self.fields.transaction.contract_address,
                    gas_used: self.fields.transaction.gas_used,
                    cumulative_gas_used: self.fields.transaction.cumulative_gas_used,
                    effective_gas_price: self.fields.transaction.effective_gas_price,
                    type_: self.fields.transaction.type_,
                    status: self.fields.transaction.status,
                    max_fee_per_blob_gas: self.fields.transaction.max_fee_per_blob_gas,
                    blob_versioned_hashes: self.fields.transaction.blob_versioned_hashes,
                    l1_fee: self.fields.transaction.l1_fee,
                    l1_fee_scalar: self.fields.transaction.l1_fee_scalar,
                    l1_gas_price: self.fields.transaction.l1_gas_price,
                    l1_gas_used: false,
                    l1_blob_base_fee: self.fields.transaction.l1_blob_base_fee,
                    l1_blob_base_fee_scalar: self.fields.transaction.l1_blob_base_fee_scalar,
                    l1_base_fee_scalar: self.fields.transaction.l1_base_fee_scalar,
                },
                log: sqd_portal_client::evm::LogFields {
                    log_index: self.fields.log.log_index,
                    transaction_index: self.fields.log.transaction_index,
                    transaction_hash: self.fields.log.transaction_hash,
                    address: self.fields.log.address,
                    data: self.fields.log.data,
                    topics: self.fields.log.topic0
                        || self.fields.log.topic1
                        || self.fields.log.topic2
                        || self.fields.log.topic3,
                },
                trace: sqd_portal_client::evm::TraceFields {
                    transaction_index: self.fields.trace.transaction_position,
                    trace_address: self.fields.trace.trace_address,
                    subtraces: self.fields.trace.subtraces,
                    type_: self.fields.trace.type_,
                    error: self.fields.trace.error,
                    revert_reason: self.fields.trace.error,
                    create_from: self.fields.trace.from_,
                    create_value: self.fields.trace.value,
                    create_gas: self.fields.trace.gas,
                    create_init: self.fields.trace.init,
                    create_result_gas_used: self.fields.trace.gas_used,
                    create_result_code: self.fields.trace.code,
                    create_result_address: self.fields.trace.address,
                    call_from: self.fields.trace.from_,
                    call_to: self.fields.trace.to,
                    call_value: self.fields.trace.value,
                    call_gas: self.fields.trace.gas,
                    call_input: self.fields.trace.input,
                    call_sighash: self.fields.trace.sighash,
                    call_type: self.fields.trace.type_,
                    call_call_type: self.fields.trace.call_type,
                    call_result_gas_used: self.fields.trace.gas_used,
                    call_result_output: self.fields.trace.output,
                    suicide_address: self.fields.trace.address,
                    suicide_refund_address: self.fields.trace.refund_address,
                    suicide_balance: self.fields.trace.balance,
                    reward_author: self.fields.trace.author,
                    reward_value: self.fields.trace.value,
                    reward_type: self.fields.trace.author,
                },
            },
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Hash(pub [u8; 32]);

#[derive(Debug, Clone, Copy)]
pub struct Address(pub [u8; 20]);

#[derive(Debug, Clone, Copy)]
pub struct Sighash(pub [u8; 4]);

#[derive(Debug, Clone, Copy)]
pub struct Topic(pub [u8; 32]);

#[cfg(feature = "pyo3")]
fn extract_hex<const N: usize>(ob: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<[u8; N]> {
    use pyo3::types::PyAnyMethods;

    let s: &str = ob.extract()?;
    let s = s.strip_prefix("0x").context("strip 0x prefix")?;
    let mut out = [0; N];
    faster_hex::hex_decode(s.as_bytes(), &mut out).context("decode hex")?;

    Ok(out)
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for Hash {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_hex(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for Address {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_hex(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for Sighash {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_hex(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for Topic {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_hex(ob)?;
        Ok(Self(out))
    }
}

// #[derive(Default, Debug, Clone)]
// pub struct BlockRequest {
//     pub hash: Vec<Hash>,
//     pub miner: Vec<Address>,
// }

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TransactionRequest {
    pub from_: Vec<Address>,
    pub to: Vec<Address>,
    pub sighash: Vec<Sighash>,
    pub status: Vec<u8>,
    pub type_: Vec<u8>,
    pub contract_deployment_address: Vec<Address>,
    pub hash: Vec<Hash>,
    pub include_logs: bool,
    pub include_traces: bool,
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct LogRequest {
    pub address: Vec<Address>,
    pub event_signatures: Vec<String>,
    pub topic0: Vec<Topic>,
    pub topic1: Vec<Topic>,
    pub topic2: Vec<Topic>,
    pub topic3: Vec<Topic>,
    pub include_transactions: bool,
    pub include_transaction_logs: bool,
    pub include_transaction_traces: bool,
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TraceRequest {
    pub from_: Vec<Address>,
    pub to: Vec<Address>,
    pub address: Vec<Address>,
    pub call_type: Vec<String>,
    pub reward_type: Vec<String>,
    pub type_: Vec<String>,
    pub sighash: Vec<Sighash>,
    pub author: Vec<Address>,
    pub include_transactions: bool,
    pub include_transaction_logs: bool,
    pub include_transaction_traces: bool,
}

#[derive(Serialize, Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct Fields {
    pub block: BlockFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub trace: TraceFields,
}

impl Fields {
    pub fn all() -> Self {
        Self {
            block: BlockFields::all(),
            transaction: TransactionFields::all(),
            log: LogFields::all(),
            trace: TraceFields::all(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct BlockFields {
    pub number: bool,
    pub hash: bool,
    pub parent_hash: bool,
    pub nonce: bool,
    pub sha3_uncles: bool,
    pub logs_bloom: bool,
    pub transactions_root: bool,
    pub state_root: bool,
    pub receipts_root: bool,
    pub miner: bool,
    pub difficulty: bool,
    pub total_difficulty: bool,
    pub extra_data: bool,
    pub size: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub timestamp: bool,
    pub uncles: bool,
    pub base_fee_per_gas: bool,
    pub blob_gas_used: bool,
    pub excess_blob_gas: bool,
    pub parent_beacon_block_root: bool,
    pub withdrawals_root: bool,
    pub withdrawals: bool,
    pub l1_block_number: bool,
    pub send_count: bool,
    pub send_root: bool,
    pub mix_hash: bool,
}

impl BlockFields {
    pub fn all() -> Self {
        BlockFields {
            number: true,
            hash: true,
            parent_hash: true,
            nonce: true,
            sha3_uncles: true,
            logs_bloom: true,
            transactions_root: true,
            state_root: true,
            receipts_root: true,
            miner: true,
            difficulty: true,
            total_difficulty: true,
            extra_data: true,
            size: true,
            gas_limit: true,
            gas_used: true,
            timestamp: true,
            uncles: true,
            base_fee_per_gas: true,
            blob_gas_used: true,
            excess_blob_gas: true,
            parent_beacon_block_root: true,
            withdrawals_root: true,
            withdrawals: true,
            l1_block_number: true,
            send_count: true,
            send_root: true,
            mix_hash: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TransactionFields {
    pub block_hash: bool,
    pub block_number: bool,
    pub from_: bool,
    pub gas: bool,
    pub gas_price: bool,
    pub hash: bool,
    pub input: bool,
    pub nonce: bool,
    pub to: bool,
    pub transaction_index: bool,
    pub value: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    pub max_priority_fee_per_gas: bool,
    pub max_fee_per_gas: bool,
    pub chain_id: bool,
    pub cumulative_gas_used: bool,
    pub effective_gas_price: bool,
    pub gas_used: bool,
    pub contract_address: bool,
    pub logs_bloom: bool,
    pub type_: bool,
    pub root: bool,
    pub status: bool,
    pub sighash: bool,
    pub y_parity: bool,
    pub access_list: bool,
    pub l1_fee: bool,
    pub l1_gas_price: bool,
    pub l1_fee_scalar: bool,
    pub gas_used_for_l1: bool,
    pub max_fee_per_blob_gas: bool,
    pub blob_versioned_hashes: bool,
    pub deposit_nonce: bool,
    pub blob_gas_price: bool,
    pub deposit_receipt_version: bool,
    pub blob_gas_used: bool,
    pub l1_base_fee_scalar: bool,
    pub l1_blob_base_fee: bool,
    pub l1_blob_base_fee_scalar: bool,
    pub l1_block_number: bool,
    pub mint: bool,
    pub source_hash: bool,
}

impl TransactionFields {
    pub fn all() -> Self {
        TransactionFields {
            block_hash: true,
            block_number: true,
            from_: true,
            gas: true,
            gas_price: true,
            hash: true,
            input: true,
            nonce: true,
            to: true,
            transaction_index: true,
            value: true,
            v: true,
            r: true,
            s: true,
            max_priority_fee_per_gas: true,
            max_fee_per_gas: true,
            chain_id: true,
            cumulative_gas_used: true,
            effective_gas_price: true,
            gas_used: true,
            contract_address: true,
            logs_bloom: true,
            type_: true,
            root: true,
            status: true,
            sighash: true,
            y_parity: true,
            access_list: true,
            l1_fee: true,
            l1_gas_price: true,
            l1_fee_scalar: true,
            gas_used_for_l1: true,
            max_fee_per_blob_gas: true,
            blob_versioned_hashes: true,
            deposit_nonce: true,
            blob_gas_price: true,
            deposit_receipt_version: true,
            blob_gas_used: true,
            l1_base_fee_scalar: true,
            l1_blob_base_fee: true,
            l1_blob_base_fee_scalar: true,
            l1_block_number: true,
            mint: true,
            source_hash: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct LogFields {
    pub removed: bool,
    pub log_index: bool,
    pub transaction_index: bool,
    pub transaction_hash: bool,
    pub block_hash: bool,
    pub block_number: bool,
    pub address: bool,
    pub data: bool,
    pub topic0: bool,
    pub topic1: bool,
    pub topic2: bool,
    pub topic3: bool,
}

impl LogFields {
    pub fn all() -> Self {
        LogFields {
            removed: true,
            log_index: true,
            transaction_index: true,
            transaction_hash: true,
            block_hash: true,
            block_number: true,
            address: true,
            data: true,
            topic0: true,
            topic1: true,
            topic2: true,
            topic3: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TraceFields {
    pub from_: bool,
    pub to: bool,
    pub call_type: bool,
    pub gas: bool,
    pub input: bool,
    pub init: bool,
    pub value: bool,
    pub author: bool,
    pub reward_type: bool,
    pub block_hash: bool,
    pub block_number: bool,
    pub address: bool,
    pub code: bool,
    pub gas_used: bool,
    pub output: bool,
    pub subtraces: bool,
    pub trace_address: bool,
    pub transaction_hash: bool,
    pub transaction_position: bool,
    pub type_: bool,
    pub error: bool,
    pub sighash: bool,
    pub action_address: bool,
    pub balance: bool,
    pub refund_address: bool,
}

impl TraceFields {
    pub fn all() -> Self {
        TraceFields {
            from_: true,
            to: true,
            call_type: true,
            gas: true,
            input: true,
            init: true,
            value: true,
            author: true,
            reward_type: true,
            block_hash: true,
            block_number: true,
            address: true,
            code: true,
            gas_used: true,
            output: true,
            subtraces: true,
            trace_address: true,
            transaction_hash: true,
            transaction_position: true,
            type_: true,
            error: true,
            sighash: true,
            action_address: true,
            balance: true,
            refund_address: true,
        }
    }
}
