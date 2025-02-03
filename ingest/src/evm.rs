#[derive(Default, Debug, Clone)]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    // pub blocks: Vec<BlockRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub logs: Vec<LogRequest>,
    pub traces: Vec<TraceRequest>,
    pub join_mode: JoinMode,
}

impl Query {
    pub fn to_sqd(&self) -> sqd_portal_client::evm::Query {
        todo!()
    }
}

pub type Hash = [u8; 32];
pub type Address = [u8; 20];
pub type Sighash = [u8; 4];
pub type Topic = [u8; 32];

#[derive(Default, Clone, Copy, Debug)]
pub enum JoinMode {
    Default,
    JoinAll,
    #[default]
    JoinNothing,
}

// #[derive(Default, Debug, Clone)]
// pub struct BlockRequest {
//     pub hash: Vec<Hash>,
//     pub miner: Vec<Address>,
// }

#[derive(Default, Debug, Clone)]
pub struct TransactionRequest {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub sighash: Vec<Sighash>,
    pub status: Vec<u8>,
    pub type_: Vec<u8>,
    pub contract_deployment_address: Vec<Address>,
    pub hash: Vec<Hash>,
}

#[derive(Default, Debug, Clone)]
pub struct LogRequest {
    pub address: Vec<Address>,
    pub topic0: Vec<Topic>,
    pub topic1: Vec<Topic>,
    pub topic2: Vec<Topic>,
    pub topic3: Vec<Topic>,
}

#[derive(Default, Debug, Clone)]
pub struct TraceRequest {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub address: Vec<Address>,
    pub call_type: Vec<String>,
    pub reward_type: Vec<String>,
    pub type_: Vec<String>,
    pub sighash: Vec<Sighash>,
    pub author: Vec<Address>,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FieldSelection {
    pub block: BlockFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub trace: TraceFields,
}

#[derive(Default, Debug, Clone, Copy)]
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

#[derive(Default, Debug, Clone, Copy)]
pub struct TransactionFields {
    pub block_hash: bool,
    pub block_number: bool,
    pub from: bool,
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

#[derive(Default, Debug, Clone, Copy)]
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

#[derive(Default, Debug, Clone, Copy)]
pub struct TraceFields {
    pub from: bool,
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
