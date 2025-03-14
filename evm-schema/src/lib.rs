use std::sync::Arc;

use arrow::array::builder;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

pub fn blocks_schema() -> Schema {
    Schema::new(vec![
        Field::new("number", DataType::UInt64, true),
        Field::new("hash", DataType::Binary, true),
        Field::new("parent_hash", DataType::Binary, true),
        Field::new("nonce", DataType::Binary, true),
        Field::new("sha3_uncles", DataType::Binary, true),
        Field::new("logs_bloom", DataType::Binary, true),
        Field::new("transactions_root", DataType::Binary, true),
        Field::new("state_root", DataType::Binary, true),
        Field::new("receipts_root", DataType::Binary, true),
        Field::new("miner", DataType::Binary, true),
        Field::new("difficulty", DataType::Decimal256(76, 0), true),
        Field::new("total_difficulty", DataType::Decimal256(76, 0), true),
        Field::new("extra_data", DataType::Binary, true),
        Field::new("size", DataType::Decimal256(76, 0), true),
        Field::new("gas_limit", DataType::Decimal256(76, 0), true),
        Field::new("gas_used", DataType::Decimal256(76, 0), true),
        Field::new("timestamp", DataType::Decimal256(76, 0), true),
        Field::new(
            "uncles",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        Field::new("base_fee_per_gas", DataType::Decimal256(76, 0), true),
        Field::new("blob_gas_used", DataType::Decimal256(76, 0), true),
        Field::new("excess_blob_gas", DataType::Decimal256(76, 0), true),
        Field::new("parent_beacon_block_root", DataType::Binary, true),
        Field::new("withdrawals_root", DataType::Binary, true),
        Field::new(
            "withdrawals",
            DataType::List(Arc::new(Field::new("item", withdrawal_dt(), true))),
            true,
        ),
        Field::new("l1_block_number", DataType::UInt64, true),
        Field::new("send_count", DataType::Decimal256(76, 0), true),
        Field::new("send_root", DataType::Binary, true),
        Field::new("mix_hash", DataType::Binary, true),
    ])
}

fn withdrawal_dt() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("index", DataType::UInt64, true)),
        Arc::new(Field::new("validator_index", DataType::UInt64, true)),
        Arc::new(Field::new("address", DataType::Binary, true)),
        Arc::new(Field::new("amount", DataType::Decimal256(76, 0), true)),
    ]))
}

pub fn transactions_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_hash", DataType::Binary, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("from", DataType::Binary, true),
        Field::new("gas", DataType::Decimal256(76, 0), true),
        Field::new("gas_price", DataType::Decimal256(76, 0), true),
        Field::new("hash", DataType::Binary, true),
        Field::new("input", DataType::Binary, true),
        Field::new("nonce", DataType::Decimal256(76, 0), true),
        Field::new("to", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt64, true),
        Field::new("value", DataType::Decimal256(76, 0), true),
        Field::new("v", DataType::UInt8, true),
        // keep these binary even though they are uint256 because they don't fit in i256 in
        // practice
        Field::new("r", DataType::Binary, true),
        Field::new("s", DataType::Binary, true),
        Field::new(
            "max_priority_fee_per_gas",
            DataType::Decimal256(76, 0),
            true,
        ),
        Field::new("max_fee_per_gas", DataType::Decimal256(76, 0), true),
        Field::new("chain_id", DataType::Decimal256(76, 0), true),
        Field::new("cumulative_gas_used", DataType::Decimal256(76, 0), true),
        Field::new("effective_gas_price", DataType::Decimal256(76, 0), true),
        Field::new("gas_used", DataType::Decimal256(76, 0), true),
        Field::new("contract_address", DataType::Binary, true),
        Field::new("logs_bloom", DataType::Binary, true),
        Field::new("type", DataType::UInt8, true),
        Field::new("root", DataType::Binary, true),
        Field::new("status", DataType::UInt8, true),
        Field::new("sighash", DataType::Binary, true),
        Field::new("y_parity", DataType::Boolean, true),
        Field::new(
            "access_list",
            DataType::List(Arc::new(Field::new("item", access_list_elem_dt(), true))),
            true,
        ),
        Field::new("l1_fee", DataType::Decimal256(76, 0), true),
        Field::new("l1_gas_price", DataType::Decimal256(76, 0), true),
        Field::new("l1_gas_used", DataType::Decimal256(76, 0), true),
        Field::new("l1_fee_scalar", DataType::Decimal256(76, 0), true),
        Field::new("gas_used_for_l1", DataType::Decimal256(76, 0), true),
        Field::new("max_fee_per_blob_gas", DataType::Decimal256(76, 0), true),
        Field::new(
            "blob_versioned_hashes",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        Field::new("deposit_nonce", DataType::Decimal256(76, 0), true),
        Field::new("blob_gas_price", DataType::Decimal256(76, 0), true),
        Field::new("deposit_receipt_version", DataType::Decimal256(76, 0), true),
        Field::new("blob_gas_used", DataType::Decimal256(76, 0), true),
        Field::new("l1_base_fee_scalar", DataType::Decimal256(76, 0), true),
        Field::new("l1_blob_base_fee", DataType::Decimal256(76, 0), true),
        Field::new("l1_blob_base_fee_scalar", DataType::Decimal256(76, 0), true),
        Field::new("l1_block_number", DataType::UInt64, true),
        Field::new("mint", DataType::Decimal256(76, 0), true),
        Field::new("source_hash", DataType::Binary, true),
    ])
}

fn access_list_elem_dt() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("address", DataType::Binary, true)),
        Arc::new(Field::new(
            "storage_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        )),
    ]))
}

pub fn logs_schema() -> Schema {
    Schema::new(vec![
        Field::new("removed", DataType::Boolean, true),
        Field::new("log_index", DataType::UInt64, true),
        Field::new("transaction_index", DataType::UInt64, true),
        Field::new("transaction_hash", DataType::Binary, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("address", DataType::Binary, true),
        Field::new("data", DataType::Binary, true),
        Field::new("topic0", DataType::Binary, true),
        Field::new("topic1", DataType::Binary, true),
        Field::new("topic2", DataType::Binary, true),
        Field::new("topic3", DataType::Binary, true),
    ])
}

pub fn traces_schema() -> Schema {
    Schema::new(vec![
        Field::new("from", DataType::Binary, true),
        Field::new("to", DataType::Binary, true),
        Field::new("call_type", DataType::Utf8, true),
        Field::new("gas", DataType::Decimal256(76, 0), true),
        Field::new("input", DataType::Binary, true),
        Field::new("init", DataType::Binary, true),
        Field::new("value", DataType::Decimal256(76, 0), true),
        Field::new("author", DataType::Binary, true),
        Field::new("reward_type", DataType::Utf8, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("address", DataType::Binary, true),
        Field::new("code", DataType::Binary, true),
        Field::new("gas_used", DataType::Decimal256(76, 0), true),
        Field::new("output", DataType::Binary, true),
        Field::new("subtraces", DataType::UInt64, true),
        Field::new(
            "trace_address",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new("transaction_hash", DataType::Binary, true),
        Field::new("transaction_position", DataType::UInt64, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("error", DataType::Utf8, true),
        Field::new("sighash", DataType::Binary, true),
        Field::new("action_address", DataType::Binary, true),
        Field::new("balance", DataType::Decimal256(76, 0), true),
        Field::new("refund_address", DataType::Binary, true),
    ])
}

#[derive(Default)]
pub struct BlocksBuilder {
    pub number: builder::UInt64Builder,
    pub hash: builder::BinaryBuilder,
    pub parent_hash: builder::BinaryBuilder,
    pub nonce: builder::BinaryBuilder,
    pub sha3_uncles: builder::BinaryBuilder,
    pub logs_bloom: builder::BinaryBuilder,
    pub transactions_root: builder::BinaryBuilder,
    pub state_root: builder::BinaryBuilder,
    pub receipts_root: builder::BinaryBuilder,
    pub miner: builder::BinaryBuilder,
    pub difficulty: builder::Decimal256Builder,
    pub total_difficulty: builder::Decimal256Builder,
    pub extra_data: builder::BinaryBuilder,
    pub size: builder::Decimal256Builder,
    pub gas_limit: builder::Decimal256Builder,
    pub gas_used: builder::Decimal256Builder,
    pub timestamp: builder::Decimal256Builder,
    pub uncles: builder::ListBuilder<builder::BinaryBuilder>,
    pub base_fee_per_gas: builder::Decimal256Builder,
    pub blob_gas_used: builder::Decimal256Builder,
    pub excess_blob_gas: builder::Decimal256Builder,
    pub parent_beacon_block_root: builder::BinaryBuilder,
    pub withdrawals_root: builder::BinaryBuilder,
    pub withdrawals: WithdrawalsBuilder,
    pub l1_block_number: builder::UInt64Builder,
    pub send_count: builder::Decimal256Builder,
    pub send_root: builder::BinaryBuilder,
    pub mix_hash: builder::BinaryBuilder,
}

pub struct WithdrawalsBuilder(pub builder::ListBuilder<builder::StructBuilder>);

impl Default for WithdrawalsBuilder {
    fn default() -> Self {
        Self(builder::ListBuilder::new(builder::StructBuilder::new(
            match withdrawal_dt() {
                DataType::Struct(fields) => fields,
                _ => unreachable!(),
            },
            vec![
                Box::new(builder::UInt64Builder::default()),
                Box::new(builder::UInt64Builder::default()),
                Box::new(builder::BinaryBuilder::default()),
                Box::new(
                    builder::Decimal256Builder::default()
                        .with_precision_and_scale(76, 0)
                        .unwrap(),
                ),
            ],
        )))
    }
}

impl BlocksBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(blocks_schema()),
            vec![
                Arc::new(self.number.finish()),
                Arc::new(self.hash.finish()),
                Arc::new(self.parent_hash.finish()),
                Arc::new(self.nonce.finish()),
                Arc::new(self.sha3_uncles.finish()),
                Arc::new(self.logs_bloom.finish()),
                Arc::new(self.transactions_root.finish()),
                Arc::new(self.state_root.finish()),
                Arc::new(self.receipts_root.finish()),
                Arc::new(self.miner.finish()),
                Arc::new(
                    self.difficulty
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.total_difficulty
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.extra_data.finish()),
                Arc::new(self.size.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(
                    self.gas_limit
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.timestamp
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.uncles.finish()),
                Arc::new(
                    self.base_fee_per_gas
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.blob_gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.excess_blob_gas
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.parent_beacon_block_root.finish()),
                Arc::new(self.withdrawals_root.finish()),
                Arc::new(self.withdrawals.0.finish()),
                Arc::new(self.l1_block_number.finish()),
                Arc::new(
                    self.send_count
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.send_root.finish()),
                Arc::new(self.mix_hash.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct TransactionsBuilder {
    pub block_hash: builder::BinaryBuilder,
    pub block_number: builder::UInt64Builder,
    pub from: builder::BinaryBuilder,
    pub gas: builder::Decimal256Builder,
    pub gas_price: builder::Decimal256Builder,
    pub hash: builder::BinaryBuilder,
    pub input: builder::BinaryBuilder,
    pub nonce: builder::Decimal256Builder,
    pub to: builder::BinaryBuilder,
    pub transaction_index: builder::UInt64Builder,
    pub value: builder::Decimal256Builder,
    pub v: builder::UInt8Builder,
    pub r: builder::BinaryBuilder,
    pub s: builder::BinaryBuilder,
    pub max_priority_fee_per_gas: builder::Decimal256Builder,
    pub max_fee_per_gas: builder::Decimal256Builder,
    pub chain_id: builder::Decimal256Builder,
    pub cumulative_gas_used: builder::Decimal256Builder,
    pub effective_gas_price: builder::Decimal256Builder,
    pub gas_used: builder::Decimal256Builder,
    pub contract_address: builder::BinaryBuilder,
    pub logs_bloom: builder::BinaryBuilder,
    pub type_: builder::UInt8Builder,
    pub root: builder::BinaryBuilder,
    pub status: builder::UInt8Builder,
    pub sighash: builder::BinaryBuilder,
    pub y_parity: builder::BooleanBuilder,
    pub access_list: AccessListBuilder,
    pub l1_fee: builder::Decimal256Builder,
    pub l1_gas_price: builder::Decimal256Builder,
    pub l1_gas_used: builder::Decimal256Builder,
    pub l1_fee_scalar: builder::Decimal256Builder,
    pub gas_used_for_l1: builder::Decimal256Builder,
    pub max_fee_per_blob_gas: builder::Decimal256Builder,
    pub blob_versioned_hashes: builder::ListBuilder<builder::BinaryBuilder>,
    pub deposit_nonce: builder::Decimal256Builder,
    pub blob_gas_price: builder::Decimal256Builder,
    pub deposit_receipt_version: builder::Decimal256Builder,
    pub blob_gas_used: builder::Decimal256Builder,
    pub l1_base_fee_scalar: builder::Decimal256Builder,
    pub l1_blob_base_fee: builder::Decimal256Builder,
    pub l1_blob_base_fee_scalar: builder::Decimal256Builder,
    pub l1_block_number: builder::UInt64Builder,
    pub mint: builder::Decimal256Builder,
    pub source_hash: builder::BinaryBuilder,
}

pub struct AccessListBuilder(pub builder::ListBuilder<builder::StructBuilder>);

impl Default for AccessListBuilder {
    fn default() -> Self {
        Self(builder::ListBuilder::new(builder::StructBuilder::new(
            match access_list_elem_dt() {
                DataType::Struct(fields) => fields,
                _ => unreachable!(),
            },
            vec![
                Box::new(builder::BinaryBuilder::default()),
                Box::new(builder::ListBuilder::new(builder::BinaryBuilder::default())),
            ],
        )))
    }
}

impl TransactionsBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(transactions_schema()),
            vec![
                Arc::new(self.block_hash.finish()),
                Arc::new(self.block_number.finish()),
                Arc::new(self.from.finish()),
                Arc::new(self.gas.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(
                    self.gas_price
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.hash.finish()),
                Arc::new(self.input.finish()),
                Arc::new(self.nonce.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(self.to.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.value.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(self.v.finish()),
                Arc::new(self.r.finish()),
                Arc::new(self.s.finish()),
                Arc::new(
                    self.max_priority_fee_per_gas
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.max_fee_per_gas
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.chain_id
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.cumulative_gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.effective_gas_price
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.contract_address.finish()),
                Arc::new(self.logs_bloom.finish()),
                Arc::new(self.type_.finish()),
                Arc::new(self.root.finish()),
                Arc::new(self.status.finish()),
                Arc::new(self.sighash.finish()),
                Arc::new(self.y_parity.finish()),
                Arc::new(self.access_list.0.finish()),
                Arc::new(
                    self.l1_fee
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.l1_gas_price
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.l1_gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.l1_fee_scalar
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.gas_used_for_l1
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.max_fee_per_blob_gas
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.blob_versioned_hashes.finish()),
                Arc::new(
                    self.deposit_nonce
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.blob_gas_price
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.deposit_receipt_version
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.blob_gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.l1_base_fee_scalar
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.l1_blob_base_fee
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(
                    self.l1_blob_base_fee_scalar
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.l1_block_number.finish()),
                Arc::new(self.mint.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(self.source_hash.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct LogsBuilder {
    pub removed: builder::BooleanBuilder,
    pub log_index: builder::UInt64Builder,
    pub transaction_index: builder::UInt64Builder,
    pub transaction_hash: builder::BinaryBuilder,
    pub block_hash: builder::BinaryBuilder,
    pub block_number: builder::UInt64Builder,
    pub address: builder::BinaryBuilder,
    pub data: builder::BinaryBuilder,
    pub topic0: builder::BinaryBuilder,
    pub topic1: builder::BinaryBuilder,
    pub topic2: builder::BinaryBuilder,
    pub topic3: builder::BinaryBuilder,
}

impl LogsBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(logs_schema()),
            vec![
                Arc::new(self.removed.finish()),
                Arc::new(self.log_index.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.transaction_hash.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.block_number.finish()),
                Arc::new(self.address.finish()),
                Arc::new(self.data.finish()),
                Arc::new(self.topic0.finish()),
                Arc::new(self.topic1.finish()),
                Arc::new(self.topic2.finish()),
                Arc::new(self.topic3.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct TracesBuilder {
    pub from: builder::BinaryBuilder,
    pub to: builder::BinaryBuilder,
    pub call_type: builder::StringBuilder,
    pub gas: builder::Decimal256Builder,
    pub input: builder::BinaryBuilder,
    pub init: builder::BinaryBuilder,
    pub value: builder::Decimal256Builder,
    pub author: builder::BinaryBuilder,
    pub reward_type: builder::StringBuilder,
    pub block_hash: builder::BinaryBuilder,
    pub block_number: builder::UInt64Builder,
    pub address: builder::BinaryBuilder,
    pub code: builder::BinaryBuilder,
    pub gas_used: builder::Decimal256Builder,
    pub output: builder::BinaryBuilder,
    pub subtraces: builder::UInt64Builder,
    pub trace_address: builder::ListBuilder<builder::UInt64Builder>,
    pub transaction_hash: builder::BinaryBuilder,
    pub transaction_position: builder::UInt64Builder,
    pub type_: builder::StringBuilder,
    pub error: builder::StringBuilder,
    pub sighash: builder::BinaryBuilder,
    pub action_address: builder::BinaryBuilder,
    pub balance: builder::Decimal256Builder,
    pub refund_address: builder::BinaryBuilder,
}

impl TracesBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(traces_schema()),
            vec![
                Arc::new(self.from.finish()),
                Arc::new(self.to.finish()),
                Arc::new(self.call_type.finish()),
                Arc::new(self.gas.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(self.input.finish()),
                Arc::new(self.init.finish()),
                Arc::new(self.value.with_precision_and_scale(76, 0).unwrap().finish()),
                Arc::new(self.author.finish()),
                Arc::new(self.reward_type.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.block_number.finish()),
                Arc::new(self.address.finish()),
                Arc::new(self.code.finish()),
                Arc::new(
                    self.gas_used
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.output.finish()),
                Arc::new(self.subtraces.finish()),
                Arc::new(self.trace_address.finish()),
                Arc::new(self.transaction_hash.finish()),
                Arc::new(self.transaction_position.finish()),
                Arc::new(self.type_.finish()),
                Arc::new(self.error.finish()),
                Arc::new(self.sighash.finish()),
                Arc::new(self.action_address.finish()),
                Arc::new(
                    self.balance
                        .with_precision_and_scale(76, 0)
                        .unwrap()
                        .finish(),
                ),
                Arc::new(self.refund_address.finish()),
            ],
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        BlocksBuilder::default().finish();
        TransactionsBuilder::default().finish();
        LogsBuilder::default().finish();
        TracesBuilder::default().finish();
    }
}
