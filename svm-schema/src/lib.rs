use std::sync::Arc;

use arrow::array::builder;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

pub fn blocks_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, true),
        Field::new("hash", DataType::Binary, true),
        Field::new("parent_slot", DataType::UInt64, true),
        Field::new("parent_hash", DataType::Binary, true),
        Field::new("height", DataType::UInt64, true),
        Field::new("timestamp", DataType::Int64, true),
    ])
}

pub fn rewards_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("pubkey", DataType::Binary, true),
        Field::new("lamports", DataType::UInt64, true),
        Field::new("post_balance", DataType::UInt64, true),
        Field::new("reward_type", DataType::Utf8, true),
        Field::new("commission", DataType::UInt8, true),
    ])
}

pub fn token_balances_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt32, true),
        Field::new("account", DataType::Binary, true),
        Field::new("pre_mint", DataType::Binary, true),
        Field::new("post_mint", DataType::Binary, true),
        Field::new("pre_decimals", DataType::UInt16, true),
        Field::new("post_decimals", DataType::UInt16, true),
        Field::new("pre_program_id", DataType::Binary, true),
        Field::new("post_program_id", DataType::Binary, true),
        Field::new("pre_owner", DataType::Binary, true),
        Field::new("post_owner", DataType::Binary, true),
        Field::new("pre_amount", DataType::UInt64, true),
        Field::new("post_amount", DataType::UInt64, true),
    ])
}

pub fn balances_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt32, true),
        Field::new("account", DataType::Binary, true),
        Field::new("pre", DataType::UInt64, true),
        Field::new("post", DataType::UInt64, true),
    ])
}

pub fn logs_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt32, true),
        Field::new("log_index", DataType::UInt32, true),
        Field::new(
            "instruction_address",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            true,
        ),
        Field::new("program_id", DataType::Binary, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
    ])
}

pub fn transactions_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt32, true),
        Field::new("version", DataType::Int8, true),
        Field::new(
            "account_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        Field::new(
            "address_table_lookups",
            DataType::List(Arc::new(Field::new(
                "item",
                address_table_lookup_dt(),
                true,
            ))),
            true,
        ),
        Field::new("num_readonly_signed_accounts", DataType::UInt32, true),
        Field::new("num_readonly_unsigned_accounts", DataType::UInt32, true),
        Field::new("num_required_signatures", DataType::UInt32, true),
        Field::new("recent_blockhash", DataType::Binary, true),
        Field::new(
            "signatures",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        // encoded as json string
        Field::new("err", DataType::Utf8, true),
        Field::new("fee", DataType::UInt64, true),
        Field::new("compute_units_consumed", DataType::UInt64, true),
        Field::new(
            "loaded_readonly_addresses",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        Field::new(
            "loaded_writable_addresses",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        Field::new("fee_payer", DataType::Binary, true),
        Field::new("has_dropped_log_messages", DataType::Boolean, true),
    ])
}

fn address_table_lookup_dt() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("account_key", DataType::Binary, true)),
        Arc::new(Field::new(
            "writable_indexes",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        )),
        Arc::new(Field::new(
            "readonly_indexes",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        )),
    ]))
}

pub fn instructions_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("block_hash", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt32, true),
        Field::new(
            "instruction_address",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            true,
        ),
        Field::new("program_id", DataType::Binary, true),
        Field::new("a0", DataType::Binary, true),
        Field::new("a1", DataType::Binary, true),
        Field::new("a2", DataType::Binary, true),
        Field::new("a3", DataType::Binary, true),
        Field::new("a4", DataType::Binary, true),
        Field::new("a5", DataType::Binary, true),
        Field::new("a6", DataType::Binary, true),
        Field::new("a7", DataType::Binary, true),
        Field::new("a8", DataType::Binary, true),
        Field::new("a9", DataType::Binary, true),
        // accounts starting from index 10
        Field::new(
            "rest_of_accounts",
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        ),
        Field::new("data", DataType::Binary, true),
        Field::new("d1", DataType::Binary, true),
        Field::new("d2", DataType::Binary, true),
        Field::new("d4", DataType::Binary, true),
        Field::new("d8", DataType::Binary, true),
        Field::new("error", DataType::Utf8, true),
        Field::new("compute_units_consumed", DataType::UInt64, true),
        Field::new("is_committed", DataType::Boolean, true),
        Field::new("has_dropped_log_messages", DataType::Boolean, true),
    ])
}

#[derive(Default)]
pub struct BlocksBuilder {
    pub slot: builder::UInt64Builder,
    pub hash: builder::BinaryBuilder,
    pub parent_slot: builder::UInt64Builder,
    pub parent_hash: builder::BinaryBuilder,
    pub height: builder::UInt64Builder,
    pub timestamp: builder::Int64Builder,
}

impl BlocksBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(blocks_schema()),
            vec![
                Arc::new(self.slot.finish()),
                Arc::new(self.hash.finish()),
                Arc::new(self.parent_slot.finish()),
                Arc::new(self.parent_hash.finish()),
                Arc::new(self.height.finish()),
                Arc::new(self.timestamp.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct RewardsBuilder {
    pub block_slot: builder::UInt64Builder,
    pub block_hash: builder::BinaryBuilder,
    pub pubkey: builder::BinaryBuilder,
    pub lamports: builder::UInt64Builder,
    pub post_balance: builder::UInt64Builder,
    pub reward_type: builder::StringBuilder,
    pub commission: builder::UInt8Builder,
}

impl RewardsBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(rewards_schema()),
            vec![
                Arc::new(self.block_slot.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.pubkey.finish()),
                Arc::new(self.lamports.finish()),
                Arc::new(self.post_balance.finish()),
                Arc::new(self.reward_type.finish()),
                Arc::new(self.commission.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct TokenBalancesBuilder {
    pub block_slot: builder::UInt64Builder,
    pub block_hash: builder::BinaryBuilder,
    pub transaction_index: builder::UInt32Builder,
    pub account: builder::BinaryBuilder,
    pub pre_mint: builder::BinaryBuilder,
    pub post_mint: builder::BinaryBuilder,
    pub pre_decimals: builder::UInt16Builder,
    pub post_decimals: builder::UInt16Builder,
    pub pre_program_id: builder::BinaryBuilder,
    pub post_program_id: builder::BinaryBuilder,
    pub pre_owner: builder::BinaryBuilder,
    pub post_owner: builder::BinaryBuilder,
    pub pre_amount: builder::UInt64Builder,
    pub post_amount: builder::UInt64Builder,
}

impl TokenBalancesBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(token_balances_schema()),
            vec![
                Arc::new(self.block_slot.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.account.finish()),
                Arc::new(self.pre_mint.finish()),
                Arc::new(self.post_mint.finish()),
                Arc::new(self.pre_decimals.finish()),
                Arc::new(self.post_decimals.finish()),
                Arc::new(self.pre_program_id.finish()),
                Arc::new(self.post_program_id.finish()),
                Arc::new(self.pre_owner.finish()),
                Arc::new(self.post_owner.finish()),
                Arc::new(self.pre_amount.finish()),
                Arc::new(self.post_amount.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct BalancesBuilder {
    pub block_slot: builder::UInt64Builder,
    pub block_hash: builder::BinaryBuilder,
    pub transaction_index: builder::UInt32Builder,
    pub account: builder::BinaryBuilder,
    pub pre: builder::UInt64Builder,
    pub post: builder::UInt64Builder,
}

impl BalancesBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(balances_schema()),
            vec![
                Arc::new(self.block_slot.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.account.finish()),
                Arc::new(self.pre.finish()),
                Arc::new(self.post.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct LogsBuilder {
    pub block_slot: builder::UInt64Builder,
    pub block_hash: builder::BinaryBuilder,
    pub transaction_index: builder::UInt32Builder,
    pub log_index: builder::UInt32Builder,
    pub instruction_address: builder::ListBuilder<builder::UInt32Builder>,
    pub program_id: builder::BinaryBuilder,
    pub kind: builder::StringBuilder,
    pub message: builder::StringBuilder,
}

impl LogsBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(logs_schema()),
            vec![
                Arc::new(self.block_slot.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.log_index.finish()),
                Arc::new(self.instruction_address.finish()),
                Arc::new(self.program_id.finish()),
                Arc::new(self.kind.finish()),
                Arc::new(self.message.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct TransactionsBuilder {
    pub block_slot: builder::UInt64Builder,
    pub block_hash: builder::BinaryBuilder,
    pub transaction_index: builder::UInt32Builder,
    pub version: builder::Int8Builder,
    pub account_keys: builder::ListBuilder<builder::BinaryBuilder>,
    pub address_table_lookups: AddressTableLookupsBuilder,
    pub num_readonly_signed_accounts: builder::UInt32Builder,
    pub num_readonly_unsigned_accounts: builder::UInt32Builder,
    pub num_required_signatures: builder::UInt32Builder,
    pub recent_blockhash: builder::BinaryBuilder,
    pub signatures: builder::ListBuilder<builder::BinaryBuilder>,
    pub err: builder::StringBuilder,
    pub fee: builder::UInt64Builder,
    pub compute_units_consumed: builder::UInt64Builder,
    pub loaded_readonly_addresses: builder::ListBuilder<builder::BinaryBuilder>,
    pub loaded_writable_addresses: builder::ListBuilder<builder::BinaryBuilder>,
    pub fee_payer: builder::BinaryBuilder,
    pub has_dropped_log_messages: builder::BooleanBuilder,
}

pub struct AddressTableLookupsBuilder(pub builder::ListBuilder<builder::StructBuilder>);

impl Default for AddressTableLookupsBuilder {
    fn default() -> Self {
        Self(builder::ListBuilder::new(builder::StructBuilder::new(
            match address_table_lookup_dt() {
                DataType::Struct(fields) => fields,
                _ => unreachable!(),
            },
            vec![
                Box::new(builder::BinaryBuilder::default()),
                Box::new(builder::ListBuilder::new(builder::UInt64Builder::default())),
                Box::new(builder::ListBuilder::new(builder::UInt64Builder::default())),
            ],
        )))
    }
}

impl TransactionsBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(transactions_schema()),
            vec![
                Arc::new(self.block_slot.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.version.finish()),
                Arc::new(self.account_keys.finish()),
                Arc::new(self.address_table_lookups.0.finish()),
                Arc::new(self.num_readonly_signed_accounts.finish()),
                Arc::new(self.num_readonly_unsigned_accounts.finish()),
                Arc::new(self.num_required_signatures.finish()),
                Arc::new(self.recent_blockhash.finish()),
                Arc::new(self.signatures.finish()),
                Arc::new(self.err.finish()),
                Arc::new(self.fee.finish()),
                Arc::new(self.compute_units_consumed.finish()),
                Arc::new(self.loaded_readonly_addresses.finish()),
                Arc::new(self.loaded_writable_addresses.finish()),
                Arc::new(self.fee_payer.finish()),
                Arc::new(self.has_dropped_log_messages.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Default)]
pub struct InstructionsBuilder {
    pub block_slot: builder::UInt64Builder,
    pub block_hash: builder::BinaryBuilder,
    pub transaction_index: builder::UInt32Builder,
    pub instruction_address: builder::ListBuilder<builder::UInt32Builder>,
    pub program_id: builder::BinaryBuilder,
    pub a0: builder::BinaryBuilder,
    pub a1: builder::BinaryBuilder,
    pub a2: builder::BinaryBuilder,
    pub a3: builder::BinaryBuilder,
    pub a4: builder::BinaryBuilder,
    pub a5: builder::BinaryBuilder,
    pub a6: builder::BinaryBuilder,
    pub a7: builder::BinaryBuilder,
    pub a8: builder::BinaryBuilder,
    pub a9: builder::BinaryBuilder,
    pub rest_of_accounts: builder::ListBuilder<builder::BinaryBuilder>,
    pub data: builder::BinaryBuilder,
    pub d1: builder::BinaryBuilder,
    pub d2: builder::BinaryBuilder,
    pub d4: builder::BinaryBuilder,
    pub d8: builder::BinaryBuilder,
    pub error: builder::StringBuilder,
    pub compute_units_consumed: builder::UInt64Builder,
    pub is_committed: builder::BooleanBuilder,
    pub has_dropped_log_messages: builder::BooleanBuilder,
}

impl InstructionsBuilder {
    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(instructions_schema()),
            vec![
                Arc::new(self.block_slot.finish()),
                Arc::new(self.block_hash.finish()),
                Arc::new(self.transaction_index.finish()),
                Arc::new(self.instruction_address.finish()),
                Arc::new(self.program_id.finish()),
                Arc::new(self.a0.finish()),
                Arc::new(self.a1.finish()),
                Arc::new(self.a2.finish()),
                Arc::new(self.a3.finish()),
                Arc::new(self.a4.finish()),
                Arc::new(self.a5.finish()),
                Arc::new(self.a6.finish()),
                Arc::new(self.a7.finish()),
                Arc::new(self.a8.finish()),
                Arc::new(self.a9.finish()),
                Arc::new(self.rest_of_accounts.finish()),
                Arc::new(self.data.finish()),
                Arc::new(self.d1.finish()),
                Arc::new(self.d2.finish()),
                Arc::new(self.d4.finish()),
                Arc::new(self.d8.finish()),
                Arc::new(self.error.finish()),
                Arc::new(self.compute_units_consumed.finish()),
                Arc::new(self.is_committed.finish()),
                Arc::new(self.has_dropped_log_messages.finish()),
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
        RewardsBuilder::default().finish();
        TokenBalancesBuilder::default().finish();
        BalancesBuilder::default().finish();
        LogsBuilder::default().finish();
        TransactionsBuilder::default().finish();
        InstructionsBuilder::default().finish();
    }
}
