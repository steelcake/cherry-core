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
        Field::new("commission", DataType::UInt64, true),
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
        Field::new("instruction_address", DataType::Binary, true),
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
        Field::new("account_keys", DataType::List(Arc::new(Field::new("item", DataType::Binary, true))), true),
        Field::new("address_table_lookups", DataType::List(Arc::new(Field::new("item", address_table_lookup_dt(), true))), true),
        Field::new("num_readonly_signed_accounts", DataType::UInt32, true),
        Field::new("num_readonly_unsigned_accounts", DataType::UInt32, true),
        Field::new("num_required_signatures", DataType::UInt32, true),
        Field::new("recent_blockhash", DataType::Binary, true),
        Field::new("signatures", DataType::List(Arc::new(Field::new("item", DataType::Binary, true))), true),
        // encoded as json string
        Field::new("err", DataType::Utf8, true),
        Field::new("fee", DataType::UInt64, true),
        Field::new("compute_units_consumed", DataType::UInt64, true),
        Field::new("loaded_readonly_addresses", DataType::List(Arc::new(Field::new("item", DataType::Binary, true))), true),
        Field::new("loaded_writeable_addresses", DataType::List(Arc::new(Field::new("item", DataType::Binary, true))), true),
        Field::new("fee_payer", DataType::Binary, true),
        Field::new("has_dropped_log_messages", DataType::Boolean, true),
    ])
}

fn address_table_lookup_dt() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("account_key", DataType::Binary, true)),
        Arc::new(Field::new("writeable_indexes", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), true)),
        Arc::new(Field::new("readonly_indexes", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), true)),
    ]))
}

pub fn instructions_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_slot", DataType::UInt64, true),
        Field::new("transaction_index", DataType::UInt32, true),
        Field::new("instruction_address", DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))), true),
        Field::new("program_id", DataType::Binary, true),
        Field::new("accounts", DataType::List(Arc::new(Field::new("item", DataType::Binary, true))), true),
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
