use crate::{evm, svm};
use arrow::array::{Array, BinaryArray, StringArray, UInt8Array};
use cherry_query::{Filter, Include, Query as GenericQuery, TableSelection};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub fn evm_query_to_generic(query: &evm::Query) -> GenericQuery {
    let mut selection = BTreeMap::new();

    selection.insert(
        "transactions".to_owned(),
        query
            .transactions
            .iter()
            .map(evm_tx_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "traces".to_owned(),
        query
            .traces
            .iter()
            .map(evm_trace_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "logs".to_owned(),
        query
            .logs
            .iter()
            .map(evm_log_selection_to_generic)
            .collect(),
    );

    if query.include_all_blocks {
        selection.insert(
            "blocks".to_owned(),
            vec![TableSelection {
                filters: BTreeMap::new(),
                include: Vec::new(),
            }],
        );
    }

    GenericQuery {
        fields: [
            (
                "blocks".to_owned(),
                field_selection_to_set(&query.fields.block)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "transactions".to_owned(),
                field_selection_to_set(&query.fields.transaction)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "traces".to_owned(),
                field_selection_to_set(&query.fields.trace)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "logs".to_owned(),
                field_selection_to_set(&query.fields.log)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
        ]
        .into_iter()
        .collect(),
        selection: Arc::new(selection),
    }
}

fn evm_tx_selection_to_generic(selection: &evm::TransactionRequest) -> TableSelection {
    let from = Arc::new(BinaryArray::from_iter_values(
        selection.from_.iter().map(|x| x.0.as_slice()),
    ));
    let to = Arc::new(BinaryArray::from_iter_values(
        selection.to.iter().map(|x| x.0.as_slice()),
    ));
    let sighash = Arc::new(BinaryArray::from_iter_values(
        selection.sighash.iter().map(|x| x.0.as_slice()),
    ));
    let status = Arc::new(UInt8Array::from_iter_values(
        selection.status.iter().copied(),
    ));
    let type_ = Arc::new(UInt8Array::from_iter_values(
        selection.type_.iter().copied(),
    ));
    let contract_deployment_address = Arc::new(BinaryArray::from_iter_values(
        selection
            .contract_deployment_address
            .iter()
            .map(|x| x.0.as_slice()),
    ));
    let hash = Arc::new(BinaryArray::from_iter_values(
        selection.hash.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 7] = [
        ("from", from),
        ("to", to),
        ("sighash", sighash),
        ("status", status),
        ("type", type_),
        ("contract_deployment_address", contract_deployment_address),
        ("hash", hash),
    ];

    let filters = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(3);

    if selection.include_logs {
        include.push(Include {
            other_table_name: "logs".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_index".to_owned(),
            ],
        });
    }

    if selection.include_traces {
        include.push(Include {
            other_table_name: "traces".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_position".to_owned(),
            ],
        });
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_number".to_owned()],
            other_table_field_names: vec!["number".to_owned()],
        });
    }

    TableSelection { filters, include }
}

fn evm_trace_selection_to_generic(selection: &evm::TraceRequest) -> TableSelection {
    let from = Arc::new(BinaryArray::from_iter_values(
        selection.from_.iter().map(|x| x.0.as_slice()),
    ));
    let to = Arc::new(BinaryArray::from_iter_values(
        selection.to.iter().map(|x| x.0.as_slice()),
    ));
    let address = Arc::new(BinaryArray::from_iter_values(
        selection.address.iter().map(|x| x.0.as_slice()),
    ));
    let call_type = Arc::new(StringArray::from_iter_values(selection.call_type.iter()));
    let reward_type = Arc::new(StringArray::from_iter_values(selection.reward_type.iter()));
    let type_ = Arc::new(StringArray::from_iter_values(selection.type_.iter()));
    let sighash = Arc::new(BinaryArray::from_iter_values(
        selection.sighash.iter().map(|x| x.0.as_slice()),
    ));
    let author = Arc::new(BinaryArray::from_iter_values(
        selection.author.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 8] = [
        ("from", from),
        ("to", to),
        ("address", address),
        ("call_type", call_type),
        ("reward_type", reward_type),
        ("type", type_),
        ("sighash", sighash),
        ("author", author),
    ];

    let filters = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(4);

    if selection.include_transaction_logs {
        include.push(Include {
            other_table_name: "logs".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_position".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_index".to_owned(),
            ],
        });
    }

    if selection.include_transaction_traces {
        include.push(Include {
            other_table_name: "traces".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_position".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_position".to_owned(),
            ],
        });
    }

    if selection.include_transactions {
        include.push(Include {
            other_table_name: "transactions".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_position".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_index".to_owned(),
            ],
        });
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_number".to_owned()],
            other_table_field_names: vec!["number".to_owned()],
        });
    }

    TableSelection { filters, include }
}

fn evm_log_selection_to_generic(selection: &evm::LogRequest) -> TableSelection {
    let address = Arc::new(BinaryArray::from_iter_values(
        selection.address.iter().map(|x| x.0.as_slice()),
    ));
    let topic0 = Arc::new(BinaryArray::from_iter_values(
        selection.topic0.iter().map(|x| x.0.as_slice()),
    ));
    let topic1 = Arc::new(BinaryArray::from_iter_values(
        selection.topic1.iter().map(|x| x.0.as_slice()),
    ));
    let topic2 = Arc::new(BinaryArray::from_iter_values(
        selection.topic2.iter().map(|x| x.0.as_slice()),
    ));
    let topic3 = Arc::new(BinaryArray::from_iter_values(
        selection.topic3.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 5] = [
        ("address", address),
        ("topic0", topic0),
        ("topic1", topic1),
        ("topic2", topic2),
        ("topic3", topic3),
    ];

    let filters = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(4);

    if selection.include_transaction_logs {
        include.push(Include {
            other_table_name: "logs".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_index".to_owned(),
            ],
        });
    }

    if selection.include_transactions {
        include.push(Include {
            other_table_name: "transactions".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_index".to_owned(),
            ],
        });
    }

    if selection.include_transaction_traces {
        include.push(Include {
            other_table_name: "traces".to_owned(),
            field_names: vec!["block_number".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec![
                "block_number".to_owned(),
                "transaction_position".to_owned(),
            ],
        });
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_number".to_owned()],
            other_table_field_names: vec!["number".to_owned()],
        });
    }

    TableSelection { filters, include }
}

pub fn svm_query_to_generic(query: &svm::Query) -> GenericQuery {
    let mut selection = BTreeMap::new();

    selection.insert(
        "instructions".to_owned(),
        query
            .instructions
            .iter()
            .map(svm_instruction_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "transactions".to_owned(),
        query
            .transactions
            .iter()
            .map(svm_transaction_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "logs".to_owned(),
        query
            .logs
            .iter()
            .map(svm_log_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "balances".to_owned(),
        query
            .balances
            .iter()
            .map(svm_balance_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "token_balances".to_owned(),
        query
            .token_balances
            .iter()
            .map(svm_token_balance_selection_to_generic)
            .collect(),
    );
    selection.insert(
        "rewards".to_owned(),
        query
            .rewards
            .iter()
            .map(svm_reward_selection_to_generic)
            .collect(),
    );

    if query.include_all_blocks {
        selection.insert(
            "blocks".to_owned(),
            vec![TableSelection {
                filters: BTreeMap::new(),
                include: Vec::new(),
            }],
        );
    }

    GenericQuery {
        fields: [
            (
                "instructions".to_owned(),
                field_selection_to_set(&query.fields.instruction)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "transactions".to_owned(),
                field_selection_to_set(&query.fields.transaction)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "logs".to_owned(),
                field_selection_to_set(&query.fields.log)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "balances".to_owned(),
                field_selection_to_set(&query.fields.balance)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "token_balances".to_owned(),
                field_selection_to_set(&query.fields.token_balance)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "rewards".to_owned(),
                field_selection_to_set(&query.fields.reward)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
            (
                "blocks".to_owned(),
                field_selection_to_set(&query.fields.block)
                    .into_iter()
                    .collect::<Vec<String>>(),
            ),
        ]
        .into_iter()
        .collect(),
        selection: Arc::new(selection),
    }
}

fn svm_instruction_selection_to_generic(selection: &svm::InstructionRequest) -> TableSelection {
    let program_id = Arc::new(BinaryArray::from_iter_values(
        selection.program_id.iter().map(|x| x.0.as_slice()),
    ));
    let discriminator = Arc::new(BinaryArray::from_iter_values(
        selection.discriminator.iter().map(|x| x.0.as_slice()),
    ));
    let d1 = Arc::new(BinaryArray::from_iter_values(
        selection.d1.iter().map(|x| x.0.as_slice()),
    ));
    let d2 = Arc::new(BinaryArray::from_iter_values(
        selection.d2.iter().map(|x| x.0.as_slice()),
    ));
    let d3 = Arc::new(BinaryArray::from_iter_values(
        selection.d3.iter().map(|x| x.0.as_slice()),
    ));
    let d4 = Arc::new(BinaryArray::from_iter_values(
        selection.d4.iter().map(|x| x.0.as_slice()),
    ));
    let d8 = Arc::new(BinaryArray::from_iter_values(
        selection.d8.iter().map(|x| x.0.as_slice()),
    ));
    let a0 = Arc::new(BinaryArray::from_iter_values(
        selection.a0.iter().map(|x| x.0.as_slice()),
    ));
    let a1 = Arc::new(BinaryArray::from_iter_values(
        selection.a1.iter().map(|x| x.0.as_slice()),
    ));
    let a2 = Arc::new(BinaryArray::from_iter_values(
        selection.a2.iter().map(|x| x.0.as_slice()),
    ));
    let a3 = Arc::new(BinaryArray::from_iter_values(
        selection.a3.iter().map(|x| x.0.as_slice()),
    ));
    let a4 = Arc::new(BinaryArray::from_iter_values(
        selection.a4.iter().map(|x| x.0.as_slice()),
    ));
    let a5 = Arc::new(BinaryArray::from_iter_values(
        selection.a5.iter().map(|x| x.0.as_slice()),
    ));
    let a6 = Arc::new(BinaryArray::from_iter_values(
        selection.a6.iter().map(|x| x.0.as_slice()),
    ));
    let a7 = Arc::new(BinaryArray::from_iter_values(
        selection.a7.iter().map(|x| x.0.as_slice()),
    ));
    let a8 = Arc::new(BinaryArray::from_iter_values(
        selection.a8.iter().map(|x| x.0.as_slice()),
    ));
    let a9 = Arc::new(BinaryArray::from_iter_values(
        selection.a9.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 17] = [
        ("program_id", program_id),
        ("data", discriminator),
        ("d1", d1),
        ("d2", d2),
        ("d3", d3),
        ("d4", d4),
        ("d8", d8),
        ("a0", a0),
        ("a1", a1),
        ("a2", a2),
        ("a3", a3),
        ("a4", a4),
        ("a5", a5),
        ("a6", a6),
        ("a7", a7),
        ("a8", a8),
        ("a9", a9),
    ];

    let mut filters: BTreeMap<String, Filter> = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else if name == "data" {
                Some((name.to_owned(), Filter::starts_with(arr).unwrap()))
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    if selection.is_committed {
        filters.insert("is_committed".to_owned(), Filter::bool(true));
    }

    let mut include = Vec::with_capacity(5);

    if selection.include_logs {
        include.push(Include {
            other_table_name: "logs".to_owned(),
            field_names: vec![
                "block_slot".to_owned(),
                "transaction_index".to_owned(),
                "instruction_address".to_owned(),
            ],
            other_table_field_names: vec![
                "block_slot".to_owned(),
                "transaction_index".to_owned(),
                "instruction_address".to_owned(),
            ],
        });
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_slot".to_owned()],
            other_table_field_names: vec!["slot".to_owned()],
        });
    }

    if selection.include_transactions {
        include.push(Include {
            other_table_name: "transactions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        });
    }

    if selection.include_inner_instructions {
        include.push(Include {
            other_table_name: "instructions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        });
    }

    if selection.include_logs {
        include.push(Include {
            other_table_name: "instructions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "instruction_address".to_owned()],
            other_table_field_names: vec![
                "block_slot".to_owned(),
                "instruction_address".to_owned(),
            ],
        });
    }

    if selection.include_transaction_token_balances {
        include.push(Include {
            other_table_name: "token_balances".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        });
    }

    TableSelection { filters, include }
}

fn svm_transaction_selection_to_generic(selection: &svm::TransactionRequest) -> TableSelection {
    let fee_payer = Arc::new(BinaryArray::from_iter_values(
        selection.fee_payer.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 1] = [("fee_payer", fee_payer)];

    let filters: BTreeMap<String, Filter> = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(3);

    if selection.include_logs {
        include.push(Include {
            other_table_name: "logs".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_instructions {
        include.push(Include {
            other_table_name: "instructions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_slot".to_owned()],
            other_table_field_names: vec!["slot".to_owned()],
        })
    }

    TableSelection { filters, include }
}

fn svm_log_selection_to_generic(selection: &svm::LogRequest) -> TableSelection {
    let program_id = Arc::new(BinaryArray::from_iter_values(
        selection.program_id.iter().map(|x| x.0.as_slice()),
    ));
    let kind = Arc::new(StringArray::from_iter_values(
        selection.kind.iter().map(|x| x.as_str()),
    ));

    let filters: [(&str, Arc<dyn Array>); 2] = [("program_id", program_id), ("kind", kind)];

    let filters: BTreeMap<String, Filter> = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(3);

    if selection.include_transactions {
        include.push(Include {
            other_table_name: "transactions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_instructions {
        include.push(Include {
            other_table_name: "instructions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_slot".to_owned()],
            other_table_field_names: vec!["slot".to_owned()],
        })
    }

    TableSelection { filters, include }
}

fn svm_balance_selection_to_generic(selection: &svm::BalanceRequest) -> TableSelection {
    let account = Arc::new(BinaryArray::from_iter_values(
        selection.account.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 1] = [("account", account)];

    let filters: BTreeMap<String, Filter> = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(3);

    if selection.include_transactions {
        include.push(Include {
            other_table_name: "transactions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_transaction_instructions {
        include.push(Include {
            other_table_name: "instructions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_slot".to_owned()],
            other_table_field_names: vec!["slot".to_owned()],
        })
    }

    TableSelection { filters, include }
}

fn svm_token_balance_selection_to_generic(selection: &svm::TokenBalanceRequest) -> TableSelection {
    let account = Arc::new(BinaryArray::from_iter_values(
        selection.account.iter().map(|x| x.0.as_slice()),
    ));
    let pre_program_id = Arc::new(BinaryArray::from_iter_values(
        selection.pre_program_id.iter().map(|x| x.0.as_slice()),
    ));
    let post_program_id = Arc::new(BinaryArray::from_iter_values(
        selection.post_program_id.iter().map(|x| x.0.as_slice()),
    ));
    let pre_mint = Arc::new(BinaryArray::from_iter_values(
        selection.pre_mint.iter().map(|x| x.0.as_slice()),
    ));
    let post_mint = Arc::new(BinaryArray::from_iter_values(
        selection.post_mint.iter().map(|x| x.0.as_slice()),
    ));
    let pre_owner = Arc::new(BinaryArray::from_iter_values(
        selection.pre_owner.iter().map(|x| x.0.as_slice()),
    ));
    let post_owner = Arc::new(BinaryArray::from_iter_values(
        selection.post_owner.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 7] = [
        ("account", account),
        ("pre_program_id", pre_program_id),
        ("post_program_id", post_program_id),
        ("pre_mint", pre_mint),
        ("post_mint", post_mint),
        ("pre_owner", pre_owner),
        ("post_owner", post_owner),
    ];

    let filters: BTreeMap<String, Filter> = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(3);

    if selection.include_transactions {
        include.push(Include {
            other_table_name: "transactions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_transaction_instructions {
        include.push(Include {
            other_table_name: "instructions".to_owned(),
            field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
            other_table_field_names: vec!["block_slot".to_owned(), "transaction_index".to_owned()],
        })
    }

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_slot".to_owned()],
            other_table_field_names: vec!["slot".to_owned()],
        })
    }

    TableSelection { filters, include }
}

fn svm_reward_selection_to_generic(selection: &svm::RewardRequest) -> TableSelection {
    let pubkey = Arc::new(BinaryArray::from_iter_values(
        selection.pubkey.iter().map(|x| x.0.as_slice()),
    ));

    let filters: [(&str, Arc<dyn Array>); 1] = [("pubkey", pubkey)];

    let filters: BTreeMap<String, Filter> = filters
        .into_iter()
        .filter_map(|(name, arr)| {
            if arr.is_empty() {
                None
            } else {
                Some((name.to_owned(), Filter::contains(arr).unwrap()))
            }
        })
        .collect();

    let mut include = Vec::with_capacity(1);

    if selection.include_blocks {
        include.push(Include {
            other_table_name: "blocks".to_owned(),
            field_names: vec!["block_slot".to_owned()],
            other_table_field_names: vec!["slot".to_owned()],
        })
    }

    TableSelection { filters, include }
}

pub fn field_selection_to_set<S: Serialize>(field_selection: &S) -> BTreeSet<String> {
    let json = serde_json::to_value(field_selection).unwrap();
    let json = json.as_object().unwrap();

    let mut output = BTreeSet::new();

    for (key, value) in json.iter() {
        if value.as_bool().unwrap() {
            output.insert(key.clone());
        }
    }

    output
}
