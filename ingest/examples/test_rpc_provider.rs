use cherry_ingest::{evm, ProviderConfig, ProviderKind, Query};
use futures_lite::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Public RPC endpoint (can be changed via ETH_RPC_URL env)
    let url =
        std::env::var("ETH_RPC_URL").unwrap_or_else(|_| "https://eth.llamarpc.com".to_string());

    let config = ProviderConfig {
        kind: ProviderKind::Rpc,
        url: Some(url),
        bearer_token: None,
        buffer_size: Some(10),
        ..ProviderConfig::new(ProviderKind::Rpc)
    };

    let query = evm::Query {
        from_block: 18_000_000,
        to_block: Some(18_000_000),
        include_all_blocks: true,
        fields: evm::Fields {
            block: evm::BlockFields {
                number: true,
                hash: true,
                timestamp: true,
                gas_used: true,
                ..Default::default()
            },
            transaction: evm::TransactionFields {
                hash: true,
                from_: true,
                to: true,
                value: true,
                gas: true,
                ..Default::default()
            },
            log: evm::LogFields {
                address: true,
                topic0: true,
                data: true,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };

    let mut stream = cherry_ingest::start_stream(config, Query::Evm(query)).await?;

    let mut blocks_received = 0;
    let mut transactions_received = 0;
    let mut logs_received = 0;

    while let Some(result) = stream.next().await {
        let data = result?;

        if let Some(blocks) = data.get("blocks") {
            blocks_received += blocks.num_rows();
            assert_schema_subset(
                blocks.schema().as_ref(),
                &cherry_evm_schema::blocks_schema(),
            );
        }

        if let Some(txs) = data.get("transactions") {
            transactions_received += txs.num_rows();
            assert_schema_subset(
                txs.schema().as_ref(),
                &cherry_evm_schema::transactions_schema(),
            );
        }

        if let Some(logs) = data.get("logs") {
            logs_received += logs.num_rows();
            assert_schema_subset(logs.schema().as_ref(), &cherry_evm_schema::logs_schema());
        }
    }

    assert!(blocks_received >= 1, "Should receive at least 1 block");
    assert!(
        transactions_received >= 0,
        "Transactions may be zero in a narrow range"
    );
    assert!(logs_received >= 0, "Logs may be zero in a narrow range");

    println!(
        "✅ Passed: blocks={}, txs={}, logs={}",
        blocks_received, transactions_received, logs_received
    );

    // Test 2: Filter logs by address (USDT contract)
    let url =
        std::env::var("ETH_RPC_URL").unwrap_or_else(|_| "https://eth.llamarpc.com".to_string());
    let config = ProviderConfig {
        kind: ProviderKind::Rpc,
        url: Some(url),
        ..ProviderConfig::new(ProviderKind::Rpc)
    };

    let usdt_address = "0xdac17f958d2ee523a2206206994597c13d831ec7";
    let usdt_address = {
        let mut arr = [0u8; 20];
        let v = faster_hex::hex_decode(usdt_address.trim_start_matches("0x").as_bytes(), &mut arr)
            .map(|_| arr)
            .expect("hex decode");
        evm::Address(v)
    };

    let query2 = evm::Query {
        from_block: 18_000_000,
        to_block: Some(18_000_001),
        logs: vec![evm::LogRequest {
            address: vec![usdt_address],
            ..Default::default()
        }],
        fields: evm::Fields {
            log: evm::LogFields {
                address: true,
                data: true,
                topic0: true,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };

    let mut stream2 = cherry_ingest::start_stream(config, Query::Evm(query2)).await?;
    let mut usdt_logs = 0usize;
    while let Some(result) = stream2.next().await {
        if let Some(logs) = result?.get("logs") {
            usdt_logs += logs.num_rows();
        }
    }

    assert!(usdt_logs > 0, "Should find USDT logs");
    println!("✅ Found {} USDT logs", usdt_logs);

    Ok(())
}

fn assert_schema_subset(actual: &arrow::datatypes::Schema, full: &arrow::datatypes::Schema) {
    use arrow::datatypes::DataType;
    for f in actual.fields() {
        let name = f.name();
        let full_f = full
            .field_with_name(name)
            .expect("field exists in full schema");
        // Compare types ignoring metadata
        assert_eq!(
            f.data_type(),
            full_f.data_type(),
            "mismatched type for field {name}"
        );
    }
}
