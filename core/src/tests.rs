use std::sync::Arc;

use cherry_evm_decode::{decode_events, signature_to_topic0};
use cherry_evm_validate::{validate_block_data, validate_root_hashes};
use cherry_ingest::evm::{Address, Topic};
use futures_lite::StreamExt;
use hypersync_client::{self, ClientConfig, StreamConfig};

async fn erc20(cfg: cherry_ingest::ProviderConfig, query: cherry_ingest::Query) {
    let signature = "Transfer(address indexed from, address indexed to, uint256 amount)";
    let mut stream = cherry_ingest::start_stream(cfg, query).await.unwrap();

    while let Some(v) = stream.next().await {
        let v = v.unwrap();
        let decoded = decode_events(signature, v.get("logs").unwrap(), false).unwrap();
        dbg!(decoded);
    }
}

fn erc20_query() -> cherry_ingest::Query {
    let signature = "Transfer(address indexed from, address indexed to, uint256 amount)";
    cherry_ingest::Query::Evm(cherry_ingest::evm::Query {
        from_block: 18123123,
        to_block: Some(18123222),
        fields: cherry_ingest::evm::Fields::all(),
        logs: vec![cherry_ingest::evm::LogRequest {
            address: vec![Address(decode_hex(
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ))],
            topic0: vec![Topic(signature_to_topic0(signature).unwrap())],
            ..Default::default()
        }],
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn erc20_hypersync() {
    let provider = cherry_ingest::ProviderConfig::new(cherry_ingest::ProviderKind::Hypersync);
    let query = erc20_query();
    erc20(provider, query).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn erc20_sqd() {
    let provider = cherry_ingest::ProviderConfig {
        url: Some("https://portal.sqd.dev/datasets/ethereum-mainnet".to_owned()),
        ..cherry_ingest::ProviderConfig::new(cherry_ingest::ProviderKind::Sqd)
    };
    erc20(provider, erc20_query()).await;
}

fn decode_hex<const N: usize>(hex: &str) -> [u8; N] {
    let mut dst = [0; N];
    faster_hex::hex_decode(
        hex.strip_prefix("0x").unwrap().as_bytes(),
        dst.as_mut_slice(),
    )
    .unwrap();
    dst
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn decode_nested_list() {
    let client = hypersync_client::Client::new(ClientConfig {
        url: Some("https://10.hypersync.xyz".parse().unwrap()),
        ..Default::default()
    })
    .unwrap();
    let client = Arc::new(client);

    let signature =
        "ConfiguredQuests(address editor, uint256[] questIdList, (bool, bool, bool)[] details)";

    let query = serde_json::from_value(serde_json::json!({
        "from_block": 0,
        "logs": [{
            "address": ["0xC5893DcAB9AD32Fa47923FEbdE89883C62BfFbd6"],
            "topics": [[hypersync_client::format::LogArgument::try_from(signature_to_topic0(signature).unwrap().as_slice()).unwrap()]]
        }],
        "field_selection": {
            "log": hypersync_client::schema::log()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<String>>(),
        }
    })).unwrap();

    let res = client
        .collect_arrow(query, StreamConfig::default())
        .await
        .unwrap();

    let logs = res.data.logs.iter().map(polars_arrow_to_arrow_rs);

    for batch in logs {
        let decoded = decode_events(signature, &batch, false).unwrap();

        dbg!(decoded);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn validate_eth() {
    let client = hypersync_client::Client::new(ClientConfig {
        ..Default::default()
    })
    .unwrap();
    let client = Arc::new(client);

    let query = serde_json::from_value(serde_json::json!({
        "from_block": 18123123,
        "to_block": 18123143,
        "blocks": [{}],
        "join_mode": "JoinAll",
        "field_selection": {
            "block": hypersync_client::schema::block_header()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<String>>(),
            "transaction": hypersync_client::schema::transaction()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<String>>(),
            "log": hypersync_client::schema::log()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<String>>(),
            "trace": hypersync_client::schema::trace()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<String>>(),
        }
    }))
    .unwrap();

    let res = client
        .collect_arrow(query, StreamConfig::default())
        .await
        .unwrap();

    let blocks = res.data.blocks.iter().map(polars_arrow_to_arrow_rs);
    let transactions = res.data.transactions.iter().map(polars_arrow_to_arrow_rs);
    let logs = res.data.logs.iter().map(polars_arrow_to_arrow_rs);
    let traces = res.data.traces.iter().map(polars_arrow_to_arrow_rs);

    for (((blocks, transactions), logs), traces) in blocks.zip(transactions).zip(logs).zip(traces) {
        validate_block_data(&blocks, &transactions, &logs, &traces).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn validate_evm_hypersync() {
    let query = cherry_ingest::evm::Query {
        from_block: 22040082,
        to_block: Some(22040195),

        fields: cherry_ingest::evm::Fields::all(),
        include_all_blocks: true,
        transactions: vec![cherry_ingest::evm::TransactionRequest {
            ..Default::default()
        }],
        logs: vec![cherry_ingest::evm::LogRequest {
            ..Default::default()
        }],
        traces: vec![cherry_ingest::evm::TraceRequest {
            ..Default::default()
        }],
    };

    let mut stream = cherry_ingest::start_stream(
        cherry_ingest::ProviderConfig {
            ..cherry_ingest::ProviderConfig::new(cherry_ingest::ProviderKind::Hypersync)
        },
        cherry_ingest::Query::Evm(query),
    )
    .await
    .unwrap();

    while let Some(v) = stream.next().await {
        let v = v.unwrap();
        let blocks = v.get("blocks").unwrap();
        let transactions = v.get("transactions").unwrap();
        let logs = v.get("logs").unwrap();
        let traces = v.get("traces").unwrap();

        validate_block_data(blocks, transactions, logs, traces).unwrap();

        let issues_collector_config = cherry_evm_validate::IssueCollectorConfig {
            console_output: false,
            emit_report: true,
            report_path: "reports/data_validation_issues.txt".to_string(),
            stop_on_issue: false,
            report_format: cherry_evm_validate::ReportFormat::Text,
            current_context: cherry_evm_validate::DataContext::default(),
        };
        let mut issues_collector =
            cherry_evm_validate::IssueCollector::new(issues_collector_config);
        validate_root_hashes(blocks, logs, transactions, &mut issues_collector).unwrap();
    }
}

fn polars_arrow_to_arrow_rs(
    batch: &hypersync_client::ArrowBatch,
) -> arrow::record_batch::RecordBatch {
    let data_type = polars_arrow::datatypes::ArrowDataType::Struct(batch.schema.fields.clone());
    let arr = polars_arrow::array::StructArray::new(
        data_type.clone(),
        batch.chunk.columns().to_vec(),
        None,
    );

    let arr: arrow::ffi::FFI_ArrowArray =
        unsafe { std::mem::transmute(polars_arrow::ffi::export_array_to_c(Box::new(arr))) };
    let schema: arrow::ffi::FFI_ArrowSchema = unsafe {
        std::mem::transmute(polars_arrow::ffi::export_field_to_c(
            &polars_arrow::datatypes::Field::new("", data_type, false),
        ))
    };

    let mut arr_data = unsafe { arrow::ffi::from_ffi(arr, &schema).unwrap() };

    arr_data.align_buffers();

    let arr = arrow::array::StructArray::from(arr_data);

    arrow::record_batch::RecordBatch::from(arr)
}
