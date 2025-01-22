use std::sync::Arc;

use cherry_evm_decode::{decode_events, signature_to_topic0};
use cherry_evm_validate::validate_block_data;
use hypersync_client::{self, ClientConfig, StreamConfig};

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
async fn decode_erc20() {
    let client = hypersync_client::Client::new(ClientConfig::default()).unwrap();
    let client = Arc::new(client);

    let signature = "Transfer(address indexed from, address indexed to, uint256 amount)";

    let query = serde_json::from_value(serde_json::json!({
        "from_block": 18123123,
        "to_block": 18123222,
        "logs": [{
            "address": ["0xdAC17F958D2ee523a2206206994597C13D831ec7"],
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
