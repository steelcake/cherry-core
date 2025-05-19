use alloy_multicall::Multicall;
use alloy_primitives::{Address, U256};
use alloy_provider::ProviderBuilder;
use alloy_sol_types::{sol, JsonAbiExt};
use anyhow::{Context, Result};
use arrow::{
    array::{Array, FixedSizeBinaryBuilder, StringArray, UInt8Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::{str::FromStr, sync::Arc};

sol! {
    #[derive(Debug)]
    #[sol(abi)]
    function decimals()
        public
        view
        virtual
        override
        returns (uint8);

    #[derive(Debug)]
    #[sol(abi)]
    function symbol()
        public
        view
        virtual
        override
        returns (string memory);

    #[derive(Debug)]
    #[sol(abi)]
    function name()
        public
        view
        virtual
        override
        returns (string memory);

    #[derive(Debug)]
    #[sol(abi)]
    function totalSupply()
        public
        view
        virtual
        override
        returns (uint256);
}

#[derive(Debug)]
pub struct TokenMetadata {
    pub address: Option<Address>,
    pub decimals: Option<u8>,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub total_supply: Option<U256>,
}

#[derive(Debug)]
pub struct TokenMetadataSelector {
    pub decimals: bool,
    pub symbol: bool,
    pub name: bool,
    pub total_supply: bool,
}

impl Default for TokenMetadataSelector {
    fn default() -> Self {
        Self {
            decimals: true,
            symbol: true,
            name: true,
            total_supply: false,
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for TokenMetadataSelector {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;
        use pyo3::types::PyDict;
        // Get the dictionary
        let dict = ob.downcast::<PyDict>()?;

        let decimals = dict.get_item("decimals").unwrap();
        let symbol = dict.get_item("symbol").unwrap();
        let name = dict.get_item("name").unwrap();
        let total_supply = dict.get_item("total_supply").unwrap();

        Ok(TokenMetadataSelector {
            decimals: decimals.extract::<bool>()?,
            symbol: symbol.extract::<bool>()?,
            name: name.extract::<bool>()?,
            total_supply: total_supply.extract::<bool>()?,
        })
    }
}

pub async fn get_token_metadata(
    rpc_url: &str,
    addresses: Vec<String>,
    selector: &TokenMetadataSelector,
) -> Result<Vec<TokenMetadata>> {
    let provider = ProviderBuilder::new().on_http(rpc_url.parse().context("invalid rpc url")?);
    let mut multicall = Multicall::with_provider_chain_id(&provider)
        .await
        .context("failed to create multicall")?;

    let decimals = decimalsCall::abi();
    let symbol = symbolCall::abi();
    let name = nameCall::abi();
    let total_supply = totalSupplyCall::abi();

    let addresses: Vec<Option<Address>> = addresses
        .into_iter()
        .map(|addr| Address::from_str(&addr).ok())
        .collect();
    for address in addresses.iter().flatten() {
        if selector.decimals {
            multicall.add_call(*address, &decimals, &[], true);
        }
        if selector.symbol {
            multicall.add_call(*address, &symbol, &[], true);
        }
        if selector.name {
            multicall.add_call(*address, &name, &[], true);
        }
        if selector.total_supply {
            multicall.add_call(*address, &total_supply, &[], true);
        }
    }

    let results = multicall.call().await.context("failed to call multicall")?;
    let mut token_metadata: Vec<TokenMetadata> = Vec::new();

    // Process results in chunks (decimals, symbol, name, total_supply)
    let mut i = 0;
    let chuck_size = selector.decimals as usize
        + selector.symbol as usize
        + selector.name as usize
        + selector.total_supply as usize;
    for address in addresses.iter() {
        if let Some(address) = address {
            let mut base_idx = i * chuck_size;
            let decimals: Option<u8> = if selector.decimals {
                results
                    .get(base_idx)
                    .and_then(|result| result.as_ref().ok())
                    .and_then(|v| v.as_uint())
                    .map(|uint| uint.0.as_limbs()[0] as u8)
            } else {
                None
            };
            let symbol: Option<String> = if selector.symbol {
                base_idx += 1;
                results
                    .get(base_idx)
                    .and_then(|result| result.as_ref().ok())
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                None
            };
            let name: Option<String> = if selector.name {
                base_idx += 1;
                results
                    .get(base_idx)
                    .and_then(|result| result.as_ref().ok())
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                None
            };
            let total_supply: Option<U256> = if selector.total_supply {
                base_idx += 1;
                results
                    .get(base_idx)
                    .and_then(|result| result.as_ref().ok())
                    .and_then(|v| v.as_uint())
                    .map(|uint| uint.0)
            } else {
                None
            };

            token_metadata.push(TokenMetadata {
                address: Some(*address),
                decimals,
                symbol,
                name,
                total_supply,
            });
            i += 1;
        } else {
            token_metadata.push(TokenMetadata {
                address: None,
                decimals: None,
                symbol: None,
                name: None,
                total_supply: None,
            });
        }
    }

    Ok(token_metadata)
}

pub fn token_metadata_to_table(
    token_metadata: Vec<TokenMetadata>,
    selector: &TokenMetadataSelector,
) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    fields.push(Field::new("address", DataType::FixedSizeBinary(20), true));
    if selector.decimals {
        fields.push(Field::new("decimals", DataType::UInt8, true));
    }
    if selector.symbol {
        fields.push(Field::new("symbol", DataType::Utf8, true));
    }
    if selector.name {
        fields.push(Field::new("name", DataType::Utf8, true));
    }
    if selector.total_supply {
        fields.push(Field::new(
            "total_supply",
            DataType::FixedSizeBinary(32),
            true,
        ));
    }

    let schema = Schema::new(fields);

    let array_len = token_metadata.len();
    let mut address_builder = FixedSizeBinaryBuilder::with_capacity(array_len, 20);
    let mut decimals_values: Vec<Option<u8>> = Vec::with_capacity(array_len);
    let mut symbol_values: Vec<Option<String>> = Vec::with_capacity(array_len);
    let mut name_values: Vec<Option<String>> = Vec::with_capacity(array_len);
    let mut total_supply_builder = FixedSizeBinaryBuilder::with_capacity(array_len, 32);

    for token in token_metadata {
        let address_bytes: Option<[u8; 20]> = token
            .address
            .and_then(|addr| addr.as_slice().try_into().ok());
        match address_bytes {
            Some(address_bytes) => {
                let _ = address_builder.append_value(address_bytes);
            }
            None => address_builder.append_null(),
        }

        if selector.decimals {
            decimals_values.push(token.decimals);
        }
        if selector.symbol {
            symbol_values.push(token.symbol);
        }
        if selector.name {
            name_values.push(token.name);
        }
        if selector.total_supply {
            match token.total_supply {
                Some(supply) => {
                    // Add explicit type annotation to fix the error
                    let bytes: [u8; 32] = supply.to_be_bytes();
                    let _ = total_supply_builder.append_value(bytes);
                }
                None => total_supply_builder.append_null(),
            }
        }
    }

    let mut arrays = Vec::new();
    arrays.push(Arc::new(address_builder.finish()) as Arc<dyn Array>);
    if selector.decimals {
        arrays.push(Arc::new(UInt8Array::from(decimals_values)) as Arc<dyn Array>);
    }
    if selector.symbol {
        arrays.push(Arc::new(StringArray::from(symbol_values)) as Arc<dyn Array>);
    }
    if selector.name {
        arrays.push(Arc::new(StringArray::from(name_values)) as Arc<dyn Array>);
    }
    if selector.total_supply {
        arrays.push(Arc::new(total_supply_builder.finish()) as Arc<dyn Array>);
    }
    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

    Ok(batch)
}

#[tokio::test]
#[ignore]
async fn test_get_token_metadata() {
    let selector = TokenMetadataSelector {
        decimals: true,
        symbol: false,
        name: true,
        total_supply: false,
    };
    let token_metadata = get_token_metadata(
        "https://ethereum-rpc.publicnode.com",
        vec![
            "Invalid address".to_string(),
            "0x0000000000000000000000000000000000000000".to_string(),
            "0x6B175474E89094C44Da98b954EedeAC495271d0F".to_string(),
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
            "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84".to_string(),
        ],
        &selector,
    )
    .await;

    let table = token_metadata_to_table(token_metadata.unwrap(), &selector).unwrap();

    println!("{:?}", table);
}
