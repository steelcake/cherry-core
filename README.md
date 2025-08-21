# 🍓 Berry-Core

**High-performance blockchain data processing library forked from cherry-core**

Berry-Core is a blazingly fast Python library built with Rust for processing blockchain data across multiple networks including Ethereum, Solana, and other EVM-compatible chains. Originally forked from the excellent [cherry-core](https://github.com/steelcake/cherry-core) project, Berry-Core adds new features and improvements while maintaining full compatibility.

## 🚀 Features

- **🔗 Multi-Chain Support**: Ethereum, Solana, Polygon, BSC, and other EVM chains
- **⚡ High Performance**: Rust-powered backend with Python bindings for maximum speed
- **📊 Multiple Data Sources**: Hypersync, SQD, RPC providers, and Yellowstone gRPC
- **🔄 Real-time Streaming**: Continuous data ingestion with automatic retry mechanisms
- **🎯 Flexible Querying**: Filter by blocks, transactions, logs, and traces
- **🛠 Data Processing**: Built-in encoding/decoding for addresses, hashes, and structured data
- **📈 Token Metadata**: Fetch ERC-20 token information including decimals, symbols, and supply
- **🔍 DEX Pool Analysis**: Extract token pairs from Uniswap V2/V3 and other DEX protocols
- **📦 Apache Arrow**: Native Arrow format support for efficient data processing

## 🆚 Cherry-Core vs Berry-Core

| Feature | Cherry-Core | Berry-Core |
|---------|-------------|------------|
| **Maintenance** | ❌ No longer maintained | ✅ **Active development** |
| **Pool Data** | ❌ Not available | ✅ **`get_pools_token0_token1()`** |
| **Token Metadata** | ✅ Basic support | ✅ **Enhanced with better error handling** |
| **Documentation** | ❌ Limited | ✅ **Comprehensive docs & examples** |
| **CI/CD** | ❌ Broken | ✅ **Automated PyPI releases** |
| **Dependencies** | ❌ Outdated | ✅ **Latest versions** |
| **Python Versions** | ✅ 3.8+ | ✅ **3.8+ with better compatibility** |
| **License** | ✅ MIT/Apache-2.0 | ✅ **Same dual license** |

## 📦 Installation

```bash
pip install berry-core
```

## 🎯 Quick Start

### Basic Token Metadata

```python
import berry_core
import polars as pl

# Get token metadata
addresses = [
    "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
]

metadata = berry_core.get_token_metadata(
    "https://ethereum-rpc.publicnode.com", 
    addresses
)

# Convert to DataFrame
df = pl.from_arrow(berry_core.get_token_metadata_as_table(
    "https://ethereum-rpc.publicnode.com",
    addresses,
    {"decimals": True, "symbol": True, "name": True}
))
print(df)
```

### 🏊 DEX Pool Token Extraction

```python
import berry_core

# Extract token0/token1 from DEX pool data
pool_data = berry_core.get_pools_token0_token1(
    rpc_url="https://ethereum-rpc.publicnode.com",
    pool_addresses=["0x..."],  # Uniswap V2/V3 pool addresses
)
```

### 📡 Real-time Data Streaming

```python
from berry_core import ingest
import asyncio

async def stream_blocks():
    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=21930160,
            include_all_blocks=True,
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(number=True)
            ),
        ),
    )
    
    provider = ingest.ProviderConfig(
        kind=ingest.ProviderKind.HYPERSYNC,
        stop_on_head=False,
    )
    
    stream = ingest.start_stream(provider, query)
    
    while True:
        data = await stream.next()
        if data is None:
            break
        print(f"Block: {data['blocks'].column('number')}")

asyncio.run(stream_blocks())
```

### 🔐 SVM Instruction Decoding (Solana)

```python
from berry_core.svm_decode import InstructionSignature, ParamInput, DynType
from berry_core import svm_decode_instructions
import pyarrow.parquet as pq

# Define instruction signature
signature = InstructionSignature(
    discriminator="e517cb977ae3ad2a",
    params=[
        ParamInput(name="amount", param_type=DynType.U64),
        ParamInput(name="recipient", param_type=DynType.Pubkey),
    ]
)

# Decode instructions from parquet file
table = pq.read_table("instructions.parquet")
decoded = svm_decode_instructions(signature, table.to_batches()[0], True)
```

## 🌟 Data Sources

- **Hypersync**: Ultra-fast historical and real-time data
- **SQD Network**: Decentralized data indexing
- **RPC Providers**: Direct blockchain node access
- **Yellowstone gRPC**: High-performance Solana streaming

## 🙏 Credits

This project is a fork of [cherry-core](https://github.com/steelcake/cherry-core) by Ozgur Akkurt. We're grateful for the excellent foundation provided by the original project and continue to build upon its solid architecture.

## 📄 License

Licensed under either of:

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## 🤝 Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.