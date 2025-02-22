from cryo import collect as cryo_collect
import cherry_core
import typing
import polars
import pyarrow

signature = "Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)"
topic0 = cherry_core.evm_signature_to_topic0(signature)
# contract_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

# get filtered events from last 10 blocks
data = cryo_collect(
    datatype="logs",
    blocks=["-10:"],
    rpc="https://base.rpc.hypersync.xyz",
    output_format='polars',
    contract=[],
    topic0=[topic0],
    hex=False,
)

data = typing.cast(polars.DataFrame, data) 

batches = data.to_arrow().to_batches()
batch = pyarrow.concat_batches(batches)

# cast large_binary columns to regular binary, not sure why all binary columns end up being large_binary
for (i, col) in enumerate(batch.columns):
    if pyarrow.types.is_large_binary(col.type):
        batch = batch.set_column(i, batch.column_names[i], col.cast(target_type=pyarrow.binary()))

# decode events based on the event signature.
# This function automatically infers output types from the signature and can handle arbitrary levels
# of nesting via tuples/lists for example this: https://github.com/steelcake/cherry-core/blob/21534e31ae2e33ae62514765f25d28259ed03129/core/src/tests.rs#L18
decoded = cherry_core.evm_decode_events(signature, batch, allow_decode_fail=False)

# cast to float since polars can't ffi Int256 physical type yet
# https://github.com/pola-rs/polars/blob/main/crates/polars-arrow/src/ffi/array.rs#L26
# https://github.com/pola-rs/polars/blob/main/crates/polars-arrow/src/util/macros.rs#L25
#
# This function is a helper function to do multiple cast operations at once. It casts
# the named column 'amount' to 128 bit integers in this case.
decoded = cherry_core.cast([("amount", "Decimal128(38, 0)")], decoded, allow_cast_fail=False)

# convert all binary columns to prefix hex string format like '0xabc'
decoded = cherry_core.prefix_hex_encode(decoded)

decoded = polars.from_arrow(decoded)
decoded = typing.cast(polars.DataFrame, decoded)
decoded = decoded.hstack(polars.from_arrow(cherry_core.prefix_hex_encode(batch)))

print(decoded)

sum = decoded.get_column("amount").sum()
print(f"total volume is {sum}")

