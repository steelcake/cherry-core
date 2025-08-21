from cryo import collect as cryo_collect
import berry_core
import typing
import polars
import pyarrow

signature = "Transfer(address indexed from, address indexed to, uint256 amount)"
topic0 = berry_core.evm_signature_to_topic0(signature)
contract_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

# get filtered events from last 10 blocks
data = cryo_collect(
    datatype="logs",
    blocks=["-10:"],
    rpc="https://eth.rpc.hypersync.xyz",
    output_format="polars",
    contract=[contract_address],  # type: ignore[arg-type]
    topic0=[topic0],  # type: ignore[arg-type]
    hex=False,
)

data = typing.cast(polars.DataFrame, data)

batches = data.to_arrow().to_batches()
batch = pyarrow.concat_batches(batches)

# decode events based on the event signature.
# This function automatically infers output types from the signature and can handle arbitrary levels
# of nesting via tuples/lists for example this: https://github.com/steelcake/cherry-core/blob/21534e31ae2e33ae62514765f25d28259ed03129/core/src/tests.rs#L18
decoded = berry_core.evm_decode_events(signature, batch, allow_decode_fail=False)

# cast to float since polars can't ffi Int256 physical type yet
# https://github.com/pola-rs/polars/blob/main/crates/polars-arrow/src/ffi/array.rs#L26
# https://github.com/pola-rs/polars/blob/main/crates/polars-arrow/src/util/macros.rs#L25
#
# This function is a helper function to do multiple cast operations at once. It casts
# the named column 'amount' to 128 bit integers in this case.
decoded = berry_core.cast_by_type(
    decoded, pyarrow.decimal256(76, 0), pyarrow.decimal128(38, 0), allow_cast_fail=False
)

# convert all binary columns to prefix hex string format like '0xabc'
decoded = berry_core.prefix_hex_encode(decoded)

decoded = polars.from_arrow(decoded)
decoded = typing.cast(polars.DataFrame, decoded)
encoded_batch = polars.from_arrow(berry_core.prefix_hex_encode(batch))
if isinstance(encoded_batch, polars.DataFrame):
    decoded = decoded.hstack(encoded_batch)
else:
    raise ValueError("encoded_batch is not a polars.DataFrame")

print(decoded)

sum = decoded.get_column("amount").sum()
print(f"total volume is {sum}")
