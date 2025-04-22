import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from cherry_core.svm_decode import (
    LogSignature,
    ParamInput,
    DynType,
)
from cherry_core import svm_decode_logs

current_dir = Path(__file__).parent
input_file = current_dir / "logs.parquet"
output_file = current_dir / "decoded_logs.parquet"

print(f"Reading input file: {input_file}")
print(f"Will save output to: {output_file}")

try:
    table = pq.read_table(str(input_file))

    batch = table.to_batches()[0]

    signature = LogSignature(
        params=[
            ParamInput(
                name="whirlpool",
                param_type=DynType.FixedArray(DynType.U8, 32),
            ),
            ParamInput(
                name="a_to_b",
                param_type=DynType.Bool,
            ),
            ParamInput(
                name="pre_sqrt_price",
                param_type=DynType.U128,
            ),
            ParamInput(
                name="post_sqrt_price",
                param_type=DynType.U128,
            ),
            ParamInput(
                name="x",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="input_amount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="output_amount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="input_transfer_fee",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="output_transfer_fee",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="lp_fee",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="protocol_fee",
                param_type=DynType.U64,
            ),
        ],
    )

    print("Decoding instruction batch...")
    decoded_batch = svm_decode_logs(signature, batch, True)

    decoded_table = pa.Table.from_batches([decoded_batch])

    print("Saving decoded result...")
    pq.write_table(decoded_table, str(output_file))

    print("Successfully decoded and saved the result!")

except Exception as e:
    print(f"Error during decoding: {e}")
    raise
