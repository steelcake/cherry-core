import cherry_core
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType
from cherry_core import decode_instruction_batch

# Define input and output file paths
current_dir = Path(__file__).parent
input_file = current_dir / "transfers.parquet"
output_file = current_dir / "decoded.parquet"

print(f"Reading input file: {input_file}")
print(f"Will save output to: {output_file}")

try:
    # Read the input Parquet file
    table = pq.read_table(str(input_file))
    
    # Convert to RecordBatch
    batch = table.to_batches()[0]
    
    # Create the instruction signature using the new classes
    # For SPL Token Transfer
    
    signature = InstructionSignature(
        discriminator=bytes([3]),  # SPL Token Transfer discriminator
        params=[
            ParamInput(
                name="Amount",
                param_type=DynType.U64
            )
        ],
        accounts_names=[
            "Source",
            "Destination",
            "Authority"
        ]
    )
    
    # Decode the instruction batch
    print("Decoding instruction batch...")
    decoded_batch = decode_instruction_batch(signature, batch, True)
    
    # Convert RecordBatch to Table before writing
    decoded_table = pa.Table.from_batches([decoded_batch])
    
    # Save the decoded result to a new Parquet file
    print("Saving decoded result...")
    pq.write_table(decoded_table, str(output_file))
    
    print("Successfully decoded and saved the result!")
    
except Exception as e:
    print(f"Error during decoding: {e}")
    raise