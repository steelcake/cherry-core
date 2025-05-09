from cherry_core import ingest, base58_encode_bytes
import asyncio


async def run(provider: ingest.ProviderConfig, query: ingest.Query):
    stream = ingest.start_stream(provider, query)

    while True:
        res = await stream.next()
        if res is None:
            break

        # print(res)

        print(res["blocks"].column("slot"))

        for x in res["transactions"].column("signature"):
            print(base58_encode_bytes(x.as_py()))

        print(res["instructions"].column("instruction_address"))
        print(res["instructions"].column("transaction_index"))


query = ingest.Query(
    kind=ingest.QueryKind.SVM,
    params=ingest.svm.Query(
        from_block=332557668,
        to_block=332557668,
        include_all_blocks=True,
        fields=ingest.svm.Fields(
            block=ingest.svm.BlockFields(
                slot=True,
                hash=True,
            ),
            instruction=ingest.svm.InstructionFields(
                program_id=True,
                data=True,
                instruction_address=True,
                transaction_index=True,
            ),
            transaction=ingest.svm.TransactionFields(
                signature=True,
                transaction_index=True,
            ),
        ),
        instructions=[
            ingest.svm.InstructionRequest(
                program_id=["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                discriminator=["0xf8c6"],
                a1=["EV5Xoy9TQc4zXtqRHpjDefrxsQTi4Lm12KAE8axyBsdp"],
                # discriminator=[bytes([2, 0, 0, 0, 1, 0, 0, 0])],
                include_inner_instructions=True,
                include_transactions=True,
                is_committed=True,
            )
        ],
    ),
)

print("running with sqd")
asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            url="https://portal.sqd.dev/datasets/solana-beta",
        ),
        query=query,
    )
)
