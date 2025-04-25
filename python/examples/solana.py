from cherry_core import ingest
import asyncio


async def run(provider: ingest.ProviderConfig, query: ingest.Query):
    stream = ingest.start_stream(provider, query)

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res)

        print(res["blocks"].column("slot"))
        print(res["instructions"].column("data"))


query = ingest.Query(
    kind=ingest.QueryKind.SVM,
    params=ingest.svm.Query(
        from_block=332557468,
        to_block=332557469,
        include_all_blocks=True,
        fields=ingest.svm.Fields(
            block=ingest.svm.BlockFields(
                slot=True,
                hash=True,
            ),
            instruction=ingest.svm.InstructionFields(
                program_id=True,
                data=True,
            ),
        ),
        instructions=[
            ingest.svm.InstructionRequest(
                program_id=["11111111111111111111111111111111"],
                discriminator=[bytes([2, 0, 0, 0, 1, 0, 0, 0])],
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
