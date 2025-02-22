from cherry_core import ingest
import asyncio

async def run(provider: ingest.ProviderConfig):
    stream = ingest.start_stream(provider)

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res["blocks"].column("slot"))
        print(res["blocks"].column("hash"))

query = ingest.Query(
    kind=ingest.QueryKind.SVM,
    params=ingest.svm.Query(
        from_block=300123123,
        to_block=300123143,
        include_all_blocks=True,
        fields=ingest.svm.Fields(
            block=ingest.svm.BlockFields(
                slot=True,
                hash=True,
            )
        )
    )
)

print("running with sqd")
asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            query=query,
            url="https://portal.sqd.dev/datasets/solana-mainnet",
        )
    )
)
