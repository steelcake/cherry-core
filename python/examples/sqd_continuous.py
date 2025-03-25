from cherry_core import ingest
import asyncio


async def run(provider: ingest.ProviderConfig, query: ingest.Query):
    stream = ingest.start_stream(provider, query)

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res["blocks"].column("number"))


query = ingest.Query(
    kind=ingest.QueryKind.EVM,
    params=ingest.evm.Query(
        from_block=21930160,
        include_all_blocks=True,
        fields=ingest.evm.Fields(
            block=ingest.evm.BlockFields(
                number=True,
            ),
        ),
    ),
)

asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            url="https://portal.sqd.dev/datasets/ethereum-mainnet",
            stop_on_head=False,  # default is False as well
            head_poll_interval_millis=1000,  # default is 1000
        ),
        query=query,
    )
)
