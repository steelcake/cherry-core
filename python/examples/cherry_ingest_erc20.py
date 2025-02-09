from cherry_core import ingest
import asyncio

async def main():
    stream = ingest.start_stream(ingest.StreamConfig(
        format=ingest.Format.EVM,
        provider=ingest.Provider(
            kind=ingest.ProviderKind.SQD,
            config=ingest.ProviderConfig(
                url="https://portal.sqd.dev/datasets/ethereum-mainnet"
            ),
        ),
        query=ingest.EvmQuery(),
    ))

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res)

asyncio.run(main())
