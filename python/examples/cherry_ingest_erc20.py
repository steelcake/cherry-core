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
        query=ingest.EvmQuery(
            from_block=0,
            to_block=21123123,
            logs=[ingest.LogRequest(

            )],
            fields=ingest.Fields(
                block=ingest.BlockFields(
                    number=True,
                )
            )
        ),
    ))

    while True:
        res = await stream.next()
        if res is None:
            break

        number = res["blocks"].column("number")
        print(number)

asyncio.run(main())
