from cherry_core import ingest
import asyncio
import argparse


async def run(provider: ingest.ProviderConfig, query: ingest.Query):
    stream = ingest.start_stream(provider, query)

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res["transactions"].schema)


async def main(provider_kind: ingest.ProviderKind):
    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=0,
            transactions=[ingest.evm.TransactionRequest()],
            fields=ingest.evm.Fields(
                transaction=ingest.evm.TransactionFields(
                    v=True,
                    r=True,
                    s=True,
                ),
            ),
        ),
    )

    url = None

    if provider_kind == ingest.ProviderKind.SQD:
        url = "https://portal.sqd.dev/datasets/ethereum-mainnet"
    elif provider_kind == ingest.ProviderKind.HYPERSYNC:
        url = "https://eth.hypersync.xyz"

    await run(
        ingest.ProviderConfig(
            kind=provider_kind,
            url=url,
            stop_on_head=False,  # default is False as well
            head_poll_interval_millis=1000,  # default is 1000
        ),
        query=query,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="example")

    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )

    args = parser.parse_args()

    asyncio.run(main(args.provider))
