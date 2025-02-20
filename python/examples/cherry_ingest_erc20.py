import cherry_core
from cherry_core import ingest
import asyncio

signature = "Transfer(address indexed from, address indexed to, uint256 amount)"
topic0 = cherry_core.evm_signature_to_topic0(signature)
contract_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

async def run(provider: ingest.ProviderConfig):
    stream = ingest.start_stream(ingest.StreamConfig(
        format=ingest.Format.EVM,
        provider=provider,
        query=ingest.EvmQuery(
            from_block=20123123,
            to_block=20123223,
            logs=[ingest.LogRequest(
                address=[contract_address],
                event_signatures=[signature],
                # topic0=[topic0], same effect as above
            )],
            fields=ingest.Fields(
                block=ingest.BlockFields(
                    number=True,
                ),
                log=ingest.LogFields(
                    data=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                )
            )
        ),
    ))

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res["blocks"].column("number"))

        decoded = cherry_core.evm_decode_events(signature, res["logs"])
        print(decoded)

print("running with sqd")
asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            url="https://portal.sqd.dev/datasets/ethereum-mainnet",
        )
    )
)

print("running with hypersync")
asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.HYPERSYNC,
            url="https://eth.hypersync.xyz",
        )
    )
)
