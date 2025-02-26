import cherry_core
from cherry_core import ingest
import asyncio

signature = "Transfer(address indexed from, address indexed to, uint256 amount)"
topic0 = cherry_core.evm_signature_to_topic0(signature)
contract_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

async def run(provider: ingest.ProviderConfig):
    stream = ingest.start_stream(provider)

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res["blocks"].column("number"))

        decoded = cherry_core.evm_decode_events(signature, res["logs"])
        print(decoded)

query = ingest.Query(
    kind=ingest.QueryKind.EVM,
    params=ingest.evm.Query(
        from_block=21930160,
        logs=[ingest.evm.LogRequest(
            address=[contract_address],
            event_signatures=[signature],
            # topic0=[topic0], same effect as above
        )],
        fields=ingest.evm.Fields(
            block=ingest.evm.BlockFields(
                number=True,
            ),
            log=ingest.evm.LogFields(
                data=True,
                topic0=True,
                topic1=True,
                topic2=True,
                topic3=True,
            )
        )
    )
)

asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            query=query,
            url="https://portal.sqd.dev/datasets/ethereum-mainnet",
            stop_on_head=False, # default is False as well
            head_poll_interval_millis=1000, # default is 1000
        )
    )
)

