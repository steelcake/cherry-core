import asyncio

from berry_core import ingest

query = ingest.Query(
    kind=ingest.QueryKind.EVM,
    params=ingest.evm.Query(
        from_block=18_000_000,
        to_block=18_000_004,
        include_all_blocks=True,
        transactions=[ingest.evm.TransactionRequest()],  # ensure txs present
        logs=[ingest.evm.LogRequest()],  # ensure logs present
        fields=ingest.evm.Fields(
            block=ingest.evm.BlockFields(number=True),
            transaction=ingest.evm.TransactionFields(hash=True),
            log=ingest.evm.LogFields(address=True),
        ),
    ),
)


async def count(provider: ingest.ProviderConfig):
    stream = ingest.start_stream(provider, query)
    n_blocks = n_txs = n_logs = 0
    while True:
        res = await stream.next()
        if res is None:
            break

        n_blocks += res["blocks"].num_rows
        n_txs += res["transactions"].num_rows
        n_logs += res["logs"].num_rows
    return n_blocks, n_txs, n_logs


async def main():
    n_blocks_rpc, n_txs_rpc, n_logs_rpc = await count(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.RPC, url="https://eth.rpc.hypersync.xyz"
        )
    )
    n_blocks_hs, n_txs_hs, n_logs_hs = await count(
        ingest.ProviderConfig(kind=ingest.ProviderKind.HYPERSYNC)
    )

    assert n_blocks_rpc == n_blocks_hs == 5
    assert n_txs_rpc == n_txs_hs
    assert n_logs_rpc == n_logs_hs


if __name__ == "__main__":
    asyncio.run(main())
