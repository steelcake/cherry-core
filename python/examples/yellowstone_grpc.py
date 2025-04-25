from cherry_core import ingest, base58_encode
import asyncio
import polars
from typing import cast
import pyarrow as pa
import os


async def run(provider: ingest.ProviderConfig, query: ingest.Query):
    stream = ingest.start_stream(provider, query)

    while True:
        res = await stream.next()
        if res is None:
            break

        transactions = cast(polars.DataFrame, polars.from_arrow(res["transactions"]))
        token_balances = cast(
            polars.DataFrame, polars.from_arrow(res["token_balances"])
        )

        token_balances = token_balances.join(
            transactions, ["block_slot", "transaction_index"]
        )

        for batch in token_balances.to_arrow().to_batches():
            new_batch = batch
            for i, col in enumerate(new_batch.columns):
                if pa.types.is_large_binary(col.type):
                    new_batch = new_batch.set_column(
                        i, new_batch.column_names[i], col.cast(target_type=pa.binary())
                    )

            new_batch = base58_encode(new_batch)

            print(new_batch)


query = ingest.Query(
    kind=ingest.QueryKind.SVM,
    params=ingest.svm.Query(
        from_block=317617480,
        token_balances=[
            ingest.svm.TokenBalanceRequest(
                include_transactions=True,
            )
        ],
        fields=ingest.svm.Fields(
            token_balance=ingest.svm.TokenBalanceFields(
                block_slot=True,
                transaction_index=True,
                post_owner=True,
                pre_owner=True,
                post_amount=True,
                pre_amount=True,
                post_mint=True,
                account=True,
            ),
            transaction=ingest.svm.TransactionFields(
                block_slot=True,
                transaction_index=True,
                signature=True,
                err=True,
            ),
        ),
    ),
)

asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.YELLOWSTONE_GRPC,
            url=os.environ.get("YELLOWSTONE_GRPC_URL"),
            bearer_token=os.environ.get("YELLOWSTONE_GRPC_TOKEN"),
        ),
        query=query,
    )
)
