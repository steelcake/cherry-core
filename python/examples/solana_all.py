from cherry_core import ingest
import pyarrow.parquet as pq
import pyarrow as pa
import asyncio


async def run(provider: ingest.ProviderConfig, query: ingest.Query):
    stream = ingest.start_stream(provider, query)

    while True:
        res = await stream.next()
        if res is None:
            break

        print(res.keys())

        for key in res.keys():
            table = res[key]
            table = pa.Table.from_batches([table])
            pq.write_table(table, f"data/{key}_{start_block}_{end_block}.parquet")


start_block = 317617480
end_block = 317617580
url = "https://portal.sqd.dev/datasets/solana-mainnet"

query = ingest.Query(
    kind=ingest.QueryKind.SVM,
    params=ingest.svm.Query(
        from_block=start_block,
        to_block=end_block,
        include_all_blocks=True,
        fields=ingest.svm.Fields(
            block=ingest.svm.BlockFields(
                slot=True,
                hash=True,
                parent_slot=True,
                parent_hash=True,
                height=True,
                timestamp=True,
            ),
            instruction=ingest.svm.InstructionFields(
                block_slot=True,
                block_hash=True,
                transaction_index=True,
                instruction_address=True,
                program_id=True,
                a0=True,
                a1=True,
                a2=True,
                a3=True,
                a4=True,
                a5=True,
                a6=True,
                a7=True,
                a8=True,
                a9=True,
                rest_of_accounts=True,
                data=True,
                d1=True,
                d2=True,
                d4=True,
                d8=True,
                error=True,
                compute_units_consumed=True,
                is_committed=True,
                has_dropped_log_messages=True,
            ),
            transaction=ingest.svm.TransactionFields(
                block_slot=True,
                block_hash=True,
                transaction_index=True,
                signature=True,
                version=True,
                account_keys=True,
                address_table_lookups=True,
                num_readonly_signed_accounts=True,
                num_readonly_unsigned_accounts=True,
                num_required_signatures=True,
                # recent_blockhash=True,
                signatures=True,
                err=True,
                fee=True,
                compute_units_consumed=True,
                loaded_readonly_addresses=True,
                loaded_writable_addresses=True,
                fee_payer=True,
                has_dropped_log_messages=True,
            ),
            log=ingest.svm.LogFields(
                block_slot=True,
                block_hash=True,
                transaction_index=True,
                log_index=True,
                instruction_address=True,
                program_id=True,
                kind=True,
                message=True,
            ),
            balance=ingest.svm.BalanceFields(
                block_slot=True,
                block_hash=True,
                transaction_index=True,
                account=True,
                pre=True,
                post=True,
            ),
            token_balance=ingest.svm.TokenBalanceFields(
                block_slot=True,
                block_hash=True,
                transaction_index=True,
                account=True,
                pre_mint=True,
                post_mint=True,
                pre_decimals=True,
                post_decimals=True,
                pre_program_id=True,
                post_program_id=True,
                pre_owner=True,
                post_owner=True,
                pre_amount=True,
                post_amount=True,
            ),
            reward=ingest.svm.RewardFields(
                block_slot=True,
                block_hash=True,
                pubkey=True,
                lamports=True,
                post_balance=True,
                reward_type=True,
                commission=True,
            ),
        ),
        instructions=[ingest.svm.InstructionRequest()],
        transactions=[ingest.svm.TransactionRequest()],
        logs=[ingest.svm.LogRequest()],
        balances=[ingest.svm.BalanceRequest()],
        token_balances=[ingest.svm.TokenBalanceRequest()],
        rewards=[ingest.svm.RewardRequest()],
    ),
)

print("running with sqd")
asyncio.run(
    run(
        ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            url=url,
        ),
        query=query,
    )
)
