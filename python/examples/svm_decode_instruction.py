import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from cherry_core.svm_decode import (
    InstructionSignature,
    ParamInput,
    DynType,
    Field,
    Variant,
)
from cherry_core import svm_decode_instructions

current_dir = Path(__file__).parent
input_file = current_dir / "instructions.parquet"
output_file = current_dir / "decoded.parquet"

print(f"Reading input file: {input_file}")
print(f"Will save output to: {output_file}")

try:
    table = pq.read_table(str(input_file))

    batch = table.to_batches()[0]

    signature = InstructionSignature(
        discriminator="e517cb977ae3ad2a",
        params=[
            ParamInput(
                name="RoutePlan",
                param_type=DynType.Array(
                    DynType.Struct(
                        [
                            Field(
                                name="Swap",
                                element_type=DynType.Enum(
                                    [
                                        Variant("Saber", None),
                                        Variant("SaberAddDecimalsDeposit", None),
                                        Variant("SaberAddDecimalsWithdraw", None),
                                        Variant("TokenSwap", None),
                                        Variant("Sencha", None),
                                        Variant("Step", None),
                                        Variant("Cropper", None),
                                        Variant("Raydium", None),
                                        Variant(
                                            "Crema",
                                            DynType.Struct(
                                                [
                                                    Field("a_to_b", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant("Lifinity", None),
                                        Variant("Mercurial", None),
                                        Variant("Cykura", None),
                                        Variant(
                                            "Serum",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("MarinadeDeposit", None),
                                        Variant("MarinadeUnstake", None),
                                        Variant(
                                            "Aldrin",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "AldrinV2",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "Whirlpool",
                                            DynType.Struct(
                                                [
                                                    Field("a_to_b", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "Invariant",
                                            DynType.Struct(
                                                [
                                                    Field("x_to_y", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant("Meteora", None),
                                        Variant("GooseFX", None),
                                        Variant(
                                            "DeltaFi",
                                            DynType.Struct(
                                                [
                                                    Field("stable", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant("Balansol", None),
                                        Variant(
                                            "MarcoPolo",
                                            DynType.Struct(
                                                [
                                                    Field("x_to_y", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "Dradex",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("LifinityV2", None),
                                        Variant("RaydiumClmm", None),
                                        Variant(
                                            "Openbook",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "Phoenix",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "Symmetry",
                                            DynType.Struct(
                                                [
                                                    Field("from_token_id", DynType.U64),
                                                    Field("to_token_id", DynType.U64),
                                                ]
                                            ),
                                        ),
                                        Variant("TokenSwapV2", None),
                                        Variant(
                                            "HeliumTreasuryManagementRedeemV0", None
                                        ),
                                        Variant("StakeDexStakeWrappedSol", None),
                                        Variant(
                                            "StakeDexSwapViaStake",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "bridge_stake_seed", DynType.U32
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("GooseFXV2", None),
                                        Variant("Perps", None),
                                        Variant("PerpsAddLiquidity", None),
                                        Variant("PerpsRemoveLiquidity", None),
                                        Variant("MeteoraDlmm", None),
                                        Variant(
                                            "OpenBookV2",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("RaydiumClmmV2", None),
                                        Variant(
                                            "StakeDexPrefundWithdrawStakeAndDepositStake",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "bridge_stake_seed", DynType.U32
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "Clone",
                                            DynType.Struct(
                                                [
                                                    Field("pool_index", DynType.U8),
                                                    Field(
                                                        "quantity_is_input",
                                                        DynType.Bool,
                                                    ),
                                                    Field(
                                                        "quantity_is_collateral",
                                                        DynType.Bool,
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "SanctumS",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "src_lst_value_calc_accs",
                                                        DynType.U8,
                                                    ),
                                                    Field(
                                                        "dst_lst_value_calc_accs",
                                                        DynType.U8,
                                                    ),
                                                    Field("src_lst_index", DynType.U32),
                                                    Field("dst_lst_index", DynType.U32),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "SanctumSAddLiquidity",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "lst_value_calc_accs",
                                                        DynType.U8,
                                                    ),
                                                    Field("lst_index", DynType.U32),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "SanctumSRemoveLiquidity",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "lst_value_calc_accs",
                                                        DynType.U8,
                                                    ),
                                                    Field("lst_index", DynType.U32),
                                                ]
                                            ),
                                        ),
                                        Variant("RaydiumCP", None),
                                        Variant(
                                            "WhirlpoolSwapV2",
                                            DynType.Struct(
                                                [
                                                    Field("a_to_b", DynType.Bool),
                                                    Field(
                                                        "remaining_accounts_info",
                                                        DynType.Struct(
                                                            [
                                                                Field(
                                                                    "slices",
                                                                    DynType.Array(
                                                                        DynType.Struct(
                                                                            [
                                                                                Field(
                                                                                    "remaining_accounts_slice",
                                                                                    DynType.Struct(
                                                                                        [
                                                                                            Field(
                                                                                                "accounts_type",
                                                                                                DynType.U8,
                                                                                            ),
                                                                                            Field(
                                                                                                "length",
                                                                                                DynType.U8,
                                                                                            ),
                                                                                        ]
                                                                                    ),
                                                                                )
                                                                            ]
                                                                        )
                                                                    ),
                                                                ),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("OneIntro", None),
                                        Variant("PumpdotfunWrappedBuy", None),
                                        Variant("PumpdotfunWrappedSell", None),
                                        Variant("PerpsV2", None),
                                        Variant("PerpsV2AddLiquidity", None),
                                        Variant("PerpsV2RemoveLiquidity", None),
                                        Variant("MoonshotWrappedBuy", None),
                                        Variant("MoonshotWrappedSell", None),
                                        Variant("StabbleStableSwap", None),
                                        Variant("StabbleWeightedSwap", None),
                                        Variant(
                                            "Obric",
                                            DynType.Struct(
                                                [
                                                    Field("x_to_y", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant("FoxBuyFromEstimatedCost", None),
                                        Variant(
                                            "FoxClaimPartial",
                                            DynType.Struct(
                                                [
                                                    Field("is_y", DynType.Bool),
                                                ]
                                            ),
                                        ),
                                        Variant(
                                            "SolFi",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "is_quote_to_base", DynType.Bool
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("SolayerDelegateNoInit", None),
                                        Variant("SolayerUndelegateNoInit", None),
                                        Variant(
                                            "TokenMill",
                                            DynType.Struct(
                                                [
                                                    Field(
                                                        "side",
                                                        DynType.Enum(
                                                            [
                                                                Variant("Bid", None),
                                                                Variant("Ask", None),
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Variant("DaosFunBuy", None),
                                        Variant("DaosFunSell", None),
                                        Variant("ZeroFi", None),
                                        Variant("StakeDexWithdrawWrappedSol", None),
                                        Variant("VirtualsBuy", None),
                                        Variant("VirtualsSell", None),
                                        Variant(
                                            "Peren",
                                            DynType.Struct(
                                                [
                                                    Field("in_index", DynType.U8),
                                                    Field("out_index", DynType.U8),
                                                ]
                                            ),
                                        ),
                                        Variant("PumpdotfunAmmBuy", None),
                                        Variant("PumpdotfunAmmSell", None),
                                        Variant("Gamma", None),
                                    ]
                                ),
                            ),
                            Field("Percent", DynType.U8),
                            Field("InputIndex", DynType.U8),
                            Field("OutputIndex", DynType.U8),
                        ]
                    )
                ),
            ),
            ParamInput(
                name="InAmount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="QuotedOutAmount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="SlippageBps",
                param_type=DynType.U16,
            ),
            ParamInput(
                name="PlatformFeeBps",
                param_type=DynType.U8,
            ),
        ],
        accounts_names=[
            "TokenProgram",
            "UserTransferAuthority",
            "UserSourceTokenAccount",
            "UserDestinationTokenAccount",
            "DestinationTokenAccount",
            "PlatformFeeAccount",
            "EventAuthority",
            "Program",
        ],
    )

    print("Decoding instruction batch...")
    decoded_batch = svm_decode_instructions(signature, batch, True)

    decoded_table = pa.Table.from_batches([decoded_batch])

    print("Saving decoded result...")
    pq.write_table(decoded_table, str(output_file))

    print("Successfully decoded and saved the result!")

except Exception as e:
    print(f"Error during decoding: {e}")
    raise
