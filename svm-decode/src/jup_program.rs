use anchor_lang::{AnchorDeserialize, prelude::{Pubkey, borsh}};
use anyhow::Result;

// Program ID
pub const PROGRAM_ID: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

// Instruction discriminators
pub const CLAIM_IX_DISCM: [u8; 8] = [62, 198, 214, 193, 213, 159, 108, 210];
pub const CLAIM_TOKEN_IX_DISCM: [u8; 8] = [116, 206, 27, 191, 166, 19, 0, 73];
pub const CLOSE_TOKEN_IX_DISCM: [u8; 8] = [26, 74, 236, 151, 104, 64, 183, 249];
pub const CREATE_OPEN_ORDERS_IX_DISCM: [u8; 8] = [229, 194, 212, 172, 8, 10, 134, 147];
pub const CREATE_PROGRAM_OPEN_ORDERS_IX_DISCM: [u8; 8] = [28, 226, 32, 148, 188, 136, 113, 171];
pub const CREATE_TOKEN_LEDGER_IX_DISCM: [u8; 8] = [232, 242, 197, 253, 240, 143, 129, 52];
pub const CREATE_TOKEN_ACCOUNT_IX_DISCM: [u8; 8] = [147, 241, 123, 100, 244, 132, 174, 118];
pub const EXACT_OUT_ROUTE_IX_DISCM: [u8; 8] = [208, 51, 239, 151, 123, 43, 237, 92];
pub const ROUTE_IX_DISCM: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];
pub const ROUTE_WITH_TOKEN_LEDGER_IX_DISCM: [u8; 8] = [150, 86, 71, 116, 167, 93, 14, 104];
pub const SET_TOKEN_LEDGER_IX_DISCM: [u8; 8] = [228, 85, 185, 112, 78, 79, 77, 2];
pub const SHARED_ACCOUNTS_EXACT_OUT_ROUTE_IX_DISCM: [u8; 8] = [176, 209, 105, 168, 154, 125, 69, 62];
pub const SHARED_ACCOUNTS_ROUTE_IX_DISCM: [u8; 8] = [193, 32, 155, 51, 65, 214, 156, 129];
pub const SHARED_ACCOUNTS_ROUTE_WITH_TOKEN_LEDGER_IX_DISCM: [u8; 8] = [230, 121, 143, 80, 119, 159, 106, 170];

// Account discriminators
pub const TOKEN_LEDGER_ACCOUNT_DISCM: [u8; 8] = [156, 247, 9, 188, 54, 108, 85, 77];

// Event discriminators
pub const FEE_EVENT_DISCM: [u8; 8] = [73, 79, 78, 127, 184, 213, 13, 220];
pub const SWAP_EVENT_DISCM: [u8; 8] = [64, 198, 205, 232, 38, 8, 113, 226];

// CPI Log Discriminator, it's not in IDL, but it's absulutely required to decode the instruction
// https://stackoverflow.com/questions/79400728/how-to-correctly-decode-a-jupiter-swap-event-on-solana
pub const CPI_LOG_DISCRIMINATOR: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

#[derive(Debug, AnchorDeserialize, Clone)]
pub enum JupInstruction {
    // Instructions
    Claim {
        id: u8,
    },
    ClaimToken{
        id: u8,
    },
    CloseToken {
        id: u8,
        burn_all: bool,
    },
    CreateOpenOrders,
    CreateProgramOpenOrders {
        id: u8,
    },
    CreateTokenLedger,
    CreateTokenAccount {
        bump: u8,
    },
    ExactOutRoute {
        route_plan: RoutePlanStep,
        out_amount: u64,
        quoted_in_amount: u64,
        slippage_bps: u16,
        platform_fee_bps: u16,
    },
    Route {
        route_plan: Vec<RoutePlanStep>,
        in_amount: u64,
        quoted_out_amount: u64,
        slippage_bps: u16,
        platform_fee_bps: u8,
    },
    RouteWithTokenLedger {
        route_plan: RoutePlanStep,
        quoted_out_amount: u64,
        slippage_bps: u16,
        platform_fee_bps: u8,
    },
    SetTokenLedger,
    SharedAccountsExactOutRoute {
        id: u8,
        route_plan: RoutePlanStep,
        out_amount: u64,
        quoted_in_amount: u64,
        slippage_bps: u16,
        platform_fee_bps: u8,
    },
    SharedAccountsRoute {
        id: u8,
        route_plan: RoutePlanStep,
        in_amount: u64,
        quoted_out_amount: u64,
        slippage_bps: u16,
        platform_fee_bps: u8,
    },
    SharedAccountsRouteWithTokenLedger {
        id: u8,
        route_plan: RoutePlanStep,
        quoted_out_amount: u64,
        slippage_bps: u16,
        platform_fee_bps: u8,
    },
    // Events
    FeeEvent {
        account: Pubkey,
        mint: Pubkey,
        amount: u64,
    },
    SwapEvent {
        amm: Pubkey,
        input_mint: Pubkey,
        input_amount: u64,
        output_mint: Pubkey,
        output_amount: u64,
    },
    // Accounts
    TokenLedger {
        token_account: Pubkey,
        amount: u64,
    },
}

impl JupInstruction {
    pub fn unpack(data: &mut &[u8]) -> Result<JupInstruction> {
        let mut disc = [0u8; 8];
        disc.copy_from_slice(&data[..8]);
        let ix_data = &mut &data[8..];
        println!("Discriminator: {:?}", disc);
        match_discriminator(disc, ix_data)
    }    
}


fn match_discriminator(disc: [u8; 8], ix_data: &mut &[u8]) -> Result<JupInstruction> {
    match disc {
        CLAIM_IX_DISCM => decode_instruction::<Claim>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into Claim: {}", e)),
        CLAIM_TOKEN_IX_DISCM => decode_instruction::<ClaimToken>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into ClaimToken: {}", e)),
        CLOSE_TOKEN_IX_DISCM => decode_instruction::<CloseToken>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into CloseToken: {}", e)),
        CREATE_OPEN_ORDERS_IX_DISCM => decode_instruction::<CreateOpenOrders>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into CreateOpenOrders: {}", e)),
        CREATE_PROGRAM_OPEN_ORDERS_IX_DISCM => decode_instruction::<CreateProgramOpenOrders>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into CreateProgramOpenOrders: {}", e)),
        CREATE_TOKEN_LEDGER_IX_DISCM => decode_instruction::<CreateTokenLedger>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into CreateTokenLedger: {}", e)),
        CREATE_TOKEN_ACCOUNT_IX_DISCM => decode_instruction::<CreateTokenAccount>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into CreateTokenAccount: {}", e)),
        EXACT_OUT_ROUTE_IX_DISCM => decode_instruction::<ExactOutRoute>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into ExactOutRoute: {}", e)),
        ROUTE_IX_DISCM => decode_instruction::<Route>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into Route: {}", e)),
        ROUTE_WITH_TOKEN_LEDGER_IX_DISCM => decode_instruction::<RouteWithTokenLedger>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into RouteWithTokenLedger: {}", e)),
        SET_TOKEN_LEDGER_IX_DISCM => decode_instruction::<SetTokenLedger>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into SetTokenLedger: {}", e)),
        SHARED_ACCOUNTS_EXACT_OUT_ROUTE_IX_DISCM => decode_instruction::<SharedAccountsExactOutRoute>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into SharedAccountsExactOutRoute: {}", e)),
        SHARED_ACCOUNTS_ROUTE_IX_DISCM => decode_instruction::<SharedAccountsRoute>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into SharedAccountsRoute: {}", e)),
        SHARED_ACCOUNTS_ROUTE_WITH_TOKEN_LEDGER_IX_DISCM => decode_instruction::<SharedAccountsRouteWithTokenLedger>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into SharedAccountsRouteWithTokenLedger: {}", e)),
        FEE_EVENT_DISCM => decode_instruction::<FeeEvent>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into FeeEvent: {}", e)),
        SWAP_EVENT_DISCM => decode_instruction::<SwapEvent>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into SwapEvent: {}", e)),
        TOKEN_LEDGER_ACCOUNT_DISCM => decode_instruction::<TokenLedger>(ix_data)
            .map(|ix| ix.into_jup_instruction())
            .map_err(|e| anyhow::anyhow!("Failed to decode instruction into TokenLedger: {}", e)),
        CPI_LOG_DISCRIMINATOR => {
            let mut sec_disc = [0u8; 8];
            sec_disc.copy_from_slice(&ix_data[..8]);
            let ix_data = &mut &ix_data[8..];
            println!("Second Discriminator: {:?}", sec_disc);
            match_discriminator(sec_disc, ix_data)
        }
        _ => Err(anyhow::anyhow!("No handler found for discriminator")),
    }
}

fn decode_instruction<T: anchor_lang::AnchorDeserialize>(
    slice: &mut &[u8],
) -> Result<T, anchor_lang::error::ErrorCode> {
    let instruction: T = anchor_lang::AnchorDeserialize::deserialize(slice)
        .map_err(|_| anchor_lang::error::ErrorCode::InstructionDidNotDeserialize)?;
    Ok(instruction)
}

trait IntoJupInstruction {
    fn into_jup_instruction(self) -> JupInstruction;
}

// Instruction argument structs
#[derive(Debug, AnchorDeserialize)]
pub struct Claim {
    pub id: u8,
}

impl IntoJupInstruction for Claim {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::Claim { id: self.id }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct ClaimToken {
    pub id: u8,
}

impl IntoJupInstruction for ClaimToken {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::ClaimToken { id: self.id }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct CloseToken {
    pub id: u8,
    pub burn_all: bool,
}

impl IntoJupInstruction for CloseToken {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::CloseToken { id: self.id, burn_all: self.burn_all }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct CreateOpenOrders;

impl IntoJupInstruction for CreateOpenOrders {
    fn into_jup_instruction(self)    -> JupInstruction {
        JupInstruction::CreateOpenOrders
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct CreateProgramOpenOrders {
    pub id: u8,
}

impl IntoJupInstruction for CreateProgramOpenOrders {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::CreateProgramOpenOrders { id: self.id }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct CreateTokenLedger;

impl IntoJupInstruction for CreateTokenLedger {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::CreateTokenLedger
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct CreateTokenAccount {
    pub bump: u8,
}

impl IntoJupInstruction for CreateTokenAccount {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::CreateTokenAccount { bump: self.bump }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct ExactOutRoute {
    pub route_plan: RoutePlanStep,
    pub out_amount: u64,
    pub quoted_in_amount: u64,
    pub slippage_bps: u16,
    pub platform_fee_bps: u16,
}

impl IntoJupInstruction for ExactOutRoute {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::ExactOutRoute { route_plan: self.route_plan, out_amount: self.out_amount, quoted_in_amount: self.quoted_in_amount, slippage_bps: self.slippage_bps, platform_fee_bps: self.platform_fee_bps }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct Route {
    pub route_plan: Vec<RoutePlanStep>,
    pub in_amount: u64,
    pub quoted_out_amount: u64,
    pub slippage_bps: u16,
    pub platform_fee_bps: u8,
}

impl IntoJupInstruction for Route {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::Route { route_plan: self.route_plan, in_amount: self.in_amount, quoted_out_amount: self.quoted_out_amount, slippage_bps: self.slippage_bps, platform_fee_bps: self.platform_fee_bps }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct RouteWithTokenLedger {
    pub route_plan: RoutePlanStep,
    pub quoted_out_amount: u64,
    pub slippage_bps: u16,
    pub platform_fee_bps: u8,
}

impl IntoJupInstruction for RouteWithTokenLedger {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::RouteWithTokenLedger { route_plan: self.route_plan, quoted_out_amount: self.quoted_out_amount, slippage_bps: self.slippage_bps, platform_fee_bps: self.platform_fee_bps }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct SetTokenLedger;

impl IntoJupInstruction for SetTokenLedger {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::SetTokenLedger
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct SharedAccountsExactOutRoute {
    pub id: u8,
    pub route_plan: RoutePlanStep,
    pub out_amount: u64,
    pub quoted_in_amount: u64,
    pub slippage_bps: u16,
    pub platform_fee_bps: u8,
}

impl IntoJupInstruction for SharedAccountsExactOutRoute {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::SharedAccountsExactOutRoute { id: self.id, route_plan: self.route_plan, out_amount: self.out_amount, quoted_in_amount: self.quoted_in_amount, slippage_bps: self.slippage_bps, platform_fee_bps: self.platform_fee_bps }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct SharedAccountsRoute {
    pub id: u8,
    pub route_plan: RoutePlanStep,
    pub in_amount: u64,
    pub quoted_out_amount: u64,
    pub slippage_bps: u16,
    pub platform_fee_bps: u8,
}

impl IntoJupInstruction for SharedAccountsRoute {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::SharedAccountsRoute { id: self.id, route_plan: self.route_plan, in_amount: self.in_amount, quoted_out_amount: self.quoted_out_amount, slippage_bps: self.slippage_bps, platform_fee_bps: self.platform_fee_bps }
    }
}

#[derive(Debug, AnchorDeserialize)]
pub struct SharedAccountsRouteWithTokenLedger {
    pub id: u8,
    pub route_plan: RoutePlanStep,
    pub quoted_out_amount: u64,
    pub slippage_bps: u16,
    pub platform_fee_bps: u8,
}

impl IntoJupInstruction for SharedAccountsRouteWithTokenLedger {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::SharedAccountsRouteWithTokenLedger { id: self.id, route_plan: self.route_plan, quoted_out_amount: self.quoted_out_amount, slippage_bps: self.slippage_bps, platform_fee_bps: self.platform_fee_bps }
    }
}

// Event definitions
#[derive(Debug, AnchorDeserialize)]
pub struct SwapEvent {
    pub amm: Pubkey,
    pub input_mint: Pubkey,
    pub input_amount: u64,
    pub output_mint: Pubkey,
    pub output_amount: u64,
}

impl IntoJupInstruction for SwapEvent {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::SwapEvent { amm: self.amm, input_mint: self.input_mint, input_amount: self.input_amount, output_mint: self.output_mint, output_amount: self.output_amount }
    }
}

// Account definitions
#[derive(Debug, AnchorDeserialize)]
pub struct TokenLedger {
    pub token_account: Pubkey,
    pub amount: u64,
}

impl IntoJupInstruction for TokenLedger {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::TokenLedger { token_account: self.token_account, amount: self.amount }
    }
}

// Type definitions
#[derive(Debug, AnchorDeserialize, Clone)]
pub enum AccountsType {
    TransferHookA,
    TransferHookB,
    TransferHookReward,
    TransferHookInput,
    TransferHookIntermediate,
    TransferHookOutput,
    SupplementalTickArrays,
    SupplementalTickArraysOne,
    SupplementalTickArraysTwo,
}

#[derive(Debug, AnchorDeserialize)]
pub struct FeeEvent {
    pub account: Pubkey,
    pub mint: Pubkey,
    pub amount: u64,
}

impl IntoJupInstruction for FeeEvent {
    fn into_jup_instruction(self) -> JupInstruction {
        JupInstruction::FeeEvent {
            account: self.account,
            mint: self.mint,
            amount: self.amount,
        }
    }
}

#[derive(Debug, AnchorDeserialize, Clone)]
pub struct RemainingAccountsInfo {
    pub slices: Vec<RemainingAccountsSlice>,
}

#[derive(Debug, AnchorDeserialize, Clone)]
pub struct RemainingAccountsSlice {
    pub accounts_type: AccountsType,
    pub length: u8,
}

#[derive(Debug, AnchorDeserialize, Clone)]
pub struct RoutePlanStep {
    pub swap: Swap,
    pub percent: u8,
    pub input_index: u8,
    pub output_index: u8,
}

#[derive(Debug, AnchorDeserialize, Clone)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, AnchorDeserialize, Clone)]
pub enum Swap {
    Saber,
    SaberAddDecimalsDeposit,
    SaberAddDecimalsWithdraw,
    TokenSwap,
    Sencha,
    Step,
    Cropper,
    Raydium,
    Crema { a_to_b: bool },
    Lifinity,
    Mercurial,
    Cykura,
    Serum { side: Side },
    MarinadeDeposit,
    MarinadeUnstake,
    Aldrin { side: Side },
    AldrinV2 { side: Side },
    Whirlpool { a_to_b: bool },
    Invariant { x_to_y: bool },
    Meteora,
    GooseFX,
    DeltaFi { stable: bool },
    Balansol,
    MarcoPolo { x_to_y: bool },
    Dradex { side: Side },
    LifinityV2,
    RaydiumClmm,
    Openbook { side: Side },
    Phoenix { side: Side },
    Symmetry { from_token_id: u64, to_token_id: u64 },
    TokenSwapV2,
    HeliumTreasuryManagementRedeemV0,
    StakeDexStakeWrappedSol,
    StakeDexSwapViaStake { bridge_stake_seed: u32 },
    GooseFXV2,
    Perps,
    PerpsAddLiquidity,
    PerpsRemoveLiquidity,
    MeteoraDlmm,
    OpenBookV2 { side: Side },
    RaydiumClmmV2,
    StakeDexPrefundWithdrawStakeAndDepositStake { bridge_stake_seed: u32 },
    Clone { pool_index: u8, quantity_is_input: bool, quantity_is_collateral: bool },
    SanctumS { src_lst_value_calc_accs: u8, dst_lst_value_calc_accs: u8, src_lst_index: u32, dst_lst_index: u32 },
    SanctumSAddLiquidity { lst_value_calc_accs: u8, lst_index: u32 },
    SanctumSRemoveLiquidity { lst_value_calc_accs: u8, lst_index: u32 },
    RaydiumCP,
    WhirlpoolSwapV2 { a_to_b: bool, remaining_accounts_info: Option<RemainingAccountsInfo> },
    OneIntro,
    PumpdotfunWrappedBuy,
    PumpdotfunWrappedSell,
    PerpsV2,
    PerpsV2AddLiquidity,
    PerpsV2RemoveLiquidity,
    MoonshotWrappedBuy,
    MoonshotWrappedSell,
    StabbleStableSwap,
    StabbleWeightedSwap,
    Obric { x_to_y: bool },
    FoxBuyFromEstimatedCost,
    FoxClaimPartial { is_y: bool },
    SolFi { is_quote_to_base: bool },
    SolayerDelegateNoInit,
    SolayerUndelegateNoInit,
    TokenMill { side: Side },
    DaosFunBuy,
    DaosFunSell,
    ZeroFi,
    StakeDexWithdrawWrappedSol,
    VirtualsBuy,
    VirtualsSell,
    Perena { in_index: u8, out_index: u8 },
    PumpdotfunAmmBuy,
    PumpdotfunAmmSell,
    Gamma,
}
