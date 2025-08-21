import berry_core
import polars as pl


def main():
    # Test addresses including invalid and valid ones
    pool_addresses = [
        "Invalid address",
        "0xfBB6Eed8e7aa03B138556eeDaF5D271A5E1e43ef",  # cbBTC/USDC on uniswap v3
        "0x31f609019d0CC0b8cC865656142d6FeD69853689",  # POPCAT/WETH on uniswap v2
        "0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d",  # AERO/USDC on Aerodrome
        "0x323b43332F97B1852D8567a08B1E8ed67d25A8d5",  # msETH/WETH on Pancake Swap
    ]

    # Test get_pools_token0_token1
    print("Testing get_pools_token0_token1:")
    pool_tokens = berry_core.get_pools_token0_token1(
        "https://base-rpc.publicnode.com", pool_addresses
    )
    print("Pool tokens as list of dictionaries:")
    for pool in pool_tokens:
        print(pool)
    print("\n")

    # Test get_pools_token0_token1_as_table
    print("Testing get_pools_token0_token1_as_table:")
    pool_tokens_table = berry_core.get_pools_token0_token1_as_table(
        "https://base-rpc.publicnode.com", pool_addresses
    )
    # Convert to polars DataFrame for better display
    df = pl.from_arrow(pool_tokens_table)
    print("Pool tokens as table:")
    print(df)


if __name__ == "__main__":
    main()
