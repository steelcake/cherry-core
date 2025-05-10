import cherry_core
import polars as pl


def main():
    # Test addresses including invalid and valid ones
    addresses = [
        "Invalid address",
        "0x0000000000000000000000000000000000000000",  # Zero address
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # stETH
    ]

    # Test get_token_metadata
    print("Testing get_token_metadata:")
    token_metadata = cherry_core.get_token_metadata(
        "https://ethereum-rpc.publicnode.com", addresses
    )
    print("Token metadata as list of dictionaries:")
    for metadata in token_metadata:
        print(metadata)
    print("\n")

    # Test get_token_metadata_as_table
    print("Testing get_token_metadata_as_table:")
    token_metadata_table = cherry_core.get_token_metadata_as_table(
        "https://ethereum-rpc.publicnode.com", addresses
    )

    # Convert to pandas DataFrame for better display
    df = pl.from_arrow(token_metadata_table)
    print("Token metadata as table:")
    print(df)


if __name__ == "__main__":
    main()
