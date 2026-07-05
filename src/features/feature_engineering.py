from pathlib import Path
import pandas as pd


RAW_DATA_PATH = Path("data/raw/paysim_transactions.csv")
PROCESSED_DATA_PATH = Path("data/processed/processed_transactions.csv")


def create_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["origin_prefix"] = df["nameOrig"].str[0]
    df["dest_prefix"] = df["nameDest"].str[0]

    df["is_origin_customer"] = (df["origin_prefix"] == "C").astype(int)
    df["is_dest_customer"] = (df["dest_prefix"] == "C").astype(int)
    df["is_dest_merchant"] = (df["dest_prefix"] == "M").astype(int)

    df["origin_balance_diff"] = df["oldbalanceOrg"] - df["newbalanceOrig"]
    df["destination_balance_diff"] = df["newbalanceDest"] - df["oldbalanceDest"]

    df["origin_balance_error"] = (
        df["oldbalanceOrg"] - df["amount"] - df["newbalanceOrig"]
    )

    df["destination_balance_error"] = (
        df["oldbalanceDest"] + df["amount"] - df["newbalanceDest"]
    )

    df["abs_origin_balance_error"] = df["origin_balance_error"].abs()
    df["abs_destination_balance_error"] = df["destination_balance_error"].abs()

    df["amount_to_oldbalanceOrg_ratio"] = df["amount"] / (df["oldbalanceOrg"] + 1)
    df["amount_to_oldbalanceDest_ratio"] = df["amount"] / (df["oldbalanceDest"] + 1)

    df["is_zero_oldbalanceOrg"] = (df["oldbalanceOrg"] == 0).astype(int)
    df["is_zero_oldbalanceDest"] = (df["oldbalanceDest"] == 0).astype(int)

    type_encoded = pd.get_dummies(df["type"], prefix="type", dtype=int)
    df = pd.concat([df, type_encoded], axis=1)

    df = df.drop(columns=["nameOrig", "nameDest", "origin_prefix", "dest_prefix"])

    return df


def process_dataset(
    raw_path: Path = RAW_DATA_PATH,
    processed_path: Path = PROCESSED_DATA_PATH,
    chunk_size: int = 500000,
) -> None:
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw dataset not found: {raw_path}")

    processed_path.parent.mkdir(parents=True, exist_ok=True)

    first_chunk = True
    start_id = 1

    for chunk in pd.read_csv(raw_path, chunksize=chunk_size):
        processed_chunk = create_features(chunk)

        processed_chunk.insert(
            0,
            "transaction_id",
            range(start_id, start_id + len(processed_chunk)),
        )

        start_id += len(processed_chunk)

        processed_chunk.to_csv(
            processed_path,
            mode="w" if first_chunk else "a",
            index=False,
            header=first_chunk,
        )

        first_chunk = False
        print(f"Processed rows till transaction_id: {start_id - 1}")

    print("Feature engineering completed successfully.")
    print(f"Processed dataset saved at: {processed_path}")


if __name__ == "__main__":
    process_dataset()