import os
import sys
import pandas as pd

# MVP: ściągamy jeden plik part-*.parquet z Silver i testujemy na pandas
# (Delta to folder - bierzemy parquet z niego)
SILVER_LOCAL = "/tmp/silver"

def fail(msg: str):
    print("❌", msg)
    sys.exit(1)

def ok(msg: str):
    print("✅", msg)

def main():
    # To działa w kontenerze, bo podepniemy /data z MinIO
    # Ścieżka na dysku minio: /data/lake/silver/orders_delta/
    base = os.getenv("SILVER_PATH_ON_DISK", "/data/lake/silver/orders_delta")

    if not os.path.isdir(base):
        fail(f"Nie ma folderu Silver na dysku: {base}")

    # znajdź pierwszy parquet (pomijamy _delta_log)
    parquet_files = []
    for root, dirs, files in os.walk(base):
        if "_delta_log" in root:
            continue
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))
    if not parquet_files:
        fail(f"Nie znaleziono plików parquet w: {base}")

    sample = parquet_files[0]
    df = pd.read_parquet(sample)

    # ===== 3 proste testy MVP =====
    # 1) dataset niepusty
    if len(df) == 0:
        fail("Silver jest pusty (0 wierszy).")
    ok(f"Silver niepusty: {len(df)} wierszy (próbka z {os.path.basename(sample)})")

    # 2) brak NULL w kluczowej kolumnie (spróbujmy order_id jeśli istnieje)
    key_col = "order_id" if "order_id" in df.columns else df.columns[0]
    if df[key_col].isna().any():
        fail(f"Kolumna {key_col} ma NULL-e.")
    ok(f"Kolumna {key_col} bez NULL-i")

    # 3) brak duplikatów w key_col
    if df[key_col].duplicated().any():
        fail(f"Kolumna {key_col} ma duplikaty.")
    ok(f"Kolumna {key_col} bez duplikatów")

    print("\n🎉 MVP walidacja Silver zakończona sukcesem.")

if __name__ == "__main__":
    main()
