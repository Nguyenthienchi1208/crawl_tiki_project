import pandas as pd

INPUT_FILE = "id_tiki.csv"
OUT_DIR = "."
CHUNK_SIZE = 50_000

df = pd.read_csv(INPUT_FILE)
df = df[df['id'].notna()].copy()
df['id'] = df['id'].astype(str).str.strip()
total_before = len(df)
dup = df['id'].duplicated().sum()

if dup:
    print(f"Found {dup} duplicates, removing them...")
    df = df.drop_duplicates(subset='id').reset_index(drop=True)
else:
    print("No duplicates found.")
print(f"Before: {total_before}, After: {len(df)}")

# Split into chunks
for i in range(0, len(df), CHUNK_SIZE):
    chunk = df.iloc[i:i + CHUNK_SIZE]
    out_file = f"{OUT_DIR}/id_tiki_part_{i // CHUNK_SIZE + 1}.csv"
    try:
        chunk.to_csv(out_file, index=False)
        print(f"Saved {out_file} ({len(chunk)} rows)")
    except Exception as e:
        print(f"Error saving {out_file}: {e}")
