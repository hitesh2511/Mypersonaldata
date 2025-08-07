import pandas as pd

# ----------------------------
# Adjust these paths as needed
path_all_indices = r'https://raw.githubusercontent.com/hitesh2511/Mypersonaldata/main/data/all_indices.csv'
path_old_indices = r'https://raw.githubusercontent.com/hitesh2511/Mypersonaldata/main/data/old_indices.csv'
path_NSE_contract = r'https://raw.githubusercontent.com/hitesh2511/Mypersonaldata/main/data/NSE_FO_contract.csv'
output_excel = r'https://raw.githubusercontent.com/hitesh2511/Mypersonaldata/main/data/filtered_output.xlsx'
# ----------------------------

def main():

    print("Loading all_indices.csv ...")
    all_indices = pd.read_csv(path_all_indices)
    print("Loaded all_indices.csv with shape:", all_indices.shape)

    print("Loading old_indices.csv ...")
    old_indices = pd.read_csv(path_old_indices)
    print("Loaded old_indices.csv with shape:", old_indices.shape)

    print("Loading NSE_FO_contract.csv ...")
    NSE_FO_contract = pd.read_csv(path_NSE_contract)
    print("Loaded NSE_FO_contract.csv with shape:", NSE_FO_contract.shape)

    # Step 1: Extract required columns from all_indices
    cols_needed = ['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'totalTradedVolume']
    missing_cols = [col for col in cols_needed if col not in all_indices.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in all_indices.csv: {missing_cols}")
    all_filtered = all_indices[cols_needed].copy()

    # Convert totalTradedVolume to numeric to avoid issues
    all_filtered['totalTradedVolume'] = pd.to_numeric(all_filtered['totalTradedVolume'], errors='coerce')

    # Step 2: Prepare old_indices for lookup - use totalTradedVolume as yesterdayvolume
    if 'symbol' not in old_indices.columns or 'totalTradedVolume' not in old_indices.columns:
        raise ValueError("old_indices.csv must contain 'symbol' and 'totalTradedVolume' columns.")
    old_renamed = old_indices[['symbol', 'totalTradedVolume']].rename(columns={'totalTradedVolume': 'yesterdayvolume'}).copy()
    old_renamed['yesterdayvolume'] = pd.to_numeric(old_renamed['yesterdayvolume'], errors='coerce')

    # Step 3: Merge on 'symbol'
    merged = all_filtered.merge(old_renamed, on='symbol', how='left')
    print("After merging, merged data shape:", merged.shape)

    # Step 4: Filter out zero or missing yesterdayvolume to avoid div-by-zero errors
    missing_yesterday = merged['yesterdayvolume'].isnull().sum()
    zero_yesterday = (merged['yesterdayvolume'] == 0).sum()
    print(f"Rows with missing yesterdayvolume: {missing_yesterday}")
    print(f"Rows with zero yesterdayvolume: {zero_yesterday}")

    merged = merged[merged['yesterdayvolume'] > 0].copy()

    # Step 5: Calculate change% = (totalTradedVolume - yesterdayvolume) / yesterdayvolume
    merged['change%'] = (merged['totalTradedVolume'] - merged['yesterdayvolume']) / merged['yesterdayvolume']

    print("Sample data with change%:")
    print(merged[['symbol', 'totalTradedVolume', 'yesterdayvolume', 'change%']].head(10))

    print("change% statistics:")
    print(merged['change%'].describe())

    # Step 6: Filter rows based on your specified ranges
    filtered_neg = merged[(merged['change%'] >= -0.6) & (merged['change%'] <= -0.49)].copy()
    filtered_pos = merged[(merged['change%'] >= 0.4) & (merged['change%'] <= 0.6)].copy()
    print(f"Filtered negative changes (-0.6 to -0.49): {filtered_neg.shape[0]} rows")
    print(f"Filtered positive changes (0.4 to 0.6): {filtered_pos.shape[0]} rows")

    # Step 7: Prepare symbol set from NSE_FO_contract for lookup
    contract_symbols = set(NSE_FO_contract['symbol'].dropna().unique())

    def filterdata_fn(symbol):
        if (
            pd.notna(symbol)
            and str(symbol).strip() != ''
            and symbol != '#NA'
            and symbol in contract_symbols
        ):
            return symbol
        return ''

    # Apply filterdata logic to filtered_neg and filtered_pos
    filtered_neg['filterdata'] = filtered_neg['symbol'].apply(filterdata_fn)
    filtered_neg_filtered = filtered_neg[filtered_neg['filterdata'] != ''].copy()
    filtered_neg_filtered = filtered_neg_filtered.drop_duplicates(subset=['filterdata'])
    print(f"Negative filtered data after filterdata lookup and deduplication: {filtered_neg_filtered.shape[0]} rows")

    filtered_pos['filterdata'] = filtered_pos['symbol'].apply(filterdata_fn)
    filtered_pos_filtered = filtered_pos[filtered_pos['filterdata'] != ''].copy()
    filtered_pos_filtered = filtered_pos_filtered.drop_duplicates(subset=['filterdata'])
    print(f"Positive filtered data after filterdata lookup and deduplication: {filtered_pos_filtered.shape[0]} rows")

    # Define output directory relative to the repository root
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)
    output_excel = os.path.join(output_dir, 'filtered_output.xlsx')
    print(f"Saving results to Excel file: {output_excel}")
    try:
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            filtered_neg_filtered.to_excel(writer, sheet_name='Neg_Change', index=False)
            filtered_pos_filtered.to_excel(writer, sheet_name='Pos_Change', index=False)
        print("Excel file saved successfully.")
    except Exception as e:
        print(f"Error saving Excel file: {e}")
        raise

if __name__ == "__main__":
    main()
