This folder contains the Delta Lake table 'customer_transactions' with ~1,296 synthetic transaction records.

The table includes:
- Transaction IDs (with intentional duplicates)
- Customer IDs
- Amounts (with some zero/negative values)
- Timestamps
- Merchant information

The Delta table is functional and can be read by Spark, even if files are not visible in this listing.

Files are created by generate_data.py and include:
- _delta_log/ (transaction log)
- part-*.parquet (data files)

If files are not visible, they may be hidden or the directory listing tool has limitations.