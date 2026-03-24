import sys
import os
import logging

# Create logs folder
os.makedirs("logs", exist_ok=True)

# Logging setup
logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("🚀 Pipeline started")

# Fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.load_data import load_all_data
from scripts.remove_duplicates import remove_duplicates
from scripts.clean_missing import handle_missing
from scripts.handle_outliers import handle_outliers
from scripts.save_data import save_data


# ✅ STATS FUNCTION
def get_all_stats(dfs, label=""):
    total_missing = 0
    total_duplicates = 0

    print(f"\n📊 {label} STATS (PER FILE):")

    for name, df in dfs.items():
        missing = df.isnull().sum().sum()
        duplicates = df.duplicated().sum()

        total_missing += missing
        total_duplicates += duplicates

        print(f"{name} → Missing: {missing}, Duplicates: {duplicates}")

    print(f"\n🔹 TOTAL Missing: {total_missing}")
    print(f"🔹 TOTAL Duplicates: {total_duplicates}")

    return total_missing, total_duplicates


# ✅ MAIN PIPELINE
def run_pipeline():
    try:
        logging.info("Loading data...")
        dfs = load_all_data()

        # BEFORE STATS
        before_missing, before_duplicates = get_all_stats(dfs, "BEFORE CLEANING")

        # Apply cleaning per dataframe
        for key in dfs:
            dfs[key] = handle_missing(dfs[key])
            dfs[key] = remove_duplicates(dfs[key])
            dfs[key] = handle_outliers(dfs[key])

        # AFTER STATS
        after_missing, after_duplicates = get_all_stats(dfs, "AFTER CLEANING")

        logging.info("Saving cleaned data...")
        save_data(dfs)

        # FINAL SUMMARY
        print("\n🔥 FINAL SUMMARY:")
        print(f"Missing removed: {before_missing - after_missing}")
        print(f"Duplicates removed: {before_duplicates - after_duplicates}")

        logging.info(f"Missing removed: {before_missing - after_missing}")
        logging.info(f"Duplicates removed: {before_duplicates - after_duplicates}")

        logging.info("✅ Pipeline completed successfully!")

    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}")
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    run_pipeline()