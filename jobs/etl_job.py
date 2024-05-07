import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from dependencies.spark import start_spark

def main():
    spark = start_spark()
    data = extract_data(spark)
    print(data.head())

def extract_data(spark):
    df = (
        spark
        .read
        .csv("tests/test_data/pbp-2023.csv"))

    return df

if __name__ == '__main__':
    main()
