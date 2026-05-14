# Data Warehousing with Azure Synapse Analytics - Bootcamp Project 3

## 1. Project Overview

This project implements a cloud-based data warehouse solution for a financial institution to consolidate customer transaction data for business intelligence reporting.

The solution uses Azure Data Lake Storage Gen2, Azure Data Factory, Azure Synapse Analytics, Synapse PySpark Notebook, and Parquet file format.

The main goal of this project is to ingest raw customer, account, and transaction CSV files, clean and transform the data using PySpark, and create an optimized Gold reporting layer for analytics.

---

## 2. Problem Statement

A financial institution needs a data warehouse solution to consolidate customer transaction data for business intelligence reporting.

The project requirements are:

- Design a data model for customer transactions
- Ingest raw transaction data using Azure Data Factory
- Create data transformation pipelines using Synapse PySpark pools
- Optimize performance using Parquet format and partitioning
- Validate raw and transformed data
- Generate business reports from the final Gold layer

---

## 3. Tools and Technologies Used

| Tool / Technology | Purpose |
|---|---|
| Azure Data Lake Storage Gen2 | Stores raw, bronze, silver, and gold data |
| Azure Data Factory | Copies raw CSV files into the bronze layer and triggers the notebook |
| Azure Synapse Analytics | Runs PySpark notebooks for transformation |
| Synapse Spark Pool | Executes PySpark transformation logic |
| PySpark | Cleans, transforms, joins, aggregates, and validates data |
| Parquet | Optimized storage format for analytics |
| GitHub | Stores project documentation and code |

---

## 4. Project Data Flow

```text
Raw CSV Files
    ↓
Azure Data Lake Storage Gen2 - raw container
    ↓
Azure Data Factory Copy Activity
    ↓
Azure Data Lake Storage Gen2 - processed/bronze
    ↓
Azure Synapse PySpark Notebook
    ↓
Azure Data Lake Storage Gen2 - processed/silver
    ↓
Azure Data Lake Storage Gen2 - processed/gold
    ↓
Validation and Business Reports using PySpark
```

---

## 5. Storage Folder Structure

The project uses the following ADLS Gen2 folder structure:

```text
raw
├── customers.csv
│
├── accounts.csv
│   
└── transactions.csv
    

processed
├── bronze
│   ├── customers
│   │   └── customers.csv
│   ├── accounts
│   │   └── accounts.csv
│   └── transactions
│       └── transactions.csv
├── silver
│   ├── customers
│   ├── accounts
│   └── transactions
└── gold
    └── customer_transaction_gold
```

---

## 6. Dataset Description

The project uses three CSV files:

| File | Description |
|---|---|
| customers.csv | Contains customer details such as name, city, province, and customer segment |
| accounts.csv | Contains account details such as account type and opening balance |
| transactions.csv | Contains customer transaction details such as transaction date, amount, payment method, and transaction status |

The raw dataset includes duplicate and missing values to demonstrate data cleaning.

| File | Raw Rows | Cleaned Rows |
|---|---:|---:|
| customers.csv | 17 | 15 |
| accounts.csv | 17 | 15 |
| transactions.csv | 65 | 60 |

---

## 7. Data Model Schema

### Customers Table

| Column | Description |
|---|---|
| customer_id | Unique customer ID |
| customer_name | Customer full name |
| city | Customer city |
| province | Customer province |
| country | Customer country |
| customer_segment | Customer category such as Retail, Premium, or Business |
| signup_date | Customer signup date |

### Accounts Table

| Column | Description |
|---|---|
| account_id | Unique account ID |
| customer_id | Customer ID linked to the account |
| account_type | Type of account |
| opening_balance | Starting account balance |
| account_status | Account status |

### Transactions Table

| Column | Description |
|---|---|
| transaction_id | Unique transaction ID |
| account_id | Account ID linked to the transaction |
| customer_id | Customer ID linked to the transaction |
| transaction_date | Date of transaction |
| transaction_type | Credit or Debit |
| amount | Transaction amount |
| merchant_category | Transaction merchant category |
| payment_method | Payment method used |
| transaction_status | Transaction status |
| transaction_location | Transaction location |
| ingestion_timestamp | Timestamp when data was ingested |

### Gold Reporting Table

The Gold table combines transaction, customer, and account data into one reporting-ready table.

| Column | Description |
|---|---|
| transaction_id | Unique transaction ID |
| transaction_date | Date of transaction |
| transaction_year | Year extracted from transaction date |
| transaction_month | Month extracted from transaction date |
| transaction_type | Credit or Debit |
| amount | Transaction amount |
| merchant_category | Merchant category |
| payment_method | Payment method |
| transaction_status | Transaction status |
| transaction_location | Transaction location |
| customer_id | Customer ID |
| customer_name | Customer name |
| city | Customer city |
| province | Customer province |
| customer_segment | Customer segment |
| account_id | Account ID |
| account_type | Account type |

---

## 8. Azure Resources Created

| Resource | Example Name |
|---|---|
| Resource Group | rg-synapse-bootcamp-vishnu |
| Storage Account | stsynbootcampvishnu |
| Raw Container | raw |
| Processed Container | processed |
| Data Factory | adf-synapse-bootcamp-vishnu |
| Synapse Workspace | synbootcampvishnu |
| Spark Pool | sparkpool1 |

---

## 9. Azure Data Factory Pipeline

### Pipeline Name

```text
PL_Ingest_Raw_To_Bronze
```

### Pipeline Activities

```text
Copy_Customers_To_Bronze
        ↓
Copy_Accounts_To_Bronze
        ↓
Copy_Transactions_To_Bronze
        ↓
Run_Bronze_To_Gold_Notebook
```

### ADF Linked Services

| Linked Service | Purpose |
|---|---|
| LS_ADLS_Gen2 | Connects ADF to ADLS Gen2 |
| LS_Synapse_Workspace | Connects ADF to Azure Synapse Analytics workspace |

### ADF Copy Activity Source and Sink Paths

| Activity | Source Path | Sink Path |
|---|---|---|
| Copy_Customers_To_Bronze | raw/customers/customers.csv | processed/bronze/customers/customers.csv |
| Copy_Accounts_To_Bronze | raw/accounts/accounts.csv | processed/bronze/accounts/accounts.csv |
| Copy_Transactions_To_Bronze | raw/transactions/transactions.csv | processed/bronze/transactions/transactions.csv |

---

## 10. Permissions Used

The following access was configured:

| Service | Permission |
|---|---|
| Azure Data Factory managed identity | Storage Blob Data Contributor on storage account |
| Synapse workspace managed identity | Storage Blob Data Contributor on storage account |
| Azure Data Factory managed identity | Synapse workspace access to run notebook |

This allows ADF to copy files to ADLS and trigger the Synapse notebook.

---

## 11. Synapse PySpark Notebook

### Notebook Name

```text
NB_Transform_Bronze_To_Gold
```

The notebook performs the following steps:

1. Read Bronze CSV data
2. Clean the data
3. Save cleaned data into Silver layer as Parquet
4. Join transactions, customers, and accounts
5. Create Gold reporting table
6. Save Gold table as partitioned Parquet
7. Validate row counts
8. Generate business reports
9. Check schema and execution plan
10. Verify Gold folder structure

---

## 12. PySpark Code Used

### Cell 1: Import Libraries and Read Bronze Data

```python
from pyspark.sql.functions import col, year, month, sum as spark_sum, count, round

storage_account = "stsynbootcampvishnu"

bronze_base = f"abfss://processed@{storage_account}.dfs.core.windows.net/bronze"
silver_base = f"abfss://processed@{storage_account}.dfs.core.windows.net/silver"
gold_base = f"abfss://processed@{storage_account}.dfs.core.windows.net/gold"

customers_path = f"{bronze_base}/customers/customers.csv"
accounts_path = f"{bronze_base}/accounts/accounts.csv"
transactions_path = f"{bronze_base}/transactions/transactions.csv"

customers_df = spark.read.option("header", True).csv(customers_path)
accounts_df = spark.read.option("header", True).csv(accounts_path)
transactions_df = spark.read.option("header", True).csv(transactions_path)

display(customers_df)
display(accounts_df)
display(transactions_df)
```

---

### Cell 2: Clean and Prepare Data

```python
customers_clean = customers_df.dropDuplicates(["customer_id"]).dropna(subset=["customer_id"])

accounts_clean = accounts_df.dropDuplicates(["account_id"]).dropna(subset=["account_id", "customer_id"])

transactions_clean = (
    transactions_df
    .dropDuplicates(["transaction_id"])
    .dropna(subset=["transaction_id", "account_id", "customer_id", "transaction_date", "amount"])
    .withColumn("transaction_date", col("transaction_date").cast("date"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
)
```

### Transformations Performed

- Removed duplicate customer records based on `customer_id`
- Removed duplicate account records based on `account_id`
- Removed duplicate transaction records based on `transaction_id`
- Removed rows with missing required fields
- Converted `transaction_date` from string to date
- Converted `amount` from string to double
- Converted `ingestion_timestamp` from string to timestamp

---

### Cell 3: Save Silver Layer

```python
customers_clean.write.mode("overwrite").parquet(f"{silver_base}/customers")
accounts_clean.write.mode("overwrite").parquet(f"{silver_base}/accounts")
transactions_clean.write.mode("overwrite").partitionBy("transaction_date").parquet(f"{silver_base}/transactions")
```

The cleaned data is saved into the Silver layer in Parquet format.

The transactions data is partitioned by `transaction_date`.

---

### Cell 4: Create Gold Reporting Table

```python
customer_transaction_gold = (
    transactions_clean.alias("t")
    .join(customers_clean.alias("c"), col("t.customer_id") == col("c.customer_id"), "left")
    .join(accounts_clean.alias("a"), col("t.account_id") == col("a.account_id"), "left")
    .select(
        col("t.transaction_id"),
        col("t.transaction_date"),
        year(col("t.transaction_date")).alias("transaction_year"),
        month(col("t.transaction_date")).alias("transaction_month"),
        col("t.transaction_type"),
        col("t.amount"),
        col("t.merchant_category"),
        col("t.payment_method"),
        col("t.transaction_status"),
        col("t.transaction_location"),
        col("c.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("c.province"),
        col("c.customer_segment"),
        col("a.account_id"),
        col("a.account_type")
    )
)

display(customer_transaction_gold)
```

This creates the Gold reporting table by joining:

```text
transactions_clean + customers_clean + accounts_clean
```

The Gold table includes transaction details, customer details, and account details.

---

### Cell 5: Save Gold Layer

```python
customer_transaction_gold.write.mode("overwrite").partitionBy(
    "transaction_year",
    "transaction_month"
).parquet(f"{gold_base}/customer_transaction_gold")
```

The Gold table is saved as Parquet and partitioned by:

```text
transaction_year
transaction_month
```

This improves performance for monthly and yearly reporting.

---

### Cell 6: Validate Row Counts

```python
raw_transaction_count = transactions_df.count()
silver_transaction_count = transactions_clean.count()
gold_transaction_count = customer_transaction_gold.count()

print("Raw transaction count:", raw_transaction_count)
print("Silver transaction count:", silver_transaction_count)
print("Gold transaction count:", gold_transaction_count)
```

Expected output when using the dirty raw dataset:

```text
Raw transaction count: 65
Silver transaction count: 60
Gold transaction count: 60
```

---

### Cell 7: Check Data Completeness

```python
transactions_clean.select([
    count(col(c)).alias(c) for c in transactions_clean.columns
]).show()
```

This checks the non-null count for each column in the cleaned transaction data.

Expected output should show 60 valid records for the required transaction columns.

---

### Cell 8: Customer Segment Summary

```python
customer_transaction_gold.groupBy("customer_segment").agg(
    count("*").alias("total_transactions"),
    round(spark_sum("amount"), 2).alias("total_amount")
).show()
```

This creates a summary of transactions by customer segment.

Example output:

| customer_segment | total_transactions | total_amount |
|---|---:|---:|
| Premium | 13 | 31578.39 |
| Business | 19 | 24679.25 |
| Retail | 28 | 63209.89 |

---

### Cell 9: Read Gold Data

```python
gold_path = f"{gold_base}/customer_transaction_gold"

gold_df = spark.read.parquet(gold_path)

display(gold_df)
```

This reads the Gold Parquet data back into a DataFrame to confirm it was saved successfully.

---

### Cell 10: Customer Segment Report

```python
from pyspark.sql.functions import count, sum as spark_sum, round

segment_report = (
    gold_df
    .groupBy("customer_segment")
    .agg(
        count("*").alias("total_transactions"),
        round(spark_sum("amount"), 2).alias("total_amount")
    )
    .orderBy("customer_segment")
)

display(segment_report)
```

---

### Cell 11: Monthly Transaction Report

```python
monthly_report = (
    gold_df
    .groupBy("transaction_year", "transaction_month")
    .agg(
        count("*").alias("total_transactions"),
        round(spark_sum("amount"), 2).alias("total_amount")
    )
    .orderBy("transaction_year", "transaction_month")
)

display(monthly_report)
```

---

### Cell 12: Transaction Type Report

```python
transaction_type_report = (
    gold_df
    .groupBy("transaction_type")
    .agg(
        count("*").alias("total_transactions"),
        round(spark_sum("amount"), 2).alias("total_amount")
    )
)

display(transaction_type_report)
```

---

### Cell 13: Payment Method Report

```python
payment_method_report = (
    gold_df
    .groupBy("payment_method")
    .agg(
        count("*").alias("total_transactions"),
        round(spark_sum("amount"), 2).alias("total_amount")
    )
    .orderBy("payment_method")
)

display(payment_method_report)
```

---

### Cell 14: Final Validation Check

```python
raw_transaction_count = transactions_df.count()
silver_transaction_count = transactions_clean.count()
gold_transaction_count = gold_df.count()

print("Raw transaction count:", raw_transaction_count)
print("Silver transaction count:", silver_transaction_count)
print("Gold transaction count:", gold_transaction_count)

if raw_transaction_count == silver_transaction_count == gold_transaction_count:
    print("Validation Passed: Raw, Silver, and Gold transaction counts match.")
else:
    print("Validation Failed: Counts do not match.")
```

When using the dirty raw dataset, the raw count will be higher because it includes duplicates and rows with missing key values.

Expected output:

```text
Raw transaction count: 65
Silver transaction count: 60
Gold transaction count: 60
Validation Failed: Counts do not match.
```

This is acceptable because cleaning removed invalid duplicate and null records.

For dirty data, the explanation is:

```text
The raw layer contains 65 transaction rows, including duplicate and invalid records. After cleaning, the Silver and Gold layers contain 60 valid transaction rows. This proves that the transformation process removed duplicates and rows with missing required fields.
```

---

### Cell 15: Print Gold Schema

```python
gold_df.printSchema()
```

This confirms that the Gold table has correct column names and data types.

Important data types:

| Column | Data Type |
|---|---|
| transaction_date | date |
| amount | double |
| transaction_year | int |
| transaction_month | int |

---

### Cell 16: Explain Query Plan

```python
gold_df.explain(True)
```

This displays the Spark execution plan.

It is used to show how Spark reads the optimized Parquet data.

---

### Cell 17: Verify Gold Folder Structure

```python
mssparkutils.fs.ls(f"{gold_base}/customer_transaction_gold")
```

This confirms the Gold layer was written successfully and partition folders were created.

Expected partition folder example:

```text
transaction_year=2025/
```

Inside it:

```text
transaction_month=1/
transaction_month=2/
transaction_month=3/
```

---

## 13. Business Reports Created

The following business reports were generated from the Gold layer:

### Customer Segment Report

Shows total transactions and total amount by customer segment.

Customer segments include:

```text
Business
Premium
Retail
```

### Monthly Transaction Report

Shows transaction count and total amount by year and month.

### Transaction Type Report

Shows total credit and debit transactions.

### Payment Method Report

Shows total transactions and amount by payment method.

---

## 14. Optimization Techniques Used

### 1. Parquet Format

The Silver and Gold layers are saved as Parquet files.

Parquet is optimized for analytics because it is columnar and more efficient than CSV for querying large datasets.

### 2. Partitioning

The Silver transactions dataset is partitioned by:

```text
transaction_date
```

The Gold reporting table is partitioned by:

```text
transaction_year
transaction_month
```

This improves query performance because Spark can read only the required partition folders instead of scanning the full dataset.

### 3. Cleaned Reporting Layer

The Gold layer contains only selected reporting columns, making it easier and faster to use for business intelligence.

---

## 15. Validation Performed

The project validates data quality using the following checks:

### Row Count Validation

```python
raw_transaction_count = transactions_df.count()
silver_transaction_count = transactions_clean.count()
gold_transaction_count = gold_df.count()
```

This compares the number of transaction records across Bronze, Silver, and Gold layers.

### Null Value Check

```python
transactions_clean.select([
    count(col(c)).alias(c) for c in transactions_clean.columns
]).show()
```

This confirms that required columns contain valid values after cleaning.

### Schema Check

```python
gold_df.printSchema()
```

This verifies the final Gold table structure and data types.

### Folder Structure Check

```python
mssparkutils.fs.ls(f"{gold_base}/customer_transaction_gold")
```

This confirms that the optimized partitioned Gold output was created in ADLS.

---

## 16. How to Run the Project

### Step 1: Upload Raw CSV Files

Upload the CSV files to ADLS Gen2 using this structure:

```text
raw/customers/customers.csv
raw/accounts/accounts.csv
raw/transactions/transactions.csv
```

### Step 2: Run ADF Pipeline

Run the ADF pipeline:

```text
PL_Ingest_Raw_To_Bronze
```

The pipeline will:

1. Copy customers data to Bronze
2. Copy accounts data to Bronze
3. Copy transactions data to Bronze
4. Run the Synapse PySpark notebook

### Step 3: Check Bronze Output

Confirm files exist in:

```text
processed/bronze/customers/customers.csv
processed/bronze/accounts/accounts.csv
processed/bronze/transactions/transactions.csv
```

### Step 4: Check Silver Output

Confirm cleaned Parquet files exist in:

```text
processed/silver/customers
processed/silver/accounts
processed/silver/transactions
```

### Step 5: Check Gold Output

Confirm final reporting table exists in:

```text
processed/gold/customer_transaction_gold
```

### Step 6: Review Notebook Outputs

Check the notebook outputs for:

- Validation results
- Customer segment report
- Monthly transaction report
- Transaction type report
- Payment method report
- Schema output
- Execution plan
- Gold folder structure

---

## 17. Expected Results

When using the dirty raw dataset:

```text
Raw customer rows: 17
Clean customer rows: 15

Raw account rows: 17
Clean account rows: 15

Raw transaction rows: 65
Clean transaction rows: 60
Gold transaction rows: 60
```

Expected transaction validation:

```text
Raw transaction count: 65
Silver transaction count: 60
Gold transaction count: 60
```

This proves that the notebook removed duplicate and invalid rows during the Bronze to Silver transformation.

---

## 18. Screenshots to Include in Submission

The following screenshots should be included in the final submission:

```text
1. ADLS raw folder with CSV files
2. ADF pipeline design
3. ADF successful pipeline run
4. ADLS bronze folder
5. Synapse notebook with successful execution
6. ADLS silver folder
7. ADLS gold folder
8. Validation count output
9. Customer segment report
10. Monthly transaction report
11. Transaction type report
12. Payment method report
13. Gold schema output
14. Spark explain plan
15. Gold partition folder structure
```

---

## 19. Project Completion Summary

This project successfully implemented a cloud-based data warehouse solution using Azure Data Factory, Azure Data Lake Storage Gen2, and Azure Synapse Analytics.

Raw customer, account, and transaction CSV files were uploaded to ADLS and ingested into the Bronze layer using Azure Data Factory. A Synapse PySpark notebook cleaned and transformed the data by removing duplicates, dropping records with missing required values, and converting columns into proper data types.

The cleaned data was stored in the Silver layer as Parquet files. Then, transaction data was joined with customer and account data to create a Gold reporting table. The Gold table was partitioned by transaction year and transaction month to improve reporting performance.

Validation was completed by comparing row counts across the Raw, Silver, and Gold layers and by checking schema, non-null values, and folder structure. Business reports were generated using PySpark to summarize transactions by customer segment, month, transaction type, and payment method.

---

## 20. Conclusion

The project meets the required objectives:

- Designed a customer transaction data model
- Ingested raw data using Azure Data Factory
- Transformed data using Synapse PySpark Notebook
- Created Bronze, Silver, and Gold layers
- Saved optimized Parquet files
- Applied partitioning for performance improvement
- Validated data quality and transformation results
- Generated business intelligence reports from the Gold layer
