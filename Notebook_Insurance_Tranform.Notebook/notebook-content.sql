-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "f07fe9f1-7b3c-4999-b710-ecae0e773e47",
-- META       "default_lakehouse_name": "Bronze",
-- META       "default_lakehouse_workspace_id": "00c445b5-7149-4b80-b1cd-84ce0a1a4c4f",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "f07fe9f1-7b3c-4999-b710-ecae0e773e47"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************


CREATE DATABASE insurance;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DROP TABLE IF EXISTS insurance.dim_customer;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.dim_customer (
    customer_id     BIGINT,
    customer_name   STRING,
    gender          STRING,
    age             INT,
    income_group    STRING,
    city            STRING
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.dim_customer (
    customer_id     INT,
    customer_name   STRING,
    gender          STRING,
    age             INT,
    income_group    STRING,
    city            STRING
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import rand, floor, when, col
-- MAGIC 
-- MAGIC df = spark.range(10000)
-- MAGIC 
-- MAGIC df = df.withColumn("customer_id", (col("id") + 1).cast("bigint")) \
-- MAGIC        .withColumn("customer_name", 
-- MAGIC                    when(rand() > 0.5, "Amit Sharma")
-- MAGIC                    .otherwise("Priya Patel")) \
-- MAGIC        .withColumn("gender",
-- MAGIC                    when(rand() > 0.5, "Male")
-- MAGIC                    .otherwise("Female")) \
-- MAGIC        .withColumn("age", (floor(rand()*45) + 21).cast("int")) \
-- MAGIC        .withColumn("income_group",
-- MAGIC                    when(rand() > 0.75, "High")
-- MAGIC                    .when(rand() > 0.5, "Upper-Middle")
-- MAGIC                    .when(rand() > 0.25, "Middle")
-- MAGIC                    .otherwise("Low")) \
-- MAGIC        .withColumn("city",
-- MAGIC                    when(rand() > 0.75, "Mumbai")
-- MAGIC                    .when(rand() > 0.5, "Delhi")
-- MAGIC                    .when(rand() > 0.25, "Bangalore")
-- MAGIC                    .otherwise("Pune")) \
-- MAGIC        .drop("id")
-- MAGIC 
-- MAGIC # Overwrite & create table with dataframe schema
-- MAGIC df.write.mode("overwrite").format("delta").saveAsTable("insurance.dim_customer")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM insurance.dim_customer order by customer_id limit 100 ;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.dim_policy (
    policy_id     INT,
    policy_name   STRING,
    policy_type   STRING,
    category      STRING
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO insurance.dim_policy VALUES
(1,'Car Insurance Premium','Insurance','Car'),
(2,'Life Insurance Gold','Insurance','Life'),
(3,'Health Secure Plan','Insurance','Health'),
(4,'Home Loan Advantage','Loan','Home Loan'),
(5,'Personal Loan Express','Loan','Personal Loan');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from insurance.dim_policy;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.dim_region (
    region_id    INT,
    region_name  STRING,
    zone         STRING
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO insurance.dim_region VALUES
(1,'Mumbai','West'),
(2,'Pune','West'),
(3,'Delhi','North'),
(4,'Bangalore','South'),
(5,'Chennai','South');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.dim_festival (
    festival_id    INT,
    festival_name  STRING,
    festival_year  INT
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO insurance.dim_festival VALUES
(1,'Diwali',2024),
(2,'Diwali',2025),
(3,'Christmas',2024),
(4,'New Year',2024),
(5,'New Year',2025),
(6,'Year End Sale',2024),
(7,'Year End Sale',2025);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from insurance.dim_festival;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.dim_date (
    date_id     INT,
    full_date   DATE,
    year        INT,
    month       INT
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DROP TABLE IF EXISTS insurance.dim_date;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from datetime import datetime
-- MAGIC 
-- MAGIC # Generate date range (5 years example)
-- MAGIC start_date = "2020-01-01"
-- MAGIC end_date   = "2026-12-31"
-- MAGIC 
-- MAGIC df = spark.sql(f"""
-- MAGIC SELECT sequence(to_date('{start_date}'),
-- MAGIC                 to_date('{end_date}'),
-- MAGIC                 interval 1 day) as date_seq
-- MAGIC """)
-- MAGIC 
-- MAGIC df = df.select(explode(col("date_seq")).alias("full_date"))
-- MAGIC 
-- MAGIC df = df.withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("int")) \
-- MAGIC        .withColumn("year", year("full_date")) \
-- MAGIC        .withColumn("month", month("full_date")) \
-- MAGIC        .withColumn("month_name", date_format("full_date", "MMMM")) \
-- MAGIC        .withColumn("quarter", concat(lit("Q"), quarter("full_date"))) \
-- MAGIC        .withColumn("day", dayofmonth("full_date")) \
-- MAGIC        .withColumn("day_of_week", date_format("full_date", "EEEE")) \
-- MAGIC        .withColumn("is_weekend",
-- MAGIC                    when(dayofweek("full_date").isin(1,7),"Yes")
-- MAGIC                    .otherwise("No"))
-- MAGIC 
-- MAGIC # Optional: Add simple festival tagging (example logic)
-- MAGIC df = df.withColumn("festival_name",
-- MAGIC                    when((month("full_date")==11) & (dayofmonth("full_date")==12),"Diwali")
-- MAGIC                    .when((month("full_date")==12) & (dayofmonth("full_date")==25),"Christmas")
-- MAGIC                    .when((month("full_date")==1) & (dayofmonth("full_date")==1),"New Year Sale")
-- MAGIC                    .otherwise(None))
-- MAGIC 
-- MAGIC df.write.mode("overwrite").format("delta").saveAsTable("insurance.dim_date")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from insurance.dim_date;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select distinct year,festival_name from insurance.dim_date;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select distinct festival_name from insurance.dim_date;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.fact_policy_sales (
    sales_id        INT,
    customer_id     INT,
    policy_id       INT,
    region_id       INT,
    festival_id     INT,
    date_id         INT,
    sales_amount    DECIMAL(12,2),
    cost_amount     DECIMAL(12,2),
    profit_amount   DECIMAL(12,2)
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import col, rand, floor
-- MAGIC 
-- MAGIC # generate 4000 base rows
-- MAGIC df = spark.range(4000)
-- MAGIC 
-- MAGIC df = df.withColumn("sales_id", (col("id") + 1).cast("int")) \
-- MAGIC        .withColumn("customer_id", (floor(rand()*10000) + 1).cast("int")) \
-- MAGIC        .withColumn("policy_id", (floor(rand()*5) + 1).cast("int")) \
-- MAGIC        .withColumn("region_id", (floor(rand()*5) + 1).cast("int")) \
-- MAGIC        .withColumn("festival_id", (floor(rand()*6) + 1).cast("int")) \
-- MAGIC        .withColumn("date_id", (floor(rand()*2500) + 20200101).cast("int")) \
-- MAGIC        .withColumn("sales_amount", (floor(rand()*90000) + 10000).cast("decimal(12,2)")) \
-- MAGIC        .withColumn("cost_amount", (floor(rand()*50000) + 5000).cast("decimal(12,2)")) \
-- MAGIC        .withColumn("profit_amount", (col("sales_amount") - col("cost_amount")).cast("decimal(12,2)")) \
-- MAGIC        .drop("id")
-- MAGIC 
-- MAGIC df.write.mode("append").saveAsTable("insurance.fact_policy_sales")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM insurance.fact_policy_sales limit 100;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    d.year,
    fdim.festival_name,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.profit_amount) AS total_profit
FROM insurance.fact_policy_sales f
JOIN insurance.dim_date d
    ON f.date_id = d.date_key
JOIN insurance.dim_festival fdim
    ON f.festival_id = fdim.festival_id
WHERE fdim.festival_name = 'Diwali'
GROUP BY d.year, fdim.festival_name
ORDER BY d.year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT
    fe.festival_year,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.profit_amount) AS total_profit
FROM insurance.fact_policy_sales f
JOIN insurance.dim_festival fe
ON f.festival_id = fe.festival_id
WHERE fe.festival_name = 'Diwali'
GROUP BY fe.festival_year
ORDER BY fe.festival_year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT
    SUM(CASE WHEN fe.festival_year = 2025 THEN f.sales_amount END) AS sales_2025,
    SUM(CASE WHEN fe.festival_year = 2024 THEN f.sales_amount END) AS sales_2024,
    SUM(CASE WHEN fe.festival_year = 2025 THEN f.sales_amount END) -
    SUM(CASE WHEN fe.festival_year = 2024 THEN f.sales_amount END) AS difference
FROM insurance.fact_policy_sales f
JOIN insurance.dim_festival fe
ON f.festival_id = fe.festival_id
WHERE fe.festival_name = 'Diwali';

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    fe.festival_name,
    fe.festival_year,
    COUNT(*) as records
FROM insurance.fact_policy_sales f
JOIN insurance.dim_festival fe
ON f.festival_id = fe.festival_id
WHERE fe.festival_name = 'Diwali'
GROUP BY fe.festival_name, fe.festival_year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    d.year,
    SUM(f.sales_amount) AS total_sales
FROM insurance.fact_policy_sales f
JOIN insurance.dim_date d
    ON f.date_id = d.date_key
JOIN insurance.dim_festival fe
    ON f.festival_id = fe.festival_id
WHERE fe.festival_name = 'Diwali'
GROUP BY d.year
ORDER BY d.year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    d.year,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.profit_amount) AS total_profit
FROM insurance.fact_policy_sales f
JOIN insurance.dim_date d
ON f.date_id = d.date_key
WHERE d.festival_name = 'Diwali'
GROUP BY d.year
ORDER BY d.year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    p.policy_name,
    SUM(f.sales_amount) AS total_sales
FROM insurance.fact_policy_sales f
JOIN insurance.dim_policy p
ON f.policy_id = p.policy_id
GROUP BY p.policy_name;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    r.region_name,
    SUM(f.profit_amount) AS total_profit
FROM insurance.fact_policy_sales f
JOIN insurance.dim_region r
ON f.region_id = r.region_id
GROUP BY r.region_name;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE insurance.insurance_policy_sales (
    sale_id BIGINT,
    customer_id STRING,
    policy_id STRING,
    policy_name STRING,
    policy_type STRING,
    region STRING,
    city STRING,
    sales_channel STRING,
    purchase_date DATE,
    purchase_year INT,
    purchase_month INT,
    purchase_quarter STRING,
    purchase_event STRING,
    discount_percentage DECIMAL(5,2),
    policy_duration_months INT,
    premium_amount DECIMAL(12,2)
)
USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC import random
-- MAGIC from datetime import datetime, timedelta
-- MAGIC from decimal import Decimal

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.types import *
-- MAGIC from decimal import Decimal
-- MAGIC import random
-- MAGIC from datetime import datetime, timedelta
-- MAGIC import pyspark.sql.functions as F

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC policy_names = ["Health Secure","Motor Protect","Travel Shield","Family Health Plus","Accident Guard"]
-- MAGIC 
-- MAGIC policy_types = ["Health","Motor","Travel","Life"]
-- MAGIC 
-- MAGIC regions = ["North","South","West","East"]
-- MAGIC 
-- MAGIC cities = ["Mumbai","Delhi","Bangalore","Hyderabad","Pune","Chennai"]
-- MAGIC 
-- MAGIC channels = ["Online","Agent","Branch"]
-- MAGIC 
-- MAGIC events = [
-- MAGIC "Diwali Sale",
-- MAGIC "New Year Offer",
-- MAGIC "Christmas Campaign",
-- MAGIC "Independence Day Offer",
-- MAGIC "Festive Bonanza",
-- MAGIC "No Promotion"
-- MAGIC ]

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC rows.append((
-- MAGIC     i+1,
-- MAGIC     f"CUST{random.randint(1000,9999)}",
-- MAGIC     f"POL{random.randint(100,999)}",
-- MAGIC     random.choice(policy_names),
-- MAGIC     random.choice(policy_types),
-- MAGIC     random.choice(regions),
-- MAGIC     random.choice(cities),
-- MAGIC     random.choice(channels),
-- MAGIC     purchase_date,
-- MAGIC     year,
-- MAGIC     month,
-- MAGIC     quarter,
-- MAGIC     random.choice(events),
-- MAGIC     __builtins__.round(random.uniform(0,30),2),
-- MAGIC     random.choice([12,24,36]),
-- MAGIC     __builtins__.round(random.uniform(5000,50000),2)
-- MAGIC ))

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC rows.append((
-- MAGIC     i+1,
-- MAGIC     f"CUST{random.randint(1000,9999)}",
-- MAGIC     f"POL{random.randint(100,999)}",
-- MAGIC     random.choice(policy_names),
-- MAGIC     random.choice(policy_types),
-- MAGIC     random.choice(regions),
-- MAGIC     random.choice(cities),
-- MAGIC     random.choice(channels),
-- MAGIC     purchase_date,
-- MAGIC     year,
-- MAGIC     month,
-- MAGIC     quarter,
-- MAGIC     random.choice(events),
-- MAGIC     Decimal(str(__builtins__.round(random.uniform(0,30),2))),
-- MAGIC     random.choice([12,24,36]),
-- MAGIC     Decimal(str(__builtins__.round(random.uniform(5000,50000),2)))
-- MAGIC ))

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC schema = StructType([
-- MAGIC 
-- MAGIC StructField("sale_id", LongType(), False),
-- MAGIC StructField("customer_id", StringType(), True),
-- MAGIC StructField("policy_id", StringType(), True),
-- MAGIC StructField("policy_name", StringType(), True),
-- MAGIC StructField("policy_type", StringType(), True),
-- MAGIC StructField("region", StringType(), True),
-- MAGIC StructField("city", StringType(), True),
-- MAGIC StructField("sales_channel", StringType(), True),
-- MAGIC StructField("purchase_date", DateType(), True),
-- MAGIC StructField("purchase_year", IntegerType(), True),
-- MAGIC StructField("purchase_month", IntegerType(), True),
-- MAGIC StructField("purchase_quarter", StringType(), True),
-- MAGIC StructField("purchase_event", StringType(), True),
-- MAGIC StructField("discount_percentage", DecimalType(5,2), True),
-- MAGIC StructField("policy_duration_months", IntegerType(), True),
-- MAGIC StructField("premium_amount", DecimalType(12,2), True)
-- MAGIC 
-- MAGIC ])

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO insurance.insurance_policy_sales
SELECT
    id as sale_id,
    concat('CUST', id) as customer_id,
    concat('POL', (id % 20) + 1) as policy_id,
    concat('Policy_', (id % 20) + 1) as policy_name,

    CASE 
        WHEN id % 3 = 0 THEN 'Health'
        WHEN id % 3 = 1 THEN 'Life'
        ELSE 'Vehicle'
    END as policy_type,

    CASE 
        WHEN id % 4 = 0 THEN 'West'
        WHEN id % 4 = 1 THEN 'South'
        WHEN id % 4 = 2 THEN 'North'
        ELSE 'East'
    END as region,

    CASE 
        WHEN id % 5 = 0 THEN 'Mumbai'
        WHEN id % 5 = 1 THEN 'Delhi'
        WHEN id % 5 = 2 THEN 'Bangalore'
        WHEN id % 5 = 3 THEN 'Hyderabad'
        ELSE 'Pune'
    END as city,

    CASE 
        WHEN id % 2 = 0 THEN 'Online'
        ELSE 'Agent'
    END as sales_channel,

    CASE 
        WHEN id % 4 = 0 THEN date('2022-10-24')
        WHEN id % 4 = 1 THEN date('2023-11-12')
        WHEN id % 4 = 2 THEN date('2024-11-01')
        ELSE date('2025-10-20')
    END as purchase_date,

    CASE 
        WHEN id % 4 = 0 THEN 2022
        WHEN id % 4 = 1 THEN 2023
        WHEN id % 4 = 2 THEN 2024
        ELSE 2025
    END as purchase_year,

    CASE 
        WHEN id % 2 = 0 THEN 10
        ELSE 11
    END as purchase_month,

    'Q4' as purchase_quarter,

    'Diwali' as purchase_event,

    ROUND((id % 30) + 5,2) as discount_percentage,

    CASE 
        WHEN id % 3 = 0 THEN 12
        WHEN id % 3 = 1 THEN 24
        ELSE 36
    END as policy_duration_months,

    ROUND(5000 + (id * 25),2) as premium_amount

FROM range(1,1001);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT purchase_year,
COUNT(*) AS total_policies,
SUM(premium_amount) AS total_revenue
FROM insurance.insurance_policy_sales
WHERE purchase_event = 'Diwali'
GROUP BY purchase_year
ORDER BY purchase_year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT
    SUM(CASE WHEN purchase_year = 2025 THEN premium_amount END) AS sales_2025,
    SUM(CASE WHEN purchase_year = 2024 THEN premium_amount END) AS sales_2024,
    SUM(CASE WHEN purchase_year = 2025 THEN premium_amount END) -
    SUM(CASE WHEN purchase_year = 2024 THEN premium_amount END) AS sales_difference
FROM insurance.insurance_policy_sales
WHERE purchase_event = 'Diwali'
AND purchase_year IN (2024, 2025);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import random
-- MAGIC from datetime import datetime, timedelta
-- MAGIC 
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC 
-- MAGIC # Sample lists
-- MAGIC cities = ["Mumbai","Delhi","Bangalore","Hyderabad","Chennai","Pune","Kolkata"]
-- MAGIC regions = ["North","South","West","East"]
-- MAGIC products = ["Health Insurance","Life Insurance","Motor Insurance","Travel Insurance"]
-- MAGIC agents = ["Amit Sharma","Priya Patel","Rahul Verma","Sneha Reddy","Karan Mehta","Neha Singh"]
-- MAGIC branches = ["Mumbai Branch","Delhi Branch","Bangalore Branch","Hyderabad Branch"]
-- MAGIC payment_modes = ["Monthly","Quarterly","Yearly"]
-- MAGIC claim_status = ["No Claim","Claimed","Rejected"]
-- MAGIC policy_status = ["Active","Lapsed"]
-- MAGIC 
-- MAGIC rows = []
-- MAGIC 
-- MAGIC for i in range(10000):
-- MAGIC 
-- MAGIC     purchase_date = datetime(2023,1,1) + timedelta(days=random.randint(0,900))
-- MAGIC     
-- MAGIC     premium = random.randint(5000,100000)
-- MAGIC     
-- MAGIC     claim = random.choice([0, random.randint(1000,50000)])
-- MAGIC 
-- MAGIC     rows.append((
-- MAGIC         f"P{i}",
-- MAGIC         f"CUST{i}",
-- MAGIC         f"Customer_{i}",
-- MAGIC         random.randint(22,65),
-- MAGIC         random.choice(cities),
-- MAGIC         random.choice(regions),
-- MAGIC         random.choice(products),
-- MAGIC         purchase_date.date(),
-- MAGIC         premium,
-- MAGIC         random.choice([5,10,15,20]),
-- MAGIC         random.choice(payment_modes),
-- MAGIC         f"A{random.randint(100,200)}",
-- MAGIC         random.choice(agents),
-- MAGIC         random.choice(branches),
-- MAGIC         random.choice(claim_status),
-- MAGIC         claim,
-- MAGIC         premium * 0.1,
-- MAGIC         random.choice(policy_status),
-- MAGIC         purchase_date.year,
-- MAGIC         "Q"+str((purchase_date.month-1)//3+1)
-- MAGIC     ))
-- MAGIC 
-- MAGIC columns = [
-- MAGIC "policy_id","customer_id","customer_name","age","city","region",
-- MAGIC "product_type","purchase_date","premium_amount","policy_term_years",
-- MAGIC "payment_mode","agent_id","agent_name","branch","claim_status",
-- MAGIC "claim_amount","commission","policy_status","purchase_year","purchase_quarter"
-- MAGIC ]
-- MAGIC 
-- MAGIC df = spark.createDataFrame(rows, columns)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df.write.format("delta") \
-- MAGIC .mode("overwrite") \
-- MAGIC .saveAsTable("dbo.insurance_sales_transactions")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
