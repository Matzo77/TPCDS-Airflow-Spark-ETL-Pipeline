# --------------------------------------------------------
# Initialization, Configuration, and Raw Data Loads
# --------------------------------------------------------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import ast
import traceback
from pyspark.sql.window import Window
import yaml
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def transform_and_save_to_storage(file_urls: list, minio_endpoint, minio_access_key, minio_secret_key,output_base):
    
    try:
        # --- Spark Session ---

        spark = SparkSession.builder \
            .appName("marketing-spark-job") \
            .getOrCreate()

        # --- Hadoop FS Configuration ---

        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", minio_endpoint)
        hadoop_conf.set("fs.s3a.access.key", minio_access_key)
        hadoop_conf.set("fs.s3a.secret.key", minio_secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Map file names to variables

        files_dict = {f.split('/')[-1].split(".")[0][:-11]: f for f in file_urls}

        # --- Load CSVs (Raw Tables) ---

        catalog_sales_schema = StructType([
            StructField("cs_sold_date_sk", DoubleType(), True),
            StructField("cs_sold_time_sk", DoubleType(), True),
            StructField("cs_ship_date_sk", DoubleType(), True),
            StructField("cs_bill_customer_sk", DoubleType(), True),
            StructField("cs_bill_cdemo_sk", DoubleType(), True),
            StructField("cs_bill_hdemo_sk", DoubleType(), True),
            StructField("cs_bill_addr_sk", DoubleType(), True),
            StructField("cs_ship_customer_sk", DoubleType(), True),
            StructField("cs_ship_cdemo_sk", DoubleType(), True),
            StructField("cs_ship_hdemo_sk", DoubleType(), True),
            StructField("cs_ship_addr_sk", DoubleType(), True),
            StructField("cs_call_center_sk", DoubleType(), True),
            StructField("cs_catalog_page_sk", DoubleType(), True),
            StructField("cs_ship_mode_sk", DoubleType(), True),
            StructField("cs_warehouse_sk", DoubleType(), True),
            StructField("cs_item_sk", DoubleType(), True),
            StructField("cs_promo_sk", DoubleType(), True),
            StructField("cs_order_number", IntegerType(), True),
            StructField("cs_quantity", DoubleType(), True),
            StructField("cs_wholesale_cost", DoubleType(), True),
            StructField("cs_list_price", DoubleType(), True),
            StructField("cs_sales_price", DoubleType(), True),
            StructField("cs_ext_discount_amt", DoubleType(), True),
            StructField("cs_ext_sales_price", DoubleType(), True),
            StructField("cs_ext_wholesale_cost", DoubleType(), True),
            StructField("cs_ext_list_price", DoubleType(), True),
            StructField("cs_ext_tax", DoubleType(), True),
            StructField("cs_coupon_amt", DoubleType(), True),
            StructField("cs_ext_ship_cost", DoubleType(), True),
            StructField("cs_net_paid", DoubleType(), True),
            StructField("cs_net_paid_inc_tax", DoubleType(), True),
            StructField("cs_net_paid_inc_ship", DoubleType(), True),
            StructField("cs_net_paid_inc_ship_tax", DoubleType(), True),
            StructField("cs_net_profit", DoubleType(), True),
        ])

        customer_address_schema = StructType([
            StructField("ca_address_sk", IntegerType(), True),
            StructField("ca_address_id", StringType(), True),
            StructField("ca_street_number", StringType(), True),
            StructField("ca_street_name", StringType(), True),
            StructField("ca_street_type", StringType(), True),
            StructField("ca_suite_number", StringType(), True),
            StructField("ca_city", StringType(), True),
            StructField("ca_county", StringType(), True),
            StructField("ca_state", StringType(), True),
            StructField("ca_zip", StringType(), True),
            StructField("ca_country", StringType(), True),
            StructField("ca_gmt_offset", DoubleType(), True),
            StructField("ca_location_type", StringType(), True),
        ])

        customer_schema = StructType([
            StructField("c_customer_sk", IntegerType(), True),
            StructField("c_customer_id", StringType(), True),
            StructField("c_current_cdemo_sk", DoubleType(), True),
            StructField("c_current_hdemo_sk", DoubleType(), True),
            StructField("c_current_addr_sk", DoubleType(), True),
            StructField("c_first_shipto_date_sk", DoubleType(), True),
            StructField("c_first_sales_date_sk", DoubleType(), True),
            StructField("c_salutation", StringType(), True),
            StructField("c_first_name", StringType(), True),
            StructField("c_last_name", StringType(), True),
            StructField("c_preferred_cust_flag", StringType(), True),
            StructField("c_birth_day", StringType(), True),
            StructField("c_birth_month", StringType(), True),
            StructField("c_birth_year", StringType(), True),
            StructField("c_birth_country", StringType(), True),
            StructField("c_login", StringType(), True),
            StructField("c_email_address", StringType(), True),
            StructField("c_last_review_date", StringType(), True),
        ])

        customer_demographics_schema = StructType([
            StructField("cd_demo_sk", IntegerType(), True),
            StructField("cd_gender", StringType(), True),
            StructField("cd_marital_status", StringType(), True),
            StructField("cd_education_status", StringType(), True),
            StructField("cd_purchase_estimate", DoubleType(), True),
            StructField("cd_credit_rating", StringType(), True),
            StructField("cd_dep_count", DoubleType(), True),
            StructField("cd_dep_employed_count", DoubleType(), True),
            StructField("cd_dep_college_count", DoubleType(), True),
        ])

        date_dim_schema = StructType([
            StructField("d_date_sk", IntegerType(), True),
            StructField("d_date_id", StringType(), True),
            StructField("d_date", DateType(), True),
            StructField("d_month_seq", IntegerType(), True),
            StructField("d_week_seq", IntegerType(), True),
            StructField("d_quarter_seq", IntegerType(), True),
            StructField("d_year", IntegerType(), True),
            StructField("d_dow", IntegerType(), True),
            StructField("d_moy", IntegerType(), True),
            StructField("d_dom", IntegerType(), True),
            StructField("d_qoy", IntegerType(), True),
            StructField("d_fy_year", IntegerType(), True),
            StructField("d_fy_quarter_seq", IntegerType(), True),
            StructField("d_fy_week_seq", IntegerType(), True),
            StructField("d_day_name", StringType(), True),
            StructField("d_quarter_name", StringType(), True),
            StructField("d_holiday", StringType(), True),
            StructField("d_weekend", StringType(), True),
            StructField("d_following_holiday", StringType(), True),
            StructField("d_first_dom", IntegerType(), True),
            StructField("d_last_dom", IntegerType(), True),
            StructField("d_same_day_ly", IntegerType(), True),
            StructField("d_same_day_lq", IntegerType(), True),
            StructField("d_current_day", StringType(), True),
            StructField("d_current_week", StringType(), True),
            StructField("d_current_month", StringType(), True),
            StructField("d_current_quarter", StringType(), True),
            StructField("d_current_year", StringType(), True),
        ])

        household_demographics_schema = StructType([
            StructField("hd_demo_sk", IntegerType(), True),
            StructField("hd_income_band_sk", DoubleType(), True),
            StructField("hd_buy_potential", StringType(), True),
            StructField("hd_dep_count", IntegerType(), True),
            StructField("hd_vehicle_count", IntegerType(), True),
        ])

        income_band_schema = StructType([
            StructField("ib_income_band_sk", IntegerType(), True),
            StructField("ib_lower_bound", IntegerType(), True),
            StructField("ib_upper_bound", IntegerType(), True),
        ])

        store_sales_schema = StructType([
            StructField("ss_sold_date_sk", DoubleType(), True),
            StructField("ss_sold_time_sk", DoubleType(), True),
            StructField("ss_item_sk", DoubleType(), True),
            StructField("ss_customer_sk", DoubleType(), True),
            StructField("ss_cdemo_sk", DoubleType(), True),
            StructField("ss_hdemo_sk", DoubleType(), True),
            StructField("ss_addr_sk", DoubleType(), True),
            StructField("ss_store_sk", DoubleType(), True),
            StructField("ss_promo_sk", DoubleType(), True),
            StructField("ss_ticket_number", DoubleType(), True),
            StructField("ss_quantity", DoubleType(), True),
            StructField("ss_wholesale_cost", DoubleType(), True),
            StructField("ss_list_price", DoubleType(), True),
            StructField("ss_sales_price", DoubleType(), True),
            StructField("ss_ext_discount_amt", DoubleType(), True),
            StructField("ss_ext_sales_price", DoubleType(), True),
            StructField("ss_ext_wholesale_cost", DoubleType(), True),
            StructField("ss_ext_list_price", DoubleType(), True),
            StructField("ss_ext_tax", DoubleType(), True),
            StructField("ss_coupon_amt", DoubleType(), True),
            StructField("ss_net_paid", DoubleType(), True),
            StructField("ss_net_paid_inc_tax", DoubleType(), True),
            StructField("ss_net_profit", DoubleType(), True),
        ])

        warehouse_schema = StructType([
            StructField("w_warehouse_sk", IntegerType(), True),
            StructField("w_warehouse_id", StringType(), True),
            StructField("w_warehouse_name", StringType(), True),
            StructField("w_warehouse_sq_ft", DoubleType(), True),
            StructField("w_street_number", StringType(), True),
            StructField("w_street_name", StringType(), True),
            StructField("w_street_type", StringType(), True),
            StructField("w_suite_number", StringType(), True),
            StructField("w_city", StringType(), True),
            StructField("w_county", StringType(), True),
            StructField("w_state", StringType(), True),
            StructField("w_zip", DoubleType(), True),
            StructField("w_country", StringType(), True),
            StructField("w_gmt_offset", DoubleType(), True),
        ])

        web_sales_schema = StructType([
            StructField("ws_sold_date_sk", DoubleType(), True),
            StructField("ws_sold_time_sk", DoubleType(), True),
            StructField("ws_ship_date_sk", DoubleType(), True),
            StructField("ws_item_sk", DoubleType(), True),
            StructField("ws_bill_customer_sk", DoubleType(), True),
            StructField("ws_bill_cdemo_sk", DoubleType(), True),
            StructField("ws_bill_hdemo_sk", DoubleType(), True),
            StructField("ws_bill_addr_sk", DoubleType(), True),
            StructField("ws_ship_customer_sk", DoubleType(), True),
            StructField("ws_ship_cdemo_sk", DoubleType(), True),
            StructField("ws_ship_hdemo_sk", DoubleType(), True),
            StructField("ws_ship_addr_sk", DoubleType(), True),
            StructField("ws_web_page_sk", DoubleType(), True),
            StructField("ws_web_site_sk", DoubleType(), True),
            StructField("ws_ship_mode_sk", DoubleType(), True),
            StructField("ws_warehouse_sk", DoubleType(), True),
            StructField("ws_promo_sk", DoubleType(), True),
            StructField("ws_order_number", DoubleType(), True),
            StructField("ws_quantity", DoubleType(), True),
            StructField("ws_wholesale_cost", DoubleType(), True),
            StructField("ws_list_price", DoubleType(), True),
            StructField("ws_sales_price", DoubleType(), True),
            StructField("ws_ext_discount_amt", DoubleType(), True),
            StructField("ws_ext_sales_price", DoubleType(), True),
            StructField("ws_ext_wholesale_cost", DoubleType(), True),
            StructField("ws_ext_list_price", DoubleType(), True),
            StructField("ws_ext_tax", DoubleType(), True),
            StructField("ws_coupon_amt", DoubleType(), True),
            StructField("ws_ext_ship_cost", DoubleType(), True),
            StructField("ws_net_paid", DoubleType(), True),
            StructField("ws_net_paid_inc_tax", DoubleType(), True),
            StructField("ws_net_paid_inc_ship", DoubleType(), True),
            StructField("ws_net_paid_inc_ship_tax", DoubleType(), True),
            StructField("ws_net_profit", DoubleType(), True),
        ])

        catalog_sales = spark.read.csv(files_dict["catalog_sales"], header=True, schema=catalog_sales_schema)
        customer_address = spark.read.csv(files_dict["customer_address"], header=True, schema=customer_address_schema)
        customer = spark.read.csv(files_dict["customer"], header=True, schema=customer_schema)
        customer_demographics = spark.read.csv(files_dict["customer_demographics"], header=True, schema=customer_demographics_schema)
        date_dim_raw = spark.read.csv(files_dict["date_dim"], header=True, schema=date_dim_schema)
        household_demographics = spark.read.csv(files_dict["household_demographics"], header=True, schema=household_demographics_schema)
        income_band = spark.read.csv(files_dict["income_band"], header=True, schema=income_band_schema)
        store_sales = spark.read.csv(files_dict["store_sales"], header=True, schema=store_sales_schema)
        web_sales = spark.read.csv(files_dict["web_sales"], header=True, schema=web_sales_schema)

        # --- Clean and transform date_dim ---

        window_spec = Window.partitionBy("yr_wk_num")

        date_dim = (
            date_dim_raw
            # compute week number so week starts on Sunday
            .withColumn(
                "week_of_year",
                lpad(
                    weekofyear(date_sub("d_date", 1)).cast("int"),
                    2,
                    "0"
                )
            )
            .withColumn(
                "yr_wk_num",
                concat(
                    year("d_date"),
                    lpad(weekofyear(date_sub("d_date", 1)).cast("int"), 2, "0")
                )
            )
            # Sunday=1, ..., Saturday=7 → subtract 1 to make Sunday=0
            .withColumn("weekday_index", dayofweek("d_date") - 1)
            # compute week boundaries
            .withColumn("week_start_date", date_sub("d_date", col("weekday_index")))
            .withColumn("week_end_date", date_add("week_start_date", 6))
            .select(
                col("d_date_sk").alias("date_sk"),
                col("d_date").alias("date"),
                col("d_year").alias("year"),
                col("d_moy").alias("month"),
                col("d_dom").alias("day"),
                col("d_day_name").alias("day_name"),
                col("d_qoy").alias("quarter"),
                col("d_quarter_name").alias("quarter_name"),
                col("week_of_year"),
                col("yr_wk_num"),
                col("week_start_date"),
                col("week_end_date")
            )
        )


        # --- Union of all sales channels ---

        incremental_sales = (
            catalog_sales
            .filter(
                (col("cs_quantity").isNotNull()) &
                (col("cs_sales_price").isNotNull()) &
                (col("cs_sold_date_sk").isNotNull()) &
                (col("cs_warehouse_sk").isNotNull()) &
                (col("cs_quantity") > 0) &
                ((col("cs_quantity") * col("cs_sales_price")) > 0) &
                (col("cs_bill_customer_sk").isNotNull())
            )
            .select(
                col("cs_bill_customer_sk").alias("customer_sk"),
                col("cs_item_sk").alias("item_sk"),
                col("cs_promo_sk").alias("promo_sk"),
                col("cs_order_number").alias("order_number"),
                lit("Catalog").alias("channel_key"),
                col("cs_sold_date_sk").alias("sold_date_sk"),
                col("cs_quantity").alias("daily_qty"),
                (col("cs_quantity") * col("cs_sales_price")).alias("daily_sales_amt"),
                col("cs_net_profit").alias("daily_net_profit"),
                col("cs_ext_wholesale_cost").alias("daily_cogs")
            )
            .unionAll(
                web_sales
                .filter(
                    (col("ws_quantity").isNotNull()) &
                    (col("ws_sales_price").isNotNull()) &
                    (col("ws_sold_date_sk").isNotNull()) &
                    (col("ws_warehouse_sk").isNotNull()) &
                    (col("ws_quantity") > 0) &
                    ((col("ws_quantity") * col("ws_sales_price")) > 0) &
                    (col("ws_bill_customer_sk").isNotNull())
                )
                .select(
                    col("ws_bill_customer_sk").alias("customer_sk"),
                    col("ws_item_sk").alias("item_sk"),
                    col("ws_promo_sk").alias("promo_sk"),
                    col("ws_order_number").alias("order_number"),
                    lit("Web").alias("channel_key"),
                    col("ws_sold_date_sk").alias("sold_date_sk"),
                    col("ws_quantity").alias("daily_qty"),
                    (col("ws_quantity") * col("ws_sales_price")).alias("daily_sales_amt"),
                    col("ws_net_profit").alias("daily_net_profit"),
                    col("ws_ext_wholesale_cost").alias("daily_cogs")
                )
            )
            .unionAll(
                store_sales
                .filter(
                    (col("ss_quantity").isNotNull()) &
                    (col("ss_sales_price").isNotNull()) &
                    (col("ss_sold_date_sk").isNotNull()) &
                    (col("ss_store_sk").isNotNull()) &
                    (col("ss_quantity") > 0) &
                    ((col("ss_quantity") * col("ss_sales_price")) > 0) &
                    (col("ss_customer_sk").isNotNull())
                )
                .select(
                    col("ss_customer_sk").alias("customer_sk"),
                    col("ss_item_sk").alias("item_sk"),
                    lit(None).cast("int").alias("promo_sk"),  # store_sales has no promo_sk
                    col("ss_ticket_number").alias("order_number"),
                    lit("Store").alias("channel_key"),
                    col("ss_sold_date_sk").alias("sold_date_sk"),
                    col("ss_quantity").alias("daily_qty"),
                    (col("ss_quantity") * col("ss_sales_price")).alias("daily_sales_amt"),
                    col("ss_net_profit").alias("daily_net_profit"),
                    col("ss_ext_wholesale_cost").alias("daily_cogs")
                ) 
            )
        )


        # --- Join incremental_sales with date_dim to get year/month/week ---

        sales_with_date = (
            incremental_sales
            .join(date_dim, incremental_sales['sold_date_sk'] == date_dim['date_sk'], "left")
            .select(
                col("customer_sk"),
                col("item_sk"),
                col("promo_sk"),
                col("channel_key"),
                col("sold_date_sk"),
                col("date").alias("sold_date"),
                col("year").alias("sold_yr_num"),
                col("month").alias("sold_mnth_num"),
                col("week_of_year").alias("sold_wk_num"),
                col("daily_qty"),
                col("daily_sales_amt"),
                col("daily_net_profit"),
                col("daily_cogs"),
                col("order_number")
            )
        )

        # --- Aggregate by customer, item, promo, channel, and date ---

        customer_item_sales = (
            sales_with_date
            .groupBy(
                "customer_sk",
                "item_sk",
                "promo_sk",
                "channel_key",
                "sold_date_sk",
                "sold_date",
                "sold_yr_num",
                "sold_mnth_num",
                "sold_wk_num"
            )
            .agg(
                sum("daily_qty").alias("daily_qty"),
                sum("daily_sales_amt").alias("daily_sales_amt"),
                sum("daily_net_profit").alias("daily_net_profit"),
                sum("daily_cogs").alias("daily_cogs"),
                count("order_number").alias("daily_orders")
            )
        )

        # --- Creating the RFM model
        # --- Max sold date across all sales channels ---

        max_date = (
            sales_with_date.select(max("sold_date").alias("max_sold_date"))
            .first()["max_sold_date"]
        )

        # --- Build RFM metrics ---
        customer_rfm = (
            sales_with_date
            .groupBy("customer_sk")
            .agg(
                max("sold_date").alias("recent_purchase"),
                count_distinct("order_number").alias("frequency"),
                sum("daily_sales_amt").alias("monetary")
            )
        )

        # --- Calulation to find appropriate values for "recency" ---

        # incremental_sales.groupBy("customer_sk") \
        #     .agg(max("sold_date_sk").alias("last_purchase_sk")) \
        #     .join(date_dim, col("last_purchase_sk") == col("date_sk"), "left") \
        #     .withColumn("days_since_last_purchase", datediff(lit(max_date), col("date"))) \
        #     .select("days_since_last_purchase") \
        #     .approxQuantile("days_since_last_purchase", [0.25, 0.5, 0.75], 0.01)

        # By looking at the result of the above code, we can see that 100 and 300 are appropriate values for recency calculations.

        rfm_segmented = (
            customer_rfm
            .withColumn(
                "recency_segment",
                when(col("recent_purchase") >= date_sub(lit(max_date), 100), "Active")
                .when((col("recent_purchase") < date_sub(lit(max_date), 100)) & (col("recent_purchase") >= date_sub(lit(max_date), 300)), "Lapsing")
                .otherwise("Inactive")
            )
            .withColumn(
                "frequency_segment",
                when(col("frequency") > 5, "High Frequency")
                .when((col("frequency") >= 3) & (col("frequency") <= 5), "Medium Frequency")
                .otherwise("Low Frequency")
            )
            .withColumn(
                "monetary_segment",
                when(col("monetary") > 100000, "High Value")
                .when((col("monetary") >= 50000) & (col("monetary") <= 100000), "Medium Value")
                .otherwise("Low Value")
            )
            .withColumn(
                "customer_segment",
                when(
                    (col("recency_segment") == "Active") &
                    (col("frequency_segment") == "High Frequency") &
                    (col("monetary_segment") == "High Value"), "Champions"
                )
                .when(
                    (col("recency_segment") == "Active") &
                    (col("frequency_segment") == "Medium Frequency") &
                    (col("monetary_segment") == "High Value"), "Champions"
                )
                .when(
                    (col("recency_segment") == "Lapsing") &
                    (col("frequency_segment") == "High Frequency") &
                    (col("monetary_segment") == "High Value"), "Potential Loyalists"
                )
                .when(
                    (col("recency_segment") == "Lapsing") &
                    (col("frequency_segment") == "Medium Frequency") &
                    (col("monetary_segment").isin("Medium Value","High Value")), "Hibernating"
                )
                .when(
                    (col("recency_segment") == "Active") &
                    (col("frequency_segment") == "Low Frequency"), "New User"
                )
                .otherwise("At Risk")
            )
        )

        int_customer_segment = rfm_segmented.select(
            col("customer_sk").alias("segment_customer_sk"),
            "recent_purchase",
            "recency_segment",
            "frequency_segment",
            "monetary_segment",
            "customer_segment"
        )

        # --- CLV ANALYSIS ---

        # --- confirguring the value for churn days

        churn_days = 300

        # %%
        # --- Step 1: seleting the columns we need from sales_with_date for use in CLV calculation

        clv_all_sales = (
            sales_with_date
            .select(
                "customer_sk",
                "order_number",
                "sold_date",
                "daily_sales_amt",
                "daily_net_profit",
                "daily_cogs"
            )
        )

        # --- Step 2: Calculating customer cohorts ---

        customer_cohorts = clv_all_sales.groupBy("customer_sk") \
            .agg(
                min("sold_date").alias("first_purchase_date"),
                max("sold_date").alias("last_purchase_date")
            )

        # --- Step 3: Calculating Churn & Avg Lifespan

        churn_df = customer_cohorts.withColumn(
            "is_churned",
            when(col("last_purchase_date") < date_sub(lit(max_date), churn_days), 1).otherwise(0)
        )

        churn_stats = churn_df.withColumn("cohort_year", year("first_purchase_date")) \
            .groupBy("cohort_year") \
            .agg(
                count("*").alias("total_customers"),
                sum("is_churned").alias("churned_customers")
            ).filter(col("cohort_year") < year(lit(max_date)))

        lifespan_df = churn_stats.withColumn(
            "estimated_lifespan",
            col("total_customers") / when(col("churned_customers") == 0, lit(1)).otherwise(col("churned_customers"))
        )

        avg_lifespan_df = lifespan_df.agg(avg("estimated_lifespan").alias("avg_estimated_lifespan"))

        # --- Step 4: Creating the Customer Summary ---

        customer_summary = clv_all_sales.groupBy("customer_sk") \
            .agg(
                sum("daily_sales_amt").alias("total_sales"),
                sum("daily_net_profit").alias("total_profit"),
                count_distinct("order_number").alias("total_orders"),
                avg("daily_sales_amt").alias("avg_order_value"),
                avg("daily_net_profit").alias("avg_profit_per_order")
            )

        # --- Step 5: Create the Customer CLV dataframe by cross joining with avg lifespan (to keep it distributed)

        customer_clv = customer_summary.crossJoin(avg_lifespan_df) \
            .withColumn(
                "CLV", col("avg_profit_per_order") * col("total_orders") * col("avg_estimated_lifespan")
            ).withColumnRenamed("avg_estimated_lifespan", "estimated_lifespan") \
            .orderBy(desc("CLV"))

        # --- Marketing dept Customer dimension

        dim_customer_marketing = (
            customer
                .join(customer_address, customer['c_current_addr_sk'] == customer_address['ca_address_sk'])
                .join(customer_demographics, customer['c_current_cdemo_sk'] == customer_demographics['cd_demo_sk'])
                .join(household_demographics, customer['c_current_hdemo_sk'] == household_demographics['hd_demo_sk'])
                .join(income_band, household_demographics['hd_income_band_sk'] == income_band['ib_income_band_sk'])
                .join(int_customer_segment, customer['c_customer_sk'] == int_customer_segment['segment_customer_sk'])
                .filter(
                        (col('recency_segment').isNotNull()) &
                        (col('frequency_segment').isNotNull()) &
                        (col('monetary_segment').isNotNull()) &
                        (col('customer_segment').isNotNull())
                ).select(
                    col('c_customer_sk').alias('customer_sk'),
                    col('c_customer_id').alias('customer_id'),
                    col('c_salutation').alias('salutation'),
                    col('c_first_name').alias('first_name'),
                    col('c_last_name').alias('last_name'),
                    col('c_preferred_cust_flag').alias('preferred_cust_flag'),
                    col('c_birth_day').alias('birth_day'),
                    col('c_birth_month').alias('birth_month'),
                    col('c_birth_year').alias('birth_year'),
                    col('c_login').alias('login'),
                    col('c_email_address').alias('email_address'),
                    col('ca_street_number').alias('street_number'),
                    col('ca_street_name').alias('street_name'),
                    col('ca_street_type').alias('street_type'),
                    col('ca_suite_number').alias('suite_number'),
                    col('ca_city').alias('city'),
                    col('ca_county').alias('county'),
                    col('ca_state').alias('state'),
                    col('ca_zip').alias('zip'),
                    col('ca_country').alias('country'),
                    col('ca_gmt_offset').alias('gmt_offset'),
                    col('ca_location_type').alias('location_type'),
                    col('cd_gender').alias('gender'),
                    col('cd_marital_status').alias('marital_status'),
                    col('cd_education_status').alias('education_status'),
                    col('cd_purchase_estimate').alias('purchase_estimate'),
                    col('cd_credit_rating').alias('credit_rating'),
                    col('cd_dep_count').alias('customer_dep_count'),
                    col('cd_dep_employed_count').alias('dep_employed_count'),
                    col('cd_dep_college_count').alias('dep_college_count'),
                    col('hd_buy_potential').alias('buy_potential'),
                    col('hd_dep_count').alias('household_dep_count'),
                    col('hd_vehicle_count').alias('vehicle_count'),
                    col('ib_lower_bound').alias('income_lower_bound'),
                    col('ib_upper_bound').alias('income_upper_bound'),
                    col('recency_segment'),
                    col('frequency_segment'),
                    col('monetary_segment'),
                    col('customer_segment'),
                )                
        )


        # Write each DataFrame as Parquet

        customer_clv.write.mode("overwrite").parquet(f"{output_base}/rpt_clv_marketing")
        dim_customer_marketing.write.mode("overwrite").parquet(f"{output_base}/dim_customer_marketing")
        customer_item_sales.write.mode("overwrite").parquet(f"{output_base}/fact_sales_marketing_daily")

    except Exception as e:
        print("❌ Error creating SparkSession:")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
    
if __name__ == "__main__":
    file_urls = ast.literal_eval(sys.argv[1])
    minio_endpoint = sys.argv[2]
    minio_access_key = sys.argv[3]
    minio_secret_key = sys.argv[4]
    output_base = "s3a://portfolio-project/analytics/marketing"

    transform_and_save_to_storage(file_urls, minio_endpoint, minio_access_key, minio_secret_key, output_base)