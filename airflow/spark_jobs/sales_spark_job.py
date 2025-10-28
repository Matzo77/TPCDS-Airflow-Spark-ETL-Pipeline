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
            .appName("sales-spark-job") \
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

        files_dict = {f.split("/")[-1].split(".")[0][:-11]: f for f in file_urls}

        # --- Load CSVs (Raw Tables) ---

        catalog_returns_schema = StructType([
            StructField("cr_returned_date_sk", DoubleType(), True),
            StructField("cr_returned_time_sk", DoubleType(), True),
            StructField("cr_item_sk", DoubleType(), True),
            StructField("cr_refunded_customer_sk", DoubleType(), True),
            StructField("cr_refunded_cdemo_sk", DoubleType(), True),
            StructField("cr_refunded_hdemo_sk", DoubleType(), True),
            StructField("cr_refunded_addr_sk", DoubleType(), True),
            StructField("cr_returning_customer_sk", DoubleType(), True),
            StructField("cr_returning_cdemo_sk", DoubleType(), True),
            StructField("cr_returning_hdemo_sk", DoubleType(), True),
            StructField("cr_returning_addr_sk", DoubleType(), True),
            StructField("cr_call_center_sk", DoubleType(), True),
            StructField("cr_catalog_page_sk", DoubleType(), True),
            StructField("cr_ship_mode_sk", DoubleType(), True),
            StructField("cr_warehouse_sk", DoubleType(), True),
            StructField("cr_reason_sk", DoubleType(), True),
            StructField("cr_order_number", DoubleType(), True),
            StructField("cr_return_quantity", DoubleType(), True),
            StructField("cr_return_amount", DoubleType(), True),
            StructField("cr_return_tax", DoubleType(), True),
            StructField("cr_return_amt_inc_tax", DoubleType(), True),
            StructField("cr_fee", DoubleType(), True),
            StructField("cr_return_ship_cost", DoubleType(), True),
            StructField("cr_refunded_cash", DoubleType(), True),
            StructField("cr_reversed_charge", DoubleType(), True),
            StructField("cr_store_credit", DoubleType(), True),
            StructField("cr_net_loss", DoubleType(), True),
        ])

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

        inventory_schema = StructType([
            StructField("inv_date_sk", IntegerType(), True),
            StructField("inv_item_sk", IntegerType(), True),
            StructField("inv_warehouse_sk", IntegerType(), True),
            StructField("inv_quantity_on_hand", DoubleType(), True),
        ])

        item_schema = StructType([
            StructField("i_item_sk", IntegerType(), True),
            StructField("i_item_id", StringType(), True),
            StructField("i_rec_start_date", DateType(), True),
            StructField("i_rec_end_date", DateType(), True),
            StructField("i_item_desc", StringType(), True),
            StructField("i_current_price", DoubleType(), True),
            StructField("i_wholesale_cost", DoubleType(), True),
            StructField("i_brand_id", DoubleType(), True),
            StructField("i_brand", StringType(), True),
            StructField("i_class_id", DoubleType(), True),
            StructField("i_class", StringType(), True),
            StructField("i_category_id", DoubleType(), True),
            StructField("i_category", StringType(), True),
            StructField("i_manufact_id", DoubleType(), True),
            StructField("i_manufact", StringType(), True),
            StructField("i_size", StringType(), True),
            StructField("i_formulation", StringType(), True),
            StructField("i_color", StringType(), True),
            StructField("i_units", StringType(), True),
            StructField("i_container", StringType(), True),
            StructField("i_manager_id", DoubleType(), True),
            StructField("i_product_name", StringType(), True),
        ])

        store_returns_schema = StructType([
            StructField("sr_returned_date_sk", DoubleType(), True),
            StructField("sr_return_time_sk", DoubleType(), True),
            StructField("sr_item_sk", DoubleType(), True),
            StructField("sr_customer_sk", DoubleType(), True),
            StructField("sr_cdemo_sk", DoubleType(), True),
            StructField("sr_hdemo_sk", DoubleType(), True),
            StructField("sr_addr_sk", DoubleType(), True),
            StructField("sr_store_sk", DoubleType(), True),
            StructField("sr_reason_sk", DoubleType(), True),
            StructField("sr_ticket_number", DoubleType(), True),
            StructField("sr_return_quantity", DoubleType(), True),
            StructField("sr_return_amt", DoubleType(), True),
            StructField("sr_return_tax", DoubleType(), True),
            StructField("sr_return_amt_inc_tax", DoubleType(), True),
            StructField("sr_fee", DoubleType(), True),
            StructField("sr_return_ship_cost", DoubleType(), True),
            StructField("sr_refunded_cash", DoubleType(), True),
            StructField("sr_reversed_charge", DoubleType(), True),
            StructField("sr_store_credit", DoubleType(), True),
            StructField("sr_net_loss", DoubleType(), True),
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

        web_returns_schema = StructType([
            StructField("wr_returned_date_sk", DoubleType(), True),
            StructField("wr_returned_time_sk", DoubleType(), True),
            StructField("wr_item_sk", DoubleType(), True),
            StructField("wr_refunded_customer_sk", DoubleType(), True),
            StructField("wr_refunded_cdemo_sk", DoubleType(), True),
            StructField("wr_refunded_hdemo_sk", DoubleType(), True),
            StructField("wr_refunded_addr_sk", DoubleType(), True),
            StructField("wr_returning_customer_sk", DoubleType(), True),
            StructField("wr_returning_cdemo_sk", DoubleType(), True),
            StructField("wr_returning_hdemo_sk", DoubleType(), True),
            StructField("wr_returning_addr_sk", DoubleType(), True),
            StructField("wr_web_page_sk", DoubleType(), True),
            StructField("wr_reason_sk", DoubleType(), True),
            StructField("wr_order_number", DoubleType(), True),
            StructField("wr_return_quantity", DoubleType(), True),
            StructField("wr_return_amt", DoubleType(), True),
            StructField("wr_return_tax", DoubleType(), True),
            StructField("wr_return_amt_inc_tax", DoubleType(), True),
            StructField("wr_fee", DoubleType(), True),
            StructField("wr_return_ship_cost", DoubleType(), True),
            StructField("wr_refunded_cash", DoubleType(), True),
            StructField("wr_reversed_charge", DoubleType(), True),
            StructField("wr_account_credit", DoubleType(), True),
            StructField("wr_net_loss", DoubleType(), True),
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

        catalog_returns = spark.read.csv(files_dict["catalog_returns"], header=True, schema=catalog_returns_schema)
        catalog_sales = spark.read.csv(files_dict["catalog_sales"], header=True, schema=catalog_sales_schema)
        customer_address = spark.read.csv(files_dict["customer_address"], header=True, schema=customer_address_schema)
        customer = spark.read.csv(files_dict["customer"], header=True, schema=customer_schema)
        customer_demographics = spark.read.csv(files_dict["customer_demographics"], header=True, schema=customer_demographics_schema)
        date_dim_raw = spark.read.csv(files_dict["date_dim"], header=True, schema=date_dim_schema)
        household_demographics = spark.read.csv(files_dict["household_demographics"], header=True, schema=household_demographics_schema)
        income_band = spark.read.csv(files_dict["income_band"], header=True, schema=income_band_schema)
        inventory = spark.read.csv(files_dict["inventory"], header=True, schema=inventory_schema)
        item = spark.read.csv(files_dict["item"], header=True, schema=item_schema)
        store_returns = spark.read.csv(files_dict["store_returns"], header=True, schema=store_returns_schema)
        store_sales = spark.read.csv(files_dict["store_sales"], header=True, schema=store_sales_schema)
        warehouse = spark.read.csv(files_dict["warehouse"], header=True, schema=warehouse_schema)
        web_returns = spark.read.csv(files_dict["web_returns"], header=True, schema=web_returns_schema)
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

        # --- Sales fact table
        # --- Union of web and catalog sales channels ---

        combined_daily_sales = (
            catalog_sales
            .filter(
                (col("cs_quantity").isNotNull()) &
                ((col("cs_quantity") * col("cs_sales_price")).isNotNull()) &
                (col("cs_sold_date_sk").isNotNull()) &
                (col("cs_warehouse_sk").isNotNull()) &
                (col("cs_quantity") > 0) &
                ((col("cs_quantity") * col("cs_sales_price")) > 0) &
                (col("cs_bill_customer_sk").isNotNull())
            )
            .select(
                col("cs_warehouse_sk").alias("warehouse_sk"),
                col("cs_item_sk").alias("item_sk"),
                col("cs_sold_date_sk").alias("sold_date_sk"),
                col("cs_quantity").alias("daily_qty"),
                (col("cs_quantity") * col("cs_sales_price")).alias("daily_sales_amt"),
                col("cs_net_profit").alias("daily_net_profit"),
            )
            .unionAll(
                web_sales
                .filter(
                    (col("ws_quantity").isNotNull()) &
                    ((col("ws_quantity") * col("ws_sales_price")).isNotNull()) &
                    (col("ws_sold_date_sk").isNotNull()) &
                    (col("ws_warehouse_sk").isNotNull()) &
                    (col("ws_quantity") > 0) &
                    ((col("ws_quantity") * col("ws_sales_price")) > 0) &
                    (col("ws_bill_customer_sk").isNotNull())
                )
                .select(
                    col("ws_warehouse_sk").alias("warehouse_sk"),
                    col("ws_item_sk").alias("item_sk"),
                    col("ws_sold_date_sk").alias("sold_date_sk"),
                    col("ws_quantity").alias("daily_qty"),
                    (col("ws_quantity") * col("ws_sales_price")).alias("daily_sales_amt"),
                    col("ws_net_profit").alias("daily_net_profit"),
                )
            )
        )

        # --- Adding yr_wk_num so the dataframe can be joined with inventory dataframe

        agg_daily_sales_with_date = (
            combined_daily_sales
                .groupBy("warehouse_sk","item_sk","sold_date_sk")
                    .agg(
                        sum("daily_qty").alias("daily_qty"),
                        sum("daily_sales_amt").alias("daily_sales_amt"),
                        sum("daily_net_profit").alias("daily_net_profit")
                    )
                    .join(date_dim.select("date_sk","date","week_start_date","week_end_date","yr_wk_num"), combined_daily_sales["sold_date_sk"] == date_dim["date_sk"])
                        .drop("date_sk")
        )

        # --- Adding yr_wk_num so the inventory dataframe

        inventory_with_date = (
            inventory
                .join(date_dim.select("date_sk","date","yr_wk_num"),inventory["inv_date_sk"] == date_dim["date_sk"])
                .drop("date_sk")
                .withColumnsRenamed({"inv_date_sk":"date_sk",
                                    "inv_item_sk":"item_sk",
                                    "inv_warehouse_sk":"warehouse_sk",
                                    "inv_quantity_on_hand":"quantity_on_hand",
                                    })
        )

        fact_sales_inv_weekly = (
            agg_daily_sales_with_date
                .join(inventory_with_date,["yr_wk_num", "warehouse_sk", "item_sk"], "inner")
                .groupBy("yr_wk_num", "warehouse_sk", "item_sk","week_start_date", "week_end_date")
                .agg(
                    sum("daily_qty").alias("sum_qty_wk"),
                    sum("daily_sales_amt").alias("sum_amt_wk"),
                    sum("daily_net_profit").alias("sum_profit_wk"),
                    coalesce(sum("quantity_on_hand"), lit(0)).alias("inv_on_hand_qty_wk")
                )
                .withColumn("avg_qty_dy", col("sum_qty_wk") / lit(7))
                .withColumn(
                    "wks_sply",
                    when(col("sum_qty_wk") > 0,
                        col("inv_on_hand_qty_wk") / col("sum_qty_wk"))
                    .otherwise(lit(None))
                )
                .withColumn(
                    "low_stock_flg_wk",
                    (col("avg_qty_dy") > 0) & (col("avg_qty_dy") > col("inv_on_hand_qty_wk"))
                )
            .orderBy("yr_wk_num", "warehouse_sk", "item_sk")
        )

        # --- Customer dimension

        dim_customer_sales = (
            customer
                .join(customer_address, customer['c_current_addr_sk'] == customer_address['ca_address_sk'])
                .join(customer_demographics, customer['c_current_cdemo_sk'] == customer_demographics['cd_demo_sk'])
                .join(household_demographics, customer['c_current_hdemo_sk'] == household_demographics['hd_demo_sk'])
                .join(income_band, household_demographics['hd_income_band_sk'] == income_band['ib_income_band_sk'])
                .select(
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
                    col('ib_upper_bound').alias('income_upper_bound')
                )   
        )      

        # --- Item dimension

        dim_item_sales = item.select(
            col('i_item_sk').alias('item_sk'),
            col('i_item_id').alias('item_id'),
            col('i_rec_start_date').alias('rec_start_date'),
            col('i_rec_end_date').alias('rec_end_date'),
            col('i_item_desc').alias('item_desc'),
            col('i_current_price').alias('current_price'),
            col('i_wholesale_cost').alias('wholesale_cost'),
            col('i_brand_id').alias('brand_id'),
            col('i_brand').alias('brand'),
            col('i_class_id').alias('class_id'),
            col('i_class').alias('class'),
            col('i_category_id').alias('category_id'),
            col('i_category').alias('category'),
            col('i_manufact_id').alias('manufact_id'),
            col('i_manufact').alias('manufact'),
            col('i_size').alias('size'),
            col('i_formulation').alias('formulation'),
            col('i_color').alias('color'),
            col('i_units').alias('units'),
            col('i_container').alias('container'),
            col('i_manager_id').alias('manager_id'),
            col('i_product_name').alias('product_name')
        )

        # --- Warehouse dimension

        dim_warehouse_sales = warehouse.select(
            col('w_warehouse_sk').alias('warehouse_sk'),
            col('w_warehouse_id').alias('warehouse_id'),
            col('w_warehouse_name').alias('warehouse_name'),
            col('w_warehouse_sq_ft').alias('warehouse_sq_ft'),
            col('w_street_number').alias('street_number'),
            col('w_street_name').alias('street_name'),
            col('w_street_type').alias('street_type'),
            col('w_suite_number').alias('suite_number'),
            col('w_city').alias('city'),
            col('w_county').alias('county'),
            col('w_state').alias('state'),
            col('w_zip').alias('zip'),
            col('w_country').alias('country'),
            col('w_gmt_offset').alias('gmt_offset')
        )

        # --- Returns Fact Table
        # --- Combine catalog and web returns ---

        combined_returns = (
            catalog_returns
            .filter(
                (col("cr_returned_date_sk").isNotNull()) &
                (col("cr_item_sk").isNotNull()) &
                (col("cr_return_quantity") > 0) &
                (col("cr_return_amount") > 0)
            )
            .select(
                col("cr_returned_date_sk").alias("returned_date_sk"),
                col("cr_item_sk").alias("item_sk"),
                col("cr_return_quantity").alias("return_qty"),
                col("cr_return_amount").alias("return_amt"),
                col("cr_net_loss").alias("return_loss")
            )
            .unionAll(
                web_returns
                .filter(
                    (col("wr_returned_date_sk").isNotNull()) &
                    (col("wr_item_sk").isNotNull()) &
                    (col("wr_return_quantity") > 0) &
                    (col("wr_return_amt") > 0)
                )
                .select(
                    col("wr_returned_date_sk").alias("returned_date_sk"),
                    col("wr_item_sk").alias("item_sk"),
                    col("wr_return_quantity").alias("return_qty"),
                    col("wr_return_amt").alias("return_amt"),
                    col("wr_net_loss").alias("return_loss")
                )
            )
        )

        # --- 2. Join with date_dim to get week boundaries ---

        returns_with_date = (
            combined_returns
            .join(
                date_dim.select("date_sk", "yr_wk_num", "week_start_date", "week_end_date"),
                col("returned_date_sk") == col("date_sk"),
                "left"
            )
            .drop("date_sk")
        )

        # --- 3. Aggregate weekly returns ---

        agg_returns_weekly = (
            returns_with_date
            .groupBy("yr_wk_num", "item_sk", "week_start_date", "week_end_date")
            .agg(
                sum("return_qty").alias("sum_return_qty_wk"),
                sum("return_amt").alias("sum_return_amt_wk"),
                sum("return_loss").alias("sum_return_loss_wk")
            )
        )

        # --- 4. Compute daily average returns and high-return flag ---
        # Example threshold: high return if weekly return amount > 25% of weekly sales
        # Join weekly returns with weekly sales

        sales_no_inventory = (
            fact_sales_inv_weekly.groupBy("yr_wk_num","item_sk","week_start_date","week_end_date")
                .agg(
                    sum("sum_qty_wk").alias("sum_qty_wk"),
                    sum("sum_amt_wk").alias("sum_amt_wk"),
                    sum("sum_profit_wk").alias("sum_profit_wk")
                )
                .select(
                    "yr_wk_num",
                    "item_sk",
                    "week_start_date",
                    "week_end_date",
                    "sum_qty_wk",
                    "sum_amt_wk",
                    "sum_profit_wk"
                )
        )

        fact_returns_with_sales = (
            sales_no_inventory.alias("s")
            .join(
                agg_returns_weekly.alias("r"),
                on=[
                    col("r.yr_wk_num") == col("s.yr_wk_num"),
                    col("r.item_sk") == col("s.item_sk")
                ],
                how="left"
            )
            .select(
                col("s.yr_wk_num"),
                col("s.item_sk"),
                col("s.week_start_date"),
                col("s.week_end_date"),
                col("r.sum_return_qty_wk"),
                col("r.sum_return_amt_wk"),
                col("r.sum_return_loss_wk"),
                col("s.sum_amt_wk").alias("sum_sales_amt_wk"),
                col("s.sum_qty_wk").alias("sum_sales_qty_wk")
            )
            # Compute return metrics
            .withColumn("avg_return_qty_dy", col("sum_return_qty_wk") / lit(7))
            .withColumn(
                "return_rate_amt",
                when(col("sum_sales_amt_wk") > 0,
                    round((col("sum_return_amt_wk") / col("sum_sales_amt_wk")) * 100, 2))
                .otherwise(lit(0))
            )
            .withColumn(
                "return_rate_qty",
                when(col("sum_sales_qty_wk") > 0,
                    round((col("sum_return_qty_wk") / col("sum_sales_qty_wk")) * 100, 2))
                .otherwise(lit(0))
            )
            # Flag items with high returns (e.g., >25% of weekly sales)
            .withColumn(
                "high_return_flag_wk",
                when(col("return_rate_amt") > 25, lit(True)).otherwise(lit(False))
            )
            .orderBy("yr_wk_num", "item_sk")
        )

        # Write each DataFrame as Parquet
        fact_sales_inv_weekly.write.mode("overwrite").parquet(f"{output_base}/fact_sales_inv_weekly")
        dim_customer_sales.write.mode("overwrite").parquet(f"{output_base}/dim_customer_sales")
        dim_item_sales.write.mode("overwrite").parquet(f"{output_base}/dim_item_sales")
        dim_warehouse_sales.write.mode("overwrite").parquet(f"{output_base}/dim_warehouse_sales")
        date_dim.write.mode("overwrite").parquet(f"{output_base}/dim_date")
        fact_returns_with_sales.write.mode("overwrite").parquet(f"{output_base}/fact_returns_with_sales")
    
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
    output_base = "s3a://portfolio-project/analytics/sales"

    transform_and_save_to_storage(file_urls, minio_endpoint, minio_access_key, minio_secret_key, output_base)