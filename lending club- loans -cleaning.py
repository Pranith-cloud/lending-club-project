# Databricks notebook source
loans_raw_df=spark.read\
               .format("csv")\
                .option("header","true")\
                .option("inferSchema","true")\
                .load("dbfs:/user/lendingclub_rawfiles/loans_data_csv")

# COMMAND ----------

loans_raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Changing Datatypes and Renaming Coloumns

# COMMAND ----------

loans_schema='loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string,interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string '

# COMMAND ----------

loans_raw_df=spark.read\
               .format("csv")\
                .option("header","true")\
                .schema(loans_schema)\
                .load("dbfs:/user/lendingclub_rawfiles/loans_data_csv")

# COMMAND ----------

loans_raw_df.display()

# COMMAND ----------

loans_raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingesting Current Time Stamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
loans_df_ingestd= loans_raw_df.withColumn("ingest_date",current_timestamp())

# COMMAND ----------

loans_df_ingestd.display()

# COMMAND ----------

loans_df_ingestd.createOrReplaceTempView("loansdata")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loansdata

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from loansdata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dropping nulls
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)  from loansdata where loan_amount is null 

# COMMAND ----------

columns_check=["loan_amount","funded_amount","loan_term_months","interest_rate","monthly_installment","issue_date",
               "loan_status","loan_purpose"]

# COMMAND ----------

loans_filtered=loans_df_ingestd.na.drop(subset=columns_check)

# COMMAND ----------

loans_filtered.count()

# COMMAND ----------

loans_filtered.createOrReplaceTempView("loansdata")

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Changing Loan_term_months to Loan_term_years using regexpr
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,col
loan_term_modified=loans_filtered\
.withColumn("loan_term_months",(regexp_replace(col("loan_term_months")," months","")\
.cast("int")/12)\
.cast("int"))\
.withColumnRenamed("loan_term_months","loan_term_years")

# COMMAND ----------

loan_term_modified.display()

# COMMAND ----------

loan_term_modified.createOrReplaceTempView("loansdata")

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Cleaning Loan_Purpose

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_purpose, count(*) as total from loansdata group by loan_purpose order by total desc;

# COMMAND ----------

loan_purpose_lookup=["debt_consolidation","credit_card","home_improvement","other","major_purchase","medical",
"small_business","car","vacation","moving","house","wedding","renewable_energy","educational"]

# COMMAND ----------

from pyspark.sql.functions import when
loan_purpose_modified=loan_term_modified.withColumn("loan_purpose",when(col("loan_purpose").isin(loan_purpose_lookup),col("loan_purpose")).otherwise("other"))


# COMMAND ----------

loan_purpose_modified.display()

# COMMAND ----------

loan_purpose_modified.createOrReplaceTempView("loansdata")

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_purpose, count(*) as total from loansdata group by loan_purpose order by total desc;

# COMMAND ----------

##6.Writing Data to File

# COMMAND ----------

loan_purpose_modified.write\
    .option("header","true")\
    .format("parquet")\
    .mode("overwrite")\
    .option("path","dbfs:/user/lendingclubproject/cleaned/loans_parquet")\
    .save()

# COMMAND ----------

loan_purpose_modified.write\
    .option("header","true")\
    .format("csv")\
    .mode("overwrite")\
    .option("path","dbfs:/user/lendingclubproject/cleaned/loans_csv")\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Pranithdb_lending_club.loans
# MAGIC USING PARQUET
# MAGIC LOCATION 'dbfs:/user/lendingclubproject/cleaned/loans_parquet';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Pranithdb_lending_club.loans limit(5)

# COMMAND ----------

