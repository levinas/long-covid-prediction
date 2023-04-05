from pyspark.sql.window import Window


def vaccine_test(vax_deduplicated_test):
    window_spec = Window.partitionBy("person_id").orderBy("person_id", "vax_date")
    vax_with_row_number = vax_deduplicated_test.withColumn("number", F.row_number().over(window_spec))

    vax_pivot = vax_with_row_number.groupBy("person_id").pivot("number", [1, 2, 3, 4]) \
        .agg(F.max("vax_date").alias("vax_date"), F.max("vax_type").alias("vax_type"))

    vax_transactions = vax_deduplicated_test.groupBy("person_id") \
        .agg(F.count("vax_date").alias("vaccine_txn"))

    vax_joined = vax_pivot.join(vax_transactions, ["person_id"], "left")

    final_df = vax_joined \
        .withColumn("date_diff_1_2", F.datediff("2_vax_date", "1_vax_date")) \
        .withColumn("date_diff_2_3", F.datediff("3_vax_date", "2_vax_date")) \
        .withColumn("date_diff_3_4", F.datediff("4_vax_date", "3_vax_date")) \
        .withColumn("switch_1_2", F.when((F.col("1_vax_type") != F.col("2_vax_type")) & (F.col("1_vax_date") != F.col("2_vax_date")), 1).otherwise(0)) \
        .withColumn("switch_2_3", F.when((F.col("2_vax_type") != F.col("3_vax_type")) & (F.col("2_vax_date") != F.col("3_vax_date")), 1).otherwise(0)) \
        .withColumn("switch_3_4", F.when((F.col("3_vax_type") != F.col("4_vax_type")) & (F.col("3_vax_date") != F.col("4_vax_date")), 1).otherwise(0))

    result = final_df.filter(F.col("vaccine_txn") < 5)
    return result