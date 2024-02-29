from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when,instr

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("my_spark") \
    .getOrCreate()

# 创建一个简单的 DataFrame
data =[
            ('ABC17969(AB)', '1', 'ABC17969', 2022),
            ('ABC17969(AB)', '2', 'CDC52533', 2022),
            ('ABC17969(AB)', '3', 'DEC59161', 2023),
            ('ABC17969(AB)', '4', 'F43874', 2022),
            ('ABC17969(AB)', '5', 'MY06154', 2021),
            ('ABC17969(AB)', '6', 'MY4387', 2022),
            ('AE686(AE)', '7', 'AE686', 2023),
            ('AE686(AE)', '8', 'BH2740', 2021),
            ('AE686(AE)', '9', 'EG999', 2021),
            ('AE686(AE)', '10', 'AE0908', 2021),
            ('AE686(AE)', '11', 'QA402', 2022),
            ('AE686(AE)', '12', 'OM691', 2022)
        ]


columns =["peer_id", "id_1", "id_2", "year"]
df = spark.createDataFrame(data=data, schema=columns)


# 显示 DataFrame
df.show()

# 测试 DataFrame 的基本统计信息
df.describe().show()
# 1. For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
# 使用 INSTR 函数和 when 表达式来检查 'peer_id' 字段中是否包含 'id_2'
# 如果包含，则标记为 'Yes'，否则标记为 'No'
df_with_instr = df.withColumn("Q1", when(instr(col("peer_id"), col("id_2")) > 0, "Yes").otherwise("No"))

# 使用 where 方法来过滤出包含 'id_2' 的行
filtered_df = df_with_instr.where(col("Q1") == "Yes")
# 选择特定列
filtered_df.select("peer_id", "year").filter().show()

# 2. Given a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1).
# 分组并聚合
# 使用groupBy和count进行聚合，计算每个Name和Country组合的数量
aggregated_df = df.groupBy(col("peer_id"), col("year")).count()

# 显示聚合后的DataFrame
step2=aggregated_df.filter(col("count")>3)
step2.show()

# 3. Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year.
# If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number. For example, for ‘AE686(AE)’, the year is 2023, and count are:
# 2023, 1
# 2022, 2,
# 2021, 3
# As 1(2023 count) + 2(2022 count) >= 3 (given size number), the output would be 2023, 2022.


#For example, for ‘AE686(AE)’
st3=step2.select("peer_id", "year","count").filter(peer_id='AE686(AE)')

# 使用groupBy和count进行聚合，计算每个year和Count数量
s3 = st3.groupBy(col("year")).count()
s3.show()

# 定义窗口规范，按年份分区，并按日期排序
windowSpec = Window.partitionBy("year").orderBy(df["year"].desc)
# 使用窗口函数计算累计
step3 = s3.withColumn("v_count", sum(s3["count"]).over(windowSpec))
step3.filter(col("v_count")>3).select("year").show()

# 关闭 SparkSession
spark.stop()