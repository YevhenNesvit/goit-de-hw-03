from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum

# Ініціалізація SparkSession
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# 1. Завантаження та читання CSV-файлів як DataFrame
users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

# 2. Видалення пропущених значень
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# 3. Визначення загальної суми покупок за кожною категорією продуктів
category_total_spent = purchases_df.join(products_df, "product_id", "inner") \
    .withColumn("total_spent", col("quantity") * col("price")) \
    .groupBy("category") \
    .agg(sum("total_spent").alias("category_total_spent"))

category_total_spent.show()

# 4. Визначення загальної суми покупок за кожною категорією продуктів
# для вікової категорії від 18 до 25 включно
age_category_total_spent = purchases_df.join(users_df, "user_id", "inner") \
    .filter((col("age") >= 18) & (col("age") <= 25)) \
    .join(products_df, "product_id", "inner") \
    .withColumn("total_spent", col("quantity") * col("price")) \
    .groupBy("category") \
    .agg(sum("total_spent").alias("age_category_total_spent"))

age_category_total_spent.show()

# 5. Визначення загальної частки покупок за кожною категорією товарів
# від сумарних витрат для вікової категорії від 18 до 25 років
age_category_percentage = (
    age_category_total_spent.join(category_total_spent, "category", "inner")
    .withColumn(
        "percentage",
        round((col("age_category_total_spent") / col("category_total_spent")) * 100, 2)
    )
    .select("category", "category_total_spent", "age_category_total_spent", "percentage")
)

age_category_percentage.show()

# 6. 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років
age_top_3_categories = (
    age_category_percentage
    .select("category", "percentage")
    .orderBy(col("percentage").desc())
    .limit(3)
)

age_top_3_categories.show()

spark.stop()
