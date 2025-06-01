from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession

# основное решение черещ два join
def get_product_category_pairs(
        products_df: DataFrame, 
        categories_df: DataFrame, 
        product_category_links_df: DataFrame) -> DataFrame:
    products_with_links = products_df.join(
            product_category_links_df, 
            products_df.product_id == product_category_links_df.product_id, 
            how="left"
    ).select(
            products_df.product_id,
            products_df.product_name,
            product_category_links_df.category_id
    )
    
    joined_with_categories = products_with_links.join(
            categories_df, 
            products_with_links.category_id == categories_df.category_id,
            how="left"
    )
    return joined_with_categories.select(col("product_name"), col("category_name"))
    
    
# АЛЬТЕРНАТИВА
# через связанные пары (inner join), продукты без категории (left_anti) и объединение (union)
def get_product_category_pairs_verbose(
        products_df: DataFrame, 
        categories_df: DataFrame, 
        product_category_links_df: DataFrame) -> DataFrame:
    # все пары
    product_category_pairs = (
            products_df
            .join(product_category_links_df, on="product_id", how="inner")
            .join(categories_df, on="category_id", how="inner")
            .select("product_name", "category_name")
    )
    # продукты без категории
    product_without_categories = (
            products_df
            .join(product_category_links_df, on="product_id", how="left_anti")
            .select(col("product_name"), lit(None).cast("string").alias("category_name"))
    )
    # объединение резов
    return product_category_pairs.union(product_without_categories)
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ProductCategoryDemo").getOrCreate()

    # тестовые данные
    products_data = [(1, "Ноутбук Alpha"), (2, "Смартфон Beta"), (3, "Планшет Gamma"), (4, "Наушники Delta")]
    products_df_main = spark.createDataFrame(products_data, ["product_id", "product_name"])

    categories_data = [(101, "Электроника"), (102, "Компьютеры"), (103, "Гаджеты")]
    categories_df_main = spark.createDataFrame(categories_data, ["category_id", "category_name"])

    links_data = [(1, 101), (1, 102), (2, 101), (2, 103), (3, 101)] # продукт 4 без категории
    links_df_main = spark.createDataFrame(links_data, ["product_id", "category_id"])

    print("--- Результат get_product_category_pairs ---")
    result1 = get_product_category_pairs(products_df_main, categories_df_main, links_df_main)
    result1.show(truncate=False)

    print("\n--- Результат get_product_category_pairs_verbose ---")
    result2 = get_product_category_pairs_verbose(products_df_main, categories_df_main, links_df_main)
    result2.show(truncate=False)

    spark.stop()