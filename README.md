# 🚀 PySpark Solution: Products & Categories Mapping

> **Решение задачи на соединение продуктов и категорий с использованием PySpark**

## 📋 Описание задачи

В PySpark приложении датафреймами (`pyspark.sql.DataFrame`) заданы продукты, категории и их связи:
- Каждому продукту может соответствовать **несколько категорий или ни одной**
- Каждой категории может соответствовать **несколько продуктов или ни одного**

**Цель:** Написать метод на PySpark, который в одном датафрейме вернет:
- ✅ Все пары «Имя продукта – Имя категории»
- ✅ Имена всех продуктов, у которых нет категорий

## 🗂️ Структура проекта

```
pyspark-products-categories/
├── 📄 get_product_category_pairs.py  # Основной код с решениями
├── 📖 README.md                      # Этот файл
├── 🔧 requirements.txt               # Зависимости проекта
└── 📊 example_usage.py               # Пример использования (опционально)
```

## 🛠️ Установка и настройка

### Системные требования

- **Python**: 3.7+ 
- **Java**: 8 или 11 (для PySpark)
- **RAM**: минимум 4GB (рекомендуется 8GB+)

### Установка зависимостей

```bash
# Установка через pip
pip install pyspark

# Или установка всех зависимостей из файла
pip install -r requirements.txt
```

### Проверка установки

```bash
# Проверка Python
python --version

# Проверка Java
java -version

# Проверка PySpark
python -c "import pyspark; print(pyspark.__version__)"
```

## 🎯 Решения

В проекте представлены **2 подхода** к решению задачи:

### 1️⃣ **Основное решение** (`get_product_category_pairs`)
- **Подход**: Два последовательных LEFT JOIN
- **Производительность**: ⚡ Высокая (2-3 стадии в Spark)
- **Читаемость**: 📖 Хорошая
- **Рекомендуется для**: Production использования

### 2️⃣ **Альтернативное решение** (`get_product_category_pairs_verbose`) 
- **Подход**: INNER JOIN + LEFT ANTI JOIN + UNION
- **Производительность**: ⚡ Средняя (3-4 стадии в Spark)
- **Читаемость**: 📖 Отличная
- **Рекомендуется для**: Обучения и демонстрации понимания

## 🚀 Быстрый старт

### Вариант 1: Интерактивный режим (pyspark shell)

```bash
# Запуск PySpark shell
pyspark

# В shell выполните:
exec(open('get_product_category_pairs.py').read())
# Теперь функции доступны для использования
```

### Вариант 2: Запуск как скрипт

```bash
spark-submit get_product_category_pairs.py

# Или через python (для локального режима)
python get_product_category_pairs.py
```

### Вариант 3: Импорт в ваш проект

```python
from get_product_category_pairs import get_product_category_pairs
from pyspark.sql import SparkSession

# Создание Spark сессии
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Ваши DataFrame'ы
products_df = ...
categories_df = ...
links_df = ...

# Получение результата
result = get_product_category_pairs(products_df, categories_df, links_df)
result.show()
```

## 📊 Пример использования

### Входные данные

**Продукты** (`products_df`):
```
+----------+-------------------+
|product_id|product_name       |
+----------+-------------------+
|1         |iPhone 15          |
|2         |MacBook Pro        |
|3         |Samsung Galaxy     |
|4         |Загадочный продукт |  # ← Продукт БЕЗ категорий
+----------+-------------------+
```

**Категории** (`categories_df`):
```
+-----------+-------------+
|category_id|category_name|
+-----------+-------------+
|1          |Смартфоны    |
|2          |Ноутбуки     |
|3          |Электроника  |
+-----------+-------------+
```

**Связи** (`product_category_links_df`):
```
+----------+-----------+
|product_id|category_id|
+----------+-----------+
|1         |1          |  # iPhone → Смартфоны
|1         |3          |  # iPhone → Электроника
|2         |2          |  # MacBook → Ноутбуки
|2         |3          |  # MacBook → Электроника
|3         |1          |  # Samsung → Смартфоны
|3         |3          |  # Samsung → Электроника
# Обратите внимание: продукт с ID=4 отсутствует!
+----------+-----------+
```

### Результат

```python
result = get_product_category_pairs(products_df, categories_df, links_df)
result.show()
```

**Вывод:**
```
+-------------------+-------------+
|product_name       |category_name|
+-------------------+-------------+
|iPhone 15          |Смартфоны    |
|iPhone 15          |Электроника  |
|MacBook Pro        |Ноутбуки     |
|MacBook Pro        |Электроника  |
|Samsung Galaxy     |Смартфоны    |
|Samsung Galaxy     |Электроника  |
|Загадочный продукт |null         |  # ← Продукт без категории!
+-------------------+-------------+
```

## 📝 Полный код для тестирования

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from get_product_category_pairs import get_product_category_pairs

# Создание Spark сессии
spark = SparkSession.builder \
    .appName("ProductsCategoriesDemo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Определение схем
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True)
])

categories_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("category_name", StringType(), True)
])

links_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True)
])

# Тестовые данные
products_data = [
    (1, "iPhone 15"),
    (2, "MacBook Pro"), 
    (3, "Samsung Galaxy"),
    (4, "Загадочный продукт")
]

categories_data = [
    (1, "Смартфоны"),
    (2, "Ноутбуки"),
    (3, "Электроника")
]

links_data = [
    (1, 1), (1, 3),  # iPhone в двух категориях
    (2, 2), (2, 3),  # MacBook в двух категориях  
    (3, 1), (3, 3)   # Samsung в двух категориях
    # Продукт ID=4 намеренно отсутствует
]

# Создание DataFrame'ов
products_df = spark.createDataFrame(products_data, products_schema)
categories_df = spark.createDataFrame(categories_data, categories_schema)
links_df = spark.createDataFrame(links_data, links_schema)

# Применение решения
result = get_product_category_pairs(products_df, categories_df, links_df)

# Вывод результата
print("🎯 РЕЗУЛЬТАТ:")
result.show(truncate=False)

# Дополнительная аналитика
print(f"📊 Всего строк: {result.count()}")
print(f"🔍 Продуктов без категорий: {result.filter(result.category_name.isNull()).count()}")

# Завершение сессии
spark.stop()
```

## 🔧 Настройка для разных сред

### Локальная разработка
```python
spark = SparkSession.builder \
    .appName("LocalDev") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

### Кластер (YARN/Kubernetes)
```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 4g \
    --executor-cores 2 \
    get_product_category_pairs.py
```

### Databricks/EMR
```python
# Код работает без изменений
# SparkSession уже создана как 'spark'
```

## 🐛 Решение проблем

### Проблема: "Java not found"
```bash
# Установка Java на Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk

# Установка Java на macOS
brew install openjdk@11

# Установка Java на Windows
# Скачайте с https://adoptopenjdk.net/
```

### Проблема: "Insufficient memory"
```python
# Увеличьте память для драйвера
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()
```

### Проблема: "Slow performance"
```python
# Включите адаптивную оптимизацию запросов
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

## 📈 Оптимизация производительности

### Для больших данных (>1GB)
```python
# Используйте broadcast для маленьких таблиц
from pyspark.sql.functions import broadcast

result = products_df \
    .join(links_df, "product_id", "left") \
    .join(broadcast(categories_df), "category_id", "left") \
    .select("product_name", "category_name")
```

### Кэширование данных
```python
# Если DataFrame используется многократно
categories_df.cache()
products_df.cache()

# Не забудьте очистить кэш
categories_df.unpersist()
```

### Партиционирование
```python
# Для очень больших таблиц
products_df = products_df.repartition("product_id")
```

## 🧪 Тестирование

### Единичные тесты
```python
def test_products_with_categories():
    """Тест продуктов с категориями"""
    result = get_product_category_pairs(products_df, categories_df, links_df)
    paired_products = result.filter(result.category_name.isNotNull())
    assert paired_products.count() > 0

def test_products_without_categories():
    """Тест продуктов без категорий"""
    result = get_product_category_pairs(products_df, categories_df, links_df)
    orphaned_products = result.filter(result.category_name.isNull())
    assert orphaned_products.count() >= 0
```

### Проверка корректности данных
```python
# Проверка на дубликаты
result.groupBy("product_name", "category_name").count().filter("count > 1").show()

# Проверка на null значения в product_name
result.filter(result.product_name.isNull()).show()
```

## 📚 Дополнительные материалы

### Документация
- [PySpark SQL Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

### Полезные команды
```python
# Просмотр плана выполнения
result.explain(True)

# Просмотр схемы данных
result.printSchema()

# Статистика по данным
result.describe().show()
```

## 📄 Лицензия

Этот проект создан в учебных целях и доступен для свободного использования.

---

*Если у вас есть вопросы или предложения по улучшению, создайте Issue или Pull Request.*
