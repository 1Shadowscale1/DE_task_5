import csv
import json
import pymongo
from bson import ObjectId

# Подключение к MongoDB
client = pymongo.MongoClient("")
db = client["TestCluster"]
products_collection = db["products"]
orders_collection = db["orders"]

# Функция для импорта CSV
def import_csv(filename, collection):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        collection.insert_many(list(reader))

# Функция для импорта JSON
def import_json(filename, collection):
    with open(filename, 'r', encoding='utf-8') as file:
        data = json.load(file)
        collection.insert_many(data)


import_csv("./resources/products.csv", products_collection)
import_json("./resources/orders.json", orders_collection)

products_collection.update_many(
    {"price": {"$type": "string"}},
    [
        {"$set": {"price": {"$toDouble": "$price"}}}  # Преобразование в число с плавающей точкой
    ]
)

# Задание 1: Выборка
# Products
query1_1 = products_collection.find({"category": "смартфоны"}, {"product_name": 1, "price": 1, "_id": 0}).limit(5)
query1_2 = products_collection.find({"stock_quantity": {"$lt": 10}}).sort("price", pymongo.ASCENDING).limit(5)
query1_3 = products_collection.find({"manufacturer": "Samsung"})
query1_4 = products_collection.find({"price": {"$gt": 50000}})
query1_5 = products_collection.find({"manufacturer": {"$in":["Samsung","Apple"]}})

# Orders
query1_6 = orders_collection.find({"total_amount": {"$gt": 100000}}).limit(5)
query1_7 = orders_collection.find({"customer_id": 123})
query1_8 = orders_collection.find({},{"order_id":1, "total_amount":1,"_id":0}).limit(5)
query1_9 = orders_collection.find({"order_date": {"$regex": "2024-01"}})
query1_10 = orders_collection.find({"products.product_id":1})

# Задание 2: Выборка с агрегацией
# Products
query2_1 = list(products_collection.aggregate([{"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}, "count": {"$sum": 1}}}]))
query2_2 = list(products_collection.aggregate([{"$match": {"manufacturer": "Apple"}}, {"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}}}]))
query2_3 = list(products_collection.aggregate([{"$group": {"_id": "$manufacturer", "total_stock": {"$sum": "$stock_quantity"}}}]))
query2_4 = list(products_collection.aggregate([{"$match": {"stock_quantity": {"$lt": 5}}}, {"$group": {"_id": "$category", "count": {"$sum": 1}}}]))
query2_5 = list(products_collection.aggregate([{"$group": {"_id": "$category", "min_price": {"$min": "$price"}, "max_price": {"$max": "$price"}}}]))

# Orders
query2_6 = list(orders_collection.aggregate([{"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$total_amount"}}}]))
query2_7 = list(orders_collection.aggregate([{"$unwind": "$products"}, {"$group": {"_id": "$products.product_id", "total_quantity": {"$sum": "$products.quantity"}}}]))
query2_8 = list(orders_collection.aggregate([{"$match": {"total_amount": {"$gt": 50000}}}, {"$group": {"_id": None, "avg_amount": {"$avg": "$total_amount"}}}]))
query2_9 = list(orders_collection.aggregate([
    {"$addFields": {"order_date": {"$toDate": "$order_date"}}},  # Преобразование в дату
    {"$group": {"_id": {"$month": "$order_date"}, "order_count": {"$sum": 1}}}
]))
query2_10 = list(orders_collection.aggregate([{"$unwind":"$products"},{"$lookup":{"from":"products","localField":"products.product_id","foreignField":"product_id","as":"product_info"}},{"$unwind":"$product_info"},{"$group":{"_id":"$product_info.product_name","total_ordered":{"$sum":"$products.quantity"}}}]))

# Задание 3: Обновление/удаление данных
# Products
result3_1 = products_collection.delete_many({"manufacturer": "Samsung"})
result3_2 = products_collection.update_many({"manufacturer": "Apple"}, {"$inc": {"price": 1000}})
result3_3 = products_collection.update_many({"category":"ноутбуки"}, {"$set":{"manufacturer":"Lenovo"}})
result3_4 = products_collection.update_one({"product_name": "Wireless Earbuds"}, {"$set": {"price": 100000}})
result3_5 = products_collection.delete_many({"price": {"$lt": 10000}})

# Orders
result3_6 = orders_collection.delete_many({"total_amount": {"$lt": 100000}})
result3_7 = orders_collection.update_many({"customer_id": 1003}, {"$set": {"order_date": "2024-03-15"}})
result3_8 = orders_collection.update_one({"order_id":1}, {"$push":{"products":{"product_id":1,"quantity":1}}})
result3_9 = orders_collection.update_many({"order_date": {"$regex": "2023-12"}}, {"$inc": {"total_amount": 500}})
result3_10 = orders_collection.delete_one({"order_id": 2})

# Задание 1: Выборка
query1_results = {}
# Products
query1_results["products"] = []
for query in [query1_1,query1_2,query1_3,query1_4,query1_5]:
    query1_results["products"].append(list(query))
# Orders
query1_results["orders"] = []
for query in [query1_6,query1_7,query1_8,query1_9,query1_10]:
    query1_results["orders"].append(list(query))

with open("query1_results.json", "w", encoding="utf-8") as f:
    json.dump(query1_results, f, indent=4, ensure_ascii=False, default=lambda obj: str(obj) if isinstance(obj, ObjectId) else obj)

# Задание 2: Выборка с агрегацией
query2_results = {}
# Products
query2_results["products"] = []
for query in [query2_1,query2_2,query2_3,query2_4,query2_5]:
    query2_results["products"].append(query)
# Orders
query2_results["orders"] = []
for query in [query2_6,query2_7,query2_8,query2_9,query2_10]:
    query2_results["orders"].append(query)

with open("query2_results.json", "w", encoding="utf-8") as f:
    json.dump(query2_results, f, indent=4, ensure_ascii=False, default=lambda obj: str(obj) if isinstance(obj, ObjectId) else obj)

# Задание 3: Обновление/удаление данных
query3_results = {}
query3_results["products"] = []
query3_results["products"].append({"deleted_count": result3_1.deleted_count})
query3_results["products"].append({"modified_count": result3_2.modified_count})
query3_results["products"].append({"modified_count": result3_3.modified_count})
query3_results["products"].append({"modified_count": result3_4.modified_count})
query3_results["products"].append({"deleted_count": result3_5.deleted_count})

query3_results["orders"] = []
query3_results["orders"].append({"deleted_count": result3_6.deleted_count})
query3_results["orders"].append({"modified_count": result3_7.modified_count})
query3_results["orders"].append({"modified_count": result3_8.modified_count})
query3_results["orders"].append({"modified_count": result3_9.modified_count})
query3_results["orders"].append({"deleted_count": result3_10.deleted_count})

with open("query3_results.json", "w", encoding="utf-8") as f:
    json.dump(query3_results, f, indent=4, ensure_ascii=False, default=lambda obj: str(obj) if isinstance(obj, ObjectId) else obj)

client.close()
print("Результаты сохранены в файлы query1_results.json, query2_results.json и query3_results.json")