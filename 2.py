import msgpack
import pymongo
import pandas as pd
import json
from bson import ObjectId

results = []

client = pymongo.MongoClient("")
db = client["TestCluster"]
collection = db["task_1_item"]

with open("./resources/task_2_item.msgpack", "rb") as f:
    data = msgpack.unpackb(f.read(), raw=False)

# Добавление данных в коллекцию
try:
    collection.insert_many(data)
except Exception as e:
    print(f"Ошибка при добавлении данных в MongoDB: {e}")
    exit()


# 1. Минимальная, средняя, максимальная salary
pipeline1 = [
    {"$group": {"_id": None, "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
]
results.append({"query1": list(collection.aggregate(pipeline1))})


# 2. Количество данных по профессиям
pipeline2 = [
    {"$group": {"_id": "$job", "count": {"$sum": 1}}}
]
results.append({"query2": list(collection.aggregate(pipeline2))})


# 3. Минимальная, средняя, максимальная salary по городу
pipeline3 = [
    {"$group": {"_id": "$city", "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
]
results.append({"query3": list(collection.aggregate(pipeline3))})


# 4. Минимальная, средняя, максимальная salary по профессии
pipeline4 = [
    {"$group": {"_id": "$job", "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
]
results.append({"query4": list(collection.aggregate(pipeline4))})


# 5. Минимальный, средний, максимальный возраст по городу
pipeline5 = [
    {"$group": {"_id": "$city", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}}
]
results.append({"query5": list(collection.aggregate(pipeline5))})


# 6. Минимальный, средний, максимальный возраст по профессии
pipeline6 = [
        {"$group": {"_id": "$job", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}}
]
results.append({"query6": list(collection.aggregate(pipeline6))})


# 7. Максимальная заработная плата при минимальном возрасте
pipeline7 = [
    {"$sort": {"age": 1}},
    {"$limit": 1},
    {"$project": {"_id": 0, "max_salary": "$salary", "min_age": "$age"}}
]
results.append({"query7": list(collection.aggregate(pipeline7))})


# 8. Минимальная заработная плата при максимальном возрасте
pipeline8 = [
    {"$sort": {"age": -1}},
    {"$limit": 1},
    {"$project": {"_id": 0, "min_salary": "$salary", "max_age": "$age"}}
]
results.append({"query8": list(collection.aggregate(pipeline8))})


# 9. Минимальный, средний, максимальный возраст по городу (зарплата > 50000), отсортировать по avg
pipeline9 = [
    {"$match": {"salary": {"$gt": 50000}}},
    {"$group": {"_id": "$city", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}},
    {"$sort": {"avg_age": -1}}
]
results.append({"query9": list(collection.aggregate(pipeline9))})


# 10. Минимальная, средняя, максимальная salary в диапазонах по городу, профессии и возрасту
pipeline10 = [
    {"$match": {
        "$or":[
            {"age": {"$gt": 18, "$lt": 25}},
            {"age": {"$gt": 50, "$lt": 65}}
        ]
    }},
    {"$group": {"_id": {"city": "$city", "job": "$job"}, 
                 "min_salary": {"$min": "$salary"}, 
                 "avg_salary": {"$avg": "$salary"}, 
                 "max_salary": {"$max": "$salary"}}}
]
results.append({"query10": list(collection.aggregate(pipeline10))})


# 11. Произвольный запрос с $match, $group, $sort
pipeline11 = [
    {"$match": {"city": "Москва", "salary": {"$gt": 100000}}},
    {"$group": {"_id": "$job", "avg_salary": {"$avg": "$salary"}, "count": {"$sum": 1}}},
    {"$sort": {"avg_salary": -1}}
]
results.append({"query11": list(collection.aggregate(pipeline11))})

with open("task2_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False, default=lambda obj: str(obj) if isinstance(obj, ObjectId) else obj)

client.close()