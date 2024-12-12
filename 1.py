import pymongo
import pandas as pd
import json
from bson import ObjectId

results=[]

client = pymongo.MongoClient("")
db = client["TestCluster"]
collection = db["task_1_item"]

df = pd.read_csv("./resources/task_1_item.csv", encoding='utf-8', sep=';')
data = df.to_dict('records')

# Вставка данных в MongoDB (удаляем коллекцию, если она уже существует)
collection.delete_many({})
collection.insert_many(data)


# 1. Первые 10 записей, отсортированных по убыванию по полю salary
query1 = collection.find().sort("salary", pymongo.DESCENDING).limit(10)
results.append({"query1": list(query1)})


# 2. Первые 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary
query2 = collection.find({"age": {"$lt": 30}}).sort("salary", pymongo.DESCENDING).limit(15)
results.append({"query2": list(query2)})



# 3. Первые 10 записей, отфильтрованных по сложному предикату
# (записи только из произвольного города, записи только из трех произвольно взятых профессий)
# отсортировать по возрастанию по полю age
city = "Москва"
jobs = ["Продавец", "Учитель", "Водитель"]
query3 = collection.find({"$and": [{"city": city}, {"job": {"$in": jobs}}]}).sort("age", pymongo.ASCENDING).limit(10)
results.append({"query3": list(query3)})



# 4. Вывод количества записей, получаемых в результате следующей фильтрации
# (age в произвольном диапазоне, year в [2019,2022], 50000 < salary <= 75000 || 125000 < salary < 150000)
min_age = 25
max_age = 40
query4 = collection.find({
    "$and": [
        {"age": {"$gte": min_age, "$lte": max_age}},
        {"year": {"$gte": 2019, "$lte": 2022}},
        {"$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]}
    ]
})

query4_list = list(query4)
results.append({"query4": len(query4_list)})
 
with open("task1_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False, default=lambda obj: str(obj) if isinstance(obj, ObjectId) else obj)

client.close()