import json
import pymongo

# Подключение к MongoDB (из предыдущих заданий)
client = pymongo.MongoClient("")
db = client["TestCluster"]
collection = db["task_1_item"]

# Чтение данных из JSON-файла
with open("./resources/task_3_item.json", "r", encoding="utf-8") as f:
    data = json.load(f)


# Добавление данных в коллекцию
try:
    collection.insert_many(data)
except Exception as e:
    print(f"Ошибка при добавлении данных в MongoDB: {e}")
    exit()


# 1. Удаление документов по предикату salary < 25000 || salary > 175000
result1 = collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})
print(f"Запрос 1: Удалено {result1.deleted_count} документов.")


# 2. Увеличение возраста всех документов на 1
result2 = collection.update_many({}, {"$inc": {"age": 1}})
print(f"Запрос 2: Обновлено {result2.modified_count} документов.")


# 3. Повышение зарплаты на 5% для выбранных профессий
jobs_to_update = ["Продавец", "Учитель"]
result3 = collection.update_many({"job": {"$in": jobs_to_update}}, {"$mul": {"salary": 1.05}})
print(f"Запрос 3: Обновлено {result3.modified_count} документов.")


# 4. Повышение зарплаты на 7% для выбранных городов
cities_to_update = ["Москва", "Санкт-Петербург"]
result4 = collection.update_many({"city": {"$in": cities_to_update}}, {"$mul": {"salary": 1.07}})
print(f"Запрос 4: Обновлено {result4.modified_count} документов.")


# 5. Повышение зарплаты на 10% по сложному предикату
city = "Москва"
jobs = ["Программист", "Инженер"]
min_age = 25
max_age = 35
result5 = collection.update_many(
    {"city": city, "job": {"$in": jobs}, "age": {"$gte": min_age, "$lte": max_age}},
    {"$mul": {"salary": 1.1}}
)
print(f"Запрос 5: Обновлено {result5.modified_count} документов.")


# 6. Удаление записей по произвольному предикату
result6 = collection.delete_many({"age": {"$lt": 20}})
print(f"Запрос 6: Удалено {result6.deleted_count} документов.")


client.close()