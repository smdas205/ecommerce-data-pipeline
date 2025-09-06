import random
import csv
import uuid
from datetime import datetime, timedelta

start = datetime(2023, 1, 1, 0, 0, 0)
end = datetime(2025, 7, 22, 23, 59, 59)
def random_timestamp(start, end):
    diff = end - start
    random_seconds = random.randint(0, int(diff.total_seconds()))
    return start + timedelta(seconds=random_seconds)

event = ["view", "cart", "purchase", "return"]
data = [["user_id", "type_of_event", "timestamp", "product_id", "price"]]
products = {
    "prod_00": 2399,
    "prod_01": 1999,
    "prod_02": 3399,
    "prod_03": 4599,
    "prod_04": 7799,
    "prod_05": 8099,
    "prod_06": 899,
    "prod_07": 5599,
    "prod_08": 6399,
    "prod_09": 9999}

for i in range(10000):
    random_user_id = uuid.uuid4()
    random_event = random.choice(event)
    random_time = random_timestamp(start, end)
    random_prod_id = random.choice(list(products.keys()))
    price = products[random_prod_id]
    if random_event == "return":
        random_price *= -1
    elif random_event == "view" or random_event == "cart":
        random_price = 0
    else:
        random_price = price
    data.append([str(random_user_id), random_event, random_time, random_prod_id, random_price])

with open('random.csv', mode = 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)