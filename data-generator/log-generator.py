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

for i in range(10000):
    random_user_id = uuid.uuid4()
    random_event = random.choice(event)
    random_time = random_timestamp(start, end)
    random_prod_id = random.randint(0, 9999)
    if random_event == "return":
        random_price = round(random.uniform(-10000, 0),2)
    elif random_event == "view" or random_event == "cart":
        random_price = 0
    else:
        random_price = round(random.uniform(0, 10000),2)
    data.append([str(random_user_id), random_event, random_time, str(random_prod_id), random_price])

with open('random.csv', mode = 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)