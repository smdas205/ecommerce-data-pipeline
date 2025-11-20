import random
import csv
import uuid
from datetime import datetime, time, timedelta, date

yesterday = date.today() - timedelta(days=1)
event = ["view", "cart", "purchase", "return"]
data = [["user_id", "type_of_event", "event_timestamp", "product_id", "product_revenue"]]
products = {
    "prod_00": 2399,
    "prod_01": 1499,
    "prod_02": 3699,
    "prod_03": 4799,
    "prod_04": 7199,
    "prod_05": 8099,
    "prod_06": 1199,
    "prod_07": 5399,
    "prod_08": 6299,
    "prod_09": 9599}

def events(event_list):
    weight = [9, 6, 4, 2]
    event_prob = random.choices(event_list, weights=weight, k=1)
    selected_events = "".join(event_prob)
    return selected_events

def date_of_event():
    start_of_yesterday = datetime.combine(yesterday, time.min)
    random_sec = random.randint(0, 86399)
    random_datetime = start_of_yesterday + timedelta(seconds=random_sec)
    return random_datetime

for i in range(100000):
    random_user_id = uuid.uuid4()
    random_event = events(event)
    random_time = date_of_event()
    random_prod_id = random.choice(list(products.keys()))
    price = products[random_prod_id]
    if random_event == "return":
        random_revenue = price * (-1)
    elif random_event == "view" or random_event == "cart":
        random_revenue = 0
    else:
        random_revenue = price
    data.append([str(random_user_id), random_event, random_time, random_prod_id, random_revenue])

data[1:] = sorted(data[1:], key=lambda x:x[2])

with open(f'/home/hadoop/ecommerce-data-pipeline/logs/log_{yesterday}.csv', mode = 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)