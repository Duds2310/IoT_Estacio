import requests
import time
import random
import os
import pandas as pd

ENDPOINT=os.environ['ENDPOINT']

raw_data = [
    {"product": "Arroz", "value": 100},
    {"product": "Feijao", "value": 100},
    {"product": "Macarrao", "value": 100},
    {"product": "Carne", "value": 100},
    {"product": "Salada", "value": 100},
]

dataset = pd.DataFrame(raw_data)


def get_item():
    return random.choice(dataset['product'].to_list())

def get_food(product):
    item_id = dataset[dataset["product"] == product].index.to_list()[0]
    dataset.at[item_id, "value"]-=5

# OLD
def gen_data():
    return random.randint(0,100)

def make_event(product):
    item = dataset[dataset["product"] == product].to_dict(orient='records')[0]
    return {"key":item["product"], "value":item["value"]}
    
def send_pb(product):

    event=make_event(product)
    print("INFO: {}".format(event))

    #requests.post(ENDPOINT, json=event)

def send_all():

    for product in dataset['product'].to_list():
        send_pb(product)
    

def run_service():

    send_all()

    while True:
        item = get_item()
        get_food(item)
        send_pb(item)
        time.sleep(2)


if __name__ == '__main__':

    run_service()

