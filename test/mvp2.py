import requests
import time
import random
import os
import pandas as pd
from Database import Database

# Importa endpoint do PowerBI
ENDPOINT=os.environ['ENDPOINT']

# Cria conexão com o db
connection = Database('mvp2')
connection.connect()


collection = connection.get_collection("Products")
dataset = pd.DataFrame(collection.find())


def get_item():
    return random.choice(dataset['product'].to_list())

def get_food(product):
    item_id = dataset[dataset["product"] == product].index.to_list()[0]
    dataset.at[item_id, "value"]-=5

def make_event():
    item = dataset.to_dict(orient='records')
    
    raw_item = {}
    for i in item:
        raw_item[i["product"]]=i["value"]

    raw_item["MAX"] = 100
    raw_item["MIN"] = 0

    return raw_item
    
def send_pb():

    event=make_event()
    print("INFO: {}".format(event))

    requests.post(ENDPOINT, json=event)

    

def run_service():

    send_pb()

    while True:
        item = get_item()
        get_food(item)
        send_pb()
        time.sleep(2)


if __name__ == '__main__':

    run_service()

