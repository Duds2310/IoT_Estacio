from Database import Database
import pandas as pd
import os
import requests
from time import sleep

# Importa endpoint do PowerBI
ENDPOINT=os.environ['ENDPOINT']
MAX_ITEM=os.environ['MAX']
MIN_ITEM=os.environ['MIN']
DELAY=int(os.environ['DELAY_CONTROLLER'])

# Cria conexão com o db
connection = Database('mvp2')
connection.connect()


def load_dataset():

    # Abre conexão com o db
    connection.connect()

    # Carrega a colecao "Products"
    collection = connection.get_collection("Products")

    # Carrega o dataset com os dados do db
    dataset = pd.DataFrame(collection.find({},{"_id": 0}))

    # Fecha a conexao com o db
    connection.disconnect()

    return dataset


def load_keys():

    # Abre conexão com o db
    connection.connect()

    # Carrega a colecao "Products"
    collection = connection.get_collection("Products")

    # Carrega o dataset com os dados do db
    keys = collection.find({},{"_id": 0}).distinct('product')

    # Fecha a conexao com o db
    connection.disconnect()

    return keys



def evaluate_products():

    new_dataset = []
    for key in keys:
        
        # Separa os registros por produto
        df_sort = dataset[dataset["product"] == key]
        #print(f"{'-'*25}\n{df_sort}\n{'-'*25}")

        # Armazena o menor valor de cada produto
        new_dataset.append(df_sort.min().to_dict())
    
    return new_dataset


def make_event(all_data):
    
    raw_item = {"MAX": MAX_ITEM, "MIN": MIN_ITEM}
    
    for i in all_data:
        raw_item[i["product"]]=i["value"]

    return raw_item

def send_pb(event):

    print("INFO: {}".format(event))
    requests.post(ENDPOINT, json=event)



if __name__ == '__main__':
    
    keys = load_keys()

    while True:
        
        # Carrega os dados do db
        dataset = load_dataset()

        # Filtra os menores valores de cada produto
        dataset_f = evaluate_products()

        # Monta evento json
        event=make_event(dataset_f)

        # Envia os dados para o PowerBI
        send_pb(event)

        # Aguarda x segundos
        sleep(DELAY)





    
