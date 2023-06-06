from Database import Database
from random import choice
import pandas as pd
from time import sleep
from datetime import datetime
import os

# Cria um objeto Database
connection = Database('mvp2')
DELAY=int(os.environ['DELAY_ARDUINO'])

def load_dataset():

    # Abre conexão com o db
    connection.connect()

    # Carrega o dataset com os dados do db
    collection = connection.get_collection("Products")
    dataset = pd.DataFrame(collection.find({},{"_id": 0}))

    # Fecha a conexao com o db
    connection.disconnect()

    return dataset

def insert_data(data):
    
    # Abre conexão com o db
    connection.connect()

    # Carrega o dataset com os dados do db
    collection = connection.get_collection("Products")
    
    # Carrega os dados na colecao
    ids = collection.insert_many(data)

    # Exibe os ids inseridos
    #print(ids.inserted_ids)

    # Fecha a conexao com o db
    connection.disconnect()


    


def get_item():

    # Valida quais produtos ainda tem valor(estoque)
    df_qtd = dataset.loc[dataset["value"] > 0] 

    # Retorna produto aleatorio
    return choice(df_qtd['product'].to_list()) if not df_qtd.empty > 0 else False


# Simula o consumo dos produtos
def get_food(product):

    
    # Tenta remover 5 do valor de estoque escolhido
    try:
        item_id = dataset[dataset["product"] == product].index.to_list()[0]
        dataset.at[item_id, "value"]-=5
    except:
        print("Estoque Zerado!")
        raise


def run():
    
    while True:
            
        item = get_item()

        get_food(item)

        print(f"{'-'*25}\n{dataset}\n{'-'*25}")

        data_to_db = dataset.to_dict(orient="records") 
        
        insert_data(data_to_db)

        sleep(DELAY)



if __name__ == '__main__':

    dataset = load_dataset()
    
    run()