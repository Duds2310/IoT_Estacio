from Database import Database

if __name__ == '__main__':

    # Abre conexao com o db    
    connection = Database('mvp2')
    connection.connect()

    # Recupera colecao "Products"
    col = connection.get_collection('Products')
    
    # Limpa a colecao
    col.drop()

    raw_data = [
        {"product": "Arroz", "value": 100},
        {"product": "Feijao", "value": 100},
        {"product": "Macarrao", "value": 100},
        {"product": "Carne", "value": 100},
        {"product": "Salada", "value": 100}
    ]

    # Insere os dados enviados
    ids = col.insert_many(raw_data)
    
    # Exibe os ids de insert
    print(ids.inserted_ids)

    # fecha a conexao
    connection.disconnect()

