from Database import Database

connection = Database('mvp2')
connection.connect()



def test1():

    collection = connection.get_collection('mycollection')

    # Inserir um documento na coleção
    document = {'name': 'John Doe', 'age': 111}
    collection.insert_one(document)

    # Consultar documentos na coleção
    result = collection.find()
    for document in result:
        print(f"{type(document)}: {document}")

    #collection.drop()

def test2():

    col = connection.get_collection('Products')

    raw_data = [
        {"product": "Arroz", "value": 100},
        {"product": "Feijao", "value": 100},
        {"product": "Macarrao", "value": 100},
        {"product": "Carne", "value": 100},
        {"product": "Salada", "value": 100}
    ]

    print(col.insert_many(raw_data).inserted_ids)

    #col.drop()

def test3(collection):

    col = connection.get_collection(collection)

    for x in col.find():
        print(x) 

    #col.drop()


if __name__ == '__main__':
    

    #test1()

    test2()

    #test3("Products")

    # Fechar a conexão
    connection.disconnect()


