import requests
import time
import random
import os

ENDPOINT=os.environ['ENDPOINT']

def gen_data():
    return random.randint(0,100)

def make_event():
    return {"key":"A", "value":gen_data()}
    
def send_pb():
    event=make_event()
    print(event)
    requests.post(ENDPOINT, json=event)


if __name__ == '__main__':
    
    while True:
        send_pb()
        time.sleep(5)


