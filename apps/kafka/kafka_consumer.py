import multiprocessing
import json,requests,traceback
from flask import jsonify

from kafka import KafkaConsumer
from models import User,Account


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        
    def stop(self):
        self.stop_event.set()
        
    def run(self):
        print("consumer")
        consumer = KafkaConsumer(bootstrap_servers=['54.169.58.227:9092','54.169.58.227:9093','54.169.58.227:9094'],
                                auto_offset_reset='latest',
                                consumer_timeout_ms=1000)
        consumer.subscribe(['trade-event-in'])
        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                # read data json
                
                data = json.loads(message.value.decode("utf-8"))
                print("data = "+data["user_id"])
                
                print("consumer is received message")
                url = 'http://0.0.0.0:5040/pre-trade/confirm-connect'
                payload = "{'status': '1', 'message': 'OK'}"
                headers = {'content-type': 'application/json'}
                r = requests.post(url, data=payload, headers=headers)
                print("change to preTrade") 
                # check user in database
                try:
                    if data["user_id"] is not None:
                        user = User.query.filter_by(id=data.user_id).first()
                        print("s1")
                        if(not user):
                            return jsonify({'status': 0, 'message': 'Dont Existed User'})
                        else:
                            print("s2")
                            # check account in database
                            if data["accountNo"] is not None:
                                print("s3")
                                account = Account.query.filter_by(account_no=data.accountNo).first()
                                if(not account):
                                    print("s4")
                                    url = '0.0.0.0:5040/pre-trade/confirm-connect'
                                    payload = "{'status': 1, 'message': 'OK'}"
                                    headers = {'content-type': 'application/json'}
                                    r = requests.post(url, data=payload, headers=headers)
                                    print("change to preTrade") 
                                else:
                                    print("Dont Existed Account")
                            else:    
                                print("Dont Existed accountNo")
                    else: 
                        print('Dont Existed User')
                except:
                    traceback.print_exc()
            if self.stop_event.is_set():
                break
        consumer.close()

def start_consumer():
    print("start_consumer")
    Consumer().run()

start_consumer()