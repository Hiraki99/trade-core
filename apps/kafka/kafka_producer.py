import threading, time,json
from kafka import KafkaProducer

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
    def stop(self):
        self.stop_event.set()

    def run(self, data):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        
        print("producer: "+data)
        producer.send('test1',  bytearray(data, "utf8"))
        # time.sleep(1)

        producer.close()

def run_producer(data):
    Producer().run(data)
    
