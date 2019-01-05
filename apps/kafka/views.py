from . import kafka_producer
from . import Micro
from microservices_connector.Interservices import Microservice
app = Microservice('Flask_app').app
import json
print("adfasdf")
# @Micro.typing('/listen-producer')
# @Micro.json
# @app.route('/listen-producer', methods=['POST'])
# def listen_producer():
#     data = '{"accountNo": "0001001128", "balance": 11951366807, "receivingCash": 0, "debt": 0, "debtInt": 0, "advancedCash": 0}'
#     print("listen_producer: "+data)
#     kafka_producer.run(data)
#     return jsonify({"msg": "Successfully logged out"})
    
