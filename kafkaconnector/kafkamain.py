import sys
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import json
import zlib
import random
import threading

sys.path.extend(["../"])
from sparkconnector import sparkwithwait

kafkaServer = "kafka-server:9092"
requestTopic = "requests_topic"
responseTopic = "responses_topic"

class AnalysisThread(object):
    """
    class AnalysisThread
        input : action    str
                request_id   str
                request_data    str
        call  :  sparkwithwait.sendtolivy
                 sendresponse
                 
        operations :    create a thread for each call
                        
        methods : 
                    run : if action is valid send it to sparkwithwait.sendtolivy
                            then get response from it and send it to sendresponse       
     
    """
    def __init__(self, action, request_id, request_data):
        self.interval = 1
        thread = threading.Thread(target=self.run, args=(action, request_id, request_data))
        thread.daemon = True
        thread.start()
    def run(self, action, request_id, request_data):
        actionList = [
            'guild_analysis',
            'heatmap',
            '3dclustering',
            'no_transaction_vs_amount',
            'no_transaction_vs_harmonic',
            'amount_vs_harmonic',
            'label_counts',
            'no_transactions_statics',
            'amount_statics',
            'harmonic_statics',
        ]
        if action not in actionList:
            print("action does not exist")
            # send response false

        t = random.randint(1, 2)
        data = t
        print("wait for " + str(t) + " sec...")
        time.sleep(data)
        data = str(data)
        data = sparkwithwait.send_to_livy(func=action, request_data=request_data)
        send_response(request_id, result=True, data=data, message="")

def send_response(request_id, result, data=None, message=None):
    """
    method send_response
        input : request_id     str
                result   bool
                data    str         #the res
                message   str
        call  :  producer.send

        operations :    get the response => json's it => zip's it => send it to  kafka.producer.send
     
        return ret
    """
    with open("requests_id.txt", 'a') as fw:
        fw.write(str(request_id)+"\n")

    response_dict = {'response_id': request_id, 'result': result, 'data': data, 'message': message}
    response_json = json.dumps(response_dict)

    topic = responseTopic
    compressData = True
    if compressData:
        content = str(response_json)
        zcontent = zlib.compress(str.encode(str(content)))
        ret = producer.send(topic, zcontent)
    else:
        response_bytes = str.encode(str(response_json))
        ret = producer.send(topic, response_bytes)

    time.sleep(1)
    return ret
producer = KafkaProducer(bootstrap_servers=[kafkaServer], compression_type='gzip')

def start():
    print("Start Analysis server app")
    consumer = KafkaConsumer(requestTopic, bootstrap_servers=[kafkaServer], auto_offset_reset="latest")
    for request in consumer:
        request_dict = json.loads(request.value)
        action = request_dict['action']
        request_data = ""
        if "data" in request_dict:
            request_data = request_dict['data']
        request_id = request_dict[ 'request_id' ]
        if request_id in list(open("requests_id.txt").readline()):
            print("repetetive!")
            continue
        print(request_dict)
        if action == "guild_analysis":
            print("process guild_analysis ...")
            t = AnalysisThread('heatmap', request_id, request_data)
        elif action == "3dclustering":
            print("process 3dclustering ...")
            t = AnalysisThread('3dclustering', request_id, request_data)
        elif action == "no_transaction_vs_amount":
            print("process no_transaction_vs_amount ...")
            t = AnalysisThread('no_transaction_vs_amount', request_id, request_data)
        elif action == "no_transaction_vs_harmonic":
            print("process no_transaction_vs_harmonic ...")
            t = AnalysisThread('no_transaction_vs_harmonic', request_id, request_data)
        else:
            send_response(request_id, result=False, message="action not found")

if __name__ == "__main__":
    start()
