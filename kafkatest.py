import time
import json
from random import randint

import plotly
from plotly.offline import plot as offpy
import plotly.graph_objs as go

from kafka import KafkaProducer
from kafka import KafkaConsumer

def bar_chart_plot(x, y, title):
    data = [ go.Bar(
        x=x,
        y=y
    ) ]
    layout = go.Layout(title=title)
    fig = go.Figure(data=data, layout=layout)
    return offpy(fig,include_plotlyjs=False,show_link=False,output_type='div')

def start():
    consumer = KafkaConsumer('requests',bootstrap_servers=['kafka-server:9092'])
    for request in consumer:
        temp = request.value
        req_data = json.loads(temp)
        ret_data = req_data['data'][::-1]
        x = ['senf1','senf2','senf3']
        y1 = randint(1,100)
        y2 = randint(1,100)
        y3 = randint(1,100)
        y = [y1, y2, y3]
        ret_data = bar_chart_plot(x,y,'random')
        resp_data = {'response_id': req_data['request_id'],'data':ret_data}
        resp_data = json.dumps(resp_data)
        data = str.encode(str(resp_data))
        producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'])
        topic = "responses"
        #time.sleep(30)
        ret = producer.send(topic, data)
        time.sleep(1)
        continue

if __name__ == "__main__":
    start();
