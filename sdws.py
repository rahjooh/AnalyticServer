# import random
# from functools import reduce
# import time
#
# NUM_SAMPLES = 100000000
#
#
# def sample(p):
#     x, y = random.random(), random.random()
#     return 1 if x * x + y * y < 1 else 0
#
# t1 = time.time()
# count = map(lambda x: sample(x), list(range(NUM_SAMPLES)))
# count = reduce(lambda a, b: a + b, count)
# print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
# t2 = time.time()
# print(t2-t1)
import pandas as pd
from kafka import KafkaConsumer
from kafka import KafkaProducer
from pandas.core.frame import DataFrame

df = pd.DataFrame([['a', 'b'], ['c', 'd']],
                   index=['row 1', 'row 2'],
                 columns=['col 1', 'col 2'])

a = df.to_json()
print(a)
x = pd.read_json(a)
print(x)
import json
from datetime import datetime
#j = '{"LocalTime":{"0":"1,94\\/12\\/29,10:55:50,94\\/12\\/29,18:36:37,18:36:37,001198,20000,364,6221061107013809,581672111,622106,SA,,0201860049007,000000,0024,62006362999999,ON,14,62899999,25,0,400,410,U,500900003134,0,000,,0,0000,622106,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,030,0,","1":"2,94\\/12\\/29,18:34:52,94\\/12\\/29,18:35:53,18:35:53,017744,240000,364,6273531040145429000,581672111,627353,SA,,0200871140002,000000,0249,62006362038838,ON,14,62038888,25,0,400,410,U,388880787924,0,000,,0,0000,627353,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,010,0,","2":"3,94\\/12\\/29,18:33:37,94\\/12\\/29,18:34:52,18:34:52,035434,50000,364,6395991102298759,581672111,639599,SA,,0200926354004,000000,2813,62006362039642,ON,14,62039701,25,0,400,410,U,397010780153,0,000,,0,0000,639599,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,010,0,","3":"4,94\\/12\\/29,18:02:30,94\\/12\\/29,18:34:52,18:34:52,094397,2870000,364,6280231440636875,581672111,628023,SA,,0200759541005,000000,0229,062006362037072,ON,14,62037095,25,0,400,410,U,370950781385,0,000,,0,0000,628023,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,010,0,","4":"5,94\\/12\\/29,18:34:12,94\\/12\\/29,18:34:12,00:00:00,927462,1000000,364,6037701099231885,581672111,603770,SA,,,000000,1600,,ON,14,62055788,63,0,400,0,U,557883668078,0,000,,0,0000,603770,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,000,0,","5":"6,94\\/12\\/29,18:31:29,94\\/12\\/29,18:33:06,18:33:06,004704,60000,364,5041721015488217,581672111,504172,SA,,0201795747004,000000,0600,62006362104743,ON,14,62205852,25,0,400,410,U,058520781554,0,000,,0,0000,504172,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,010,0,","6":"7,94\\/12\\/29,18:31:39,94\\/12\\/29,18:32:45,18:32:47,002193,150000,364,5029381002557486,581672111,502938,SA,,0201634921009,000000,0100,62006362062841,ON,14,62063601,00,0,400,410,R,636010781898,150000,364,94\\/12\\/29,0,0000,502938,002193,1394\\/12\\/29,18:31:39,581672111,0,0,0,0,,94\\/12\\/29,,,,,281,0,","7":"8,94\\/12\\/29,11:28:22,94\\/12\\/29,18:52:21,18:52:22,000151,50000,364,6362141075278557,581672111,636214,SA,,0201860049007,000000,0024,62006362999999,ON,14,62899999,25,0,400,410,U,500900003134,0,000,,0,0000,636214,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,030,0,","8":"9,94\\/12\\/29,18:40:22,94\\/12\\/29,18:43:26,18:43:26,001121,850000,364,6037991341638611,581672111,603799,SA,,0201459073002,000000,0340,062006362038188,ON,14,62038210,25,0,400,410,U,382100781115,0,000,,0,0000,603799,000000,1378\\/10\\/11,00:00:00,0,0,0,0,0,,94\\/12\\/29,,,,,010,0,","9":"10,94\\/12\\/29,18:36:33,94\\/12\\/29,18:43:19,18:43:20,013955,99500,364,5892101060147903,581672111,589210,SA,,0201860049007,000000,0024,62006362999999,ON,14,62899999,00,0,400,410,R,999990788529,99500,364,94\\/12\\/29,0,0000,589210,013955,1394\\/12\\/29,18:36:33,581672111,0,0,0,0,,94\\/12\\/29,,,,,281,0,"},"Amount":{"0":null,"1":null,"2":null,"3":null,"4":null,"5":null,"6":null,"7":null,"8":null,"9":null}}'



a = 10*["f"]
print(a)