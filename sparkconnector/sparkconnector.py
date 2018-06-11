import json, pprint, requests, textwrap
from time import sleep


class SparkConnector(object):
    def __init__(self):
        self._host = "http://10.100.136.40:8070"
        self._headers = {'Content-Type': 'application/json'}

    def start_session(self):
        data = {'kind': 'pyspark'}
        response = requests.post(self._host + '/sessions', data=json.dumps(data), headers=self._headers)
        return response.json()
        #delete_response = requests.get(self._host + '/sessions/1', headers=headers)

    def get_session(self, session_id):
        response = requests.get(self._host + "/sessions/" + str(session_id), headers=self._headers)
        return response.json()

    def get_all_sessions(self):
        response = requests.get(self._host + '/sessions', headers=self._headers)
        return response.json()

    def delete_session(self, session_id):
        response = requests.delete(self._host + '/sessions/'+str(session_id), headers=self._headers)
        return response.json()

    def run_code(self, session_id, code_data):
        r = requests.post(self._host+"/sessions/"+str(session_id)+"/statements", data=json.dumps(code_data), headers=self._headers)
        #sleep(20)
        pprint.pprint(r.json())

if __name__ == "__main__":
    data = {
        'code': textwrap.dedent("""
        import random
        NUM_SAMPLES = 100000
        def sample(p):
          x, y = random.random(), random.random()
          return 1 if x*x + y*y < 1 else 0

        count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
        print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)
        """)
    }

    spark_connector = SparkConnector()
    #print(spark_connector.get_all_sessions())
    #ss = spark_connector.start_session()
    #ss = spark_connector.delete_session(session_id=0)
    #print(ss)
    #ss = spark_connector.get_all_sessions()
    #print(ss)
    spark_connector.run_code(session_id=0, code_data=data)