from time import sleep

from celery import Celery, current_task, Task
import json, pprint, requests, textwrap

app = Celery('sparktask', backend='redis', broker='redis://localhost')

host = "http://10.100.136.40:8070"
headers = {'Content-Type': 'application/json'}

class SparkTask(Task):
    def on_success(self, retval, task_id, args, kwargs):
        print("SUccccesss")

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print("failed")

    def bind(self, app):
        return super(self.__class__, self).bind(app)

    def run(self, *args, **kwargs):
        code_data = kwargs.get('code_data', None)
        session_id, session_state = self.start_session()
        while session_state == "starting":
            session_state = self.get_session_state(session_id)

        post_response = requests.post(host + "/sessions/" + str(session_id) + "/statements",
                                      data=json.dumps(code_data),
                                      headers=headers)
        response = post_response.json()
        print(response)
        state = response[ "state" ]
        print(state)
        while state == "running" or state == "idle" or state == "busy" or state == "waiting":
            get_response = requests.get(host + "/sessions/" + str(session_id) + "/statements",
                                        data=json.dumps(code_data),
                                        headers=headers).json()
            response = get_response
            state = response[ 'statements' ][ 0 ][ "state" ]

            print(state)
            if state == "available":
                print(response[ 'statements' ][ 0 ][ "output" ][ "data" ])
                self.delete_session(session_id)


if __name__ == "__main__":
    spark_task = SparkTask()
    data = {
        'code': textwrap.dedent("""
            from pyspark.sql.types import * 
            baSchema=StructType([StructField('FinancialData', StringType(),True)])
            baDF=spark.read.format("com.databricks.spark.csv").option("header", "false").schema(baSchema).option("delimiter", "|").load("hdfs://10.100.136.40:9000/user/hduser/comma/*")
            baDF.createOrReplaceTempView("baView")
            sqldf = spark.sql("SELECT * FROM baView LIMIT 10")
            sqldf.collect()
            """)
    }

    #for i in range(1000):
    spark_task.bind(app)
    job = spark_task.apply_async(data = data)