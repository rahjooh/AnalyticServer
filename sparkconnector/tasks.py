from celery import Celery, current_task, Task
import json, pprint, requests, textwrap
from celery.task.base import task
from celery.utils.log import get_task_logger
import pandas as pd
from io import StringIO
from featuresegmentation.labeling import Labeling

logger = get_task_logger(__name__)

app = Celery('tasks', backend='redis', broker='redis://localhost')


host = "http://10.100.136.40:8070"
headers = {'Content-Type': 'application/json'}

class SparkTask(Task):
    def on_success(self, retval, task_id, args, kwargs):
        print("Successs")

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print("failed")



def start_session():
    data = {'kind': 'pyspark'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    start_response = response.json()
    session_id = start_response["id"]
    session_state = start_response["state"]
    return session_id, session_state


def get_all_sessions():
    response = requests.get(host + '/sessions', headers=headers)
    return response.json()


def delete_session(session_id):
    response = requests.delete(host + '/sessions/'+str(session_id), headers=headers)
    return response.json()

def get_session_state(session_id):
    response = requests.get(host + "/sessions/" + str(session_id), headers=headers)
    session_response = response.json()
    session_state = session_response["state"]
    return session_state

@task(base=SparkTask)
def run_code(code_data):
    try:
        session_id, session_state = start_session()
        #session_id = 0
        #session_state = "idle"
        print(session_id)
    except Exception as exc:
        get_task_logger("Cannot establish connection to remote server {0}".format(host))
        raise exc

    while session_state == "starting":
        session_state = get_session_state(session_id)

    post_response = requests.post(host + "/sessions/" + str(session_id) + "/statements",
                      data=json.dumps(code_data),
                      headers=headers)
    response = post_response.json()
    print(response)
    state = response["state"]
    statement_id = response["id"]
    print(state)
    while state == "running" or state == "idle" or state == "busy" or state == "waiting":
        get_response = requests.get(host+"/sessions/"+str(session_id)+"/statements/"+str(statement_id), headers=headers)
        response = get_response.json()
        try:
            state = response["state"]
        except:
            break
        #sleep(2)
        print(state)
        if state == "available":
            response_status = response["output"]["status"]
            if response_status == "error":
                error = response["output"]["evalue"]
                get_task_logger("task finished with error {0}".format(error))
                print(error)
            else:
                response_dict = response["output"]["data"]
                print("DONE!")
                clean_response_dict = response_dict["text/plain"].strip("\\").strip("'")
                dataframe = pd.read_json(clean_response_dict)
                dataframe = dataframe.set_index("merchant_number")
                #print(dataframe[["sum_amounts", "all_transactions"]])
                labeling1 = Labeling()
                labeling1.set_merchant_data_df(dataframe)
                labeling1.kmeans(num_clusters=5,visualize=True)


            #delete_session(session_id)


if __name__ == "__main__":
    data = {
        'code': textwrap.dedent("""
            from pyspark.sql.types import *
            pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqData")
            pqDF.createOrReplaceTempView("KIView")
            
            KISchema=StructType([StructField('ReferNumber',StringType(),True)
            ,StructField('FinancialDate',StringType(),True)
            ,StructField('LocalTime',StringType(),True)
            ,StructField('LocalDate',StringType(),True)
            ,StructField('InTime',StringType(),True)
            ,StructField('OutTime',StringType(),True)
            ,StructField('TraceNo',StringType(),True)
            ,StructField('Amount',LongType(),True)
            ,StructField('AcquireCurrency',StringType(),True)
            ,StructField('CardNo',StringType(),True)
            ,StructField('AcquireBankCode',StringType(),True)
            ,StructField('IssuerBankCode',StringType(),True)
            ,StructField('LocalOrShetab',StringType(),True)
            ,StructField('FromAccountNo',StringType(),True)
            ,StructField('ToAccountNo',StringType(),True)
            ,StructField('ProcessCode',StringType(),True)
            ,StructField('Branch',StringType(),True)
            ,StructField('Merchantnumber',StringType(),True)
            ,StructField('OnlineOROffline',StringType(),True)
            ,StructField('TerminalTypeCode',StringType(),True)
            ,StructField('TerminalNo',StringType(),True)
            ,StructField('ResponseCode',StringType(),True)
            ,StructField('ReturnCode',StringType(),True)
            ,StructField('MessageType',StringType(),True)
            ,StructField('StatusCode',StringType(),True)
            ,StructField('SuccessofFailure',StringType(),True)
            ,StructField('ReferenceNo',StringType(),True)
            ,StructField('TransactionAmount',LongType(),True)
            ,StructField('IssuerCurrency',StringType(),True)
            ,StructField('SettlementDateInIssuerSwitch',StringType(),True)
            ,StructField('RevisoryAmount',LongType(),True)
            ,StructField('ExpireDate',StringType(),True)
            ,StructField('SourceOrganization',StringType(),True)
            ,StructField('TraceNoforTransactionswithREVERSAL',StringType(),True)
            ,StructField('DateforTransactionwithREVERSAL',StringType(),True)
            ,StructField('TimeforTransactionwithREVERSAL',StringType(),True)
            ,StructField('AcquireforTransactionwithREVERSAL',StringType(),True)
            ,StructField('IssuerforTransactionwithREVERSAL',StringType(),True)
            ,StructField('Shetabfee',LongType(),True)
            ,StructField('Networkfee',LongType(),True)
            ,StructField('Acquirefee',LongType(),True)
            ,StructField('AccountIssuerBranch',StringType(),True)
            ,StructField('InDate',StringType(),True)
            ,StructField('PrivateUse',StringType(),True)
            ,StructField('BillNumber',StringType(),True)
            ,StructField('PaymentNumber',StringType(),True)
            ,StructField('BillTypeandOrganizationCode',StringType(),True)
            ,StructField('OperationNo',StringType(),True)
            ,StructField('Wage',StringType(),True)
            ,StructField('UnKnown',StringType(),True)])
            
            KIDDMMDF=spark.sql("SELECT Merchantnumber,SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}")

            KIDDMMDF.createOrReplaceTempView("KIDDMMView")

            KIDayNumDF=spark.sql("SELECT Merchantnumber,Amount,(cast( mm as int)-9)*30 + (cast(dd as int)) as dayNum FROM KIDDMMView")
            KIDayNumDF.createOrReplaceTempView("KIDayNumView")
            
            KIDayNumGrDF=spark.sql("SELECT Merchantnumber,dayNum,Count(*) as TxnNo,Sum(Amount) as TxnSum FROM KIDayNumView group by Merchantnumber,dayNum")
            KIDayNumGrDF.createOrReplaceTempView("KIDayNumGrView")
            
            
            KIRFMDF=spark.sql("SELECT Merchantnumber as merchant_number, Sum(TxnNo) as all_transactions, Sum(TxnSum) as sum_amounts, Sum(1/cast(dayNum as float)*cast(TxnNo as float)) as harmonic, Sum(1/sqrt(cast(dayNum as float))*cast( TxnNo as float)) as SquaredHarmonic FROM KIDayNumGrView group by Merchantnumber")
            
            kirfmdf_dataframe = KIRFMDF.toPandas()
            kirfmdf_dataframe.to_json()
            """.format("'94/09/01'", "'94/11/30'"))
    }
    #dataframe = sqldf.toPandas()
    #dataframe.to_json()
    #print(get_all_sessions())
    #print(start_session())

    #for i in range(1000):
    #job = run_code.apply_async((data,))
    labeling = Labeling()