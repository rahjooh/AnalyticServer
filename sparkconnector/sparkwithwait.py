import json, pprint, requests, textwrap
from celery.task.base import task
from celery.utils.log import get_task_logger
import pandas as pd
from io import StringIO
from featuresegmentation.labeling import Labeling
from kafka import KafkaProducer
from kafka import KafkaConsumer
from guildanalysis.guildanalysis import GuildAnalysis
from datatransformation.logtransformation import LogTransformation
import transaction_perday_analysis
from featuresegmentation.segmentsstatistics import SegmentsStatistics
from convertdaterange import convert_date, convert_KIDDMMDF_query, convert_KIDayNumDF_query
import time

host = "http://10.100.136.40:8070"
headers = {'Content-Type': 'application/json'}


def start_session():
    """
        input :   post request
        
        operattion : get session_id, session_state from Post request
            
        return session_id, session_state
    """

    # delete_session(0)
    data = {'kind': 'pyspark'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    start_response = response.json()
    print(str(start_response))

    session_id = start_response[ "id" ]
    session_state = start_response[ "state" ]

    return session_id, session_state


def get_all_sessions():
    """
        input get requests
        
        operation : convert get request to json
        
        return all request in json
    """
    response = requests.get(host + '/sessions', headers=headers)
    return response.json()


def delete_session(session_id):
    """
        input  session_id
        
        output session_id
        
        operation : delete the session 
        
        return session content in json
    """
    print("Delete session Id " + str(session_id))
    response = requests.delete(host + '/sessions/' + str(session_id), headers=headers)
    return response.json()


def get_session_state(session_id):
    """
        input  session_id

        return session state in json
    """
    response = requests.get(host + "/sessions/" + str(session_id), headers=headers)
    session_response = response.json()
    session_state = session_response[ "state" ]
    return session_state


def run_code(func, code_data):
    """
        input  func
                code data
                
        operation  :   get session_id , session_state by calling start_session()
                        waiiiiiiit until finding a session_state which not equal to "Starting"
                        send post request by that session's statment and get response which contains statment_id
                        waiiiiiiit until response state get not equal to "available"
                        send get request by that session_id and  statment_id
                        waiiiiiiit until response.stat == "available"
                        
        
        call :   start_session()
            
    """
    try:
        session_id, session_state = start_session()
    except Exception as exc:
        raise exc
    while session_state == "starting":
        session_state = get_session_state(session_id)

    post_response = requests.post(host + "/sessions/" + str(session_id) + "/statements",
                                  data=json.dumps(code_data),
                                  headers=headers)
    response = post_response.json()
    print(response)
    state = response[ "state" ]
    statement_id = response[ "id" ]
    #print(state)
    while state != "available":
        get_response = requests.get(host + "/sessions/" + str(session_id) + "/statements/" + str(statement_id),
                                    headers=headers)
        response = get_response.json()
        time.sleep(0.001)
        try:
            state = response["state"]
        except:
            break

        #print(state)
    if state == "available":
        response_status = response[ "output" ][ "status" ]
        if response_status == "error":
            error = response[ "output" ][ "evalue" ]
            #get_task_logger("task finished with error {0}".format(error))
            print(error)
        else:
            response_dict = response[ "output" ][ "data" ]
            print("DONE!")
            clean_response_dict = response_dict[ "text/plain" ].strip("\\").strip("'")
            delete_session(session_id)
            dataframe = pd.read_json(clean_response_dict)
            dataframe = dataframe.set_index("merchant_number")

            if func == "heatmap":
                guild_analysis = GuildAnalysis(merchant_data_df=dataframe)
                plot_data = guild_analysis.top_guilds_vs_cluster_number(n_clusters=6, n_top_guilds=15)
                return plot_data
            elif func == "3dclustering":
                log_transformation = LogTransformation(merchant_data_df=dataframe)
                plot_data = log_transformation.kmeans(num_clusters=6, visualize_real_scale=True)
                return plot_data
            elif func == "no_transaction_vs_amount":
                #log_transformation = LogTransformation(merchant_data_df=dataframe)
                #plot_data = log_transformation.kmeans_no_transactions_sum_amounts(num_clusters=5, visualize_log=True)
                plot_data = transaction_perday_analysis.no_transaction_vs_sum_amounts_based_on_guild(merchant_data_df=dataframe, n_guild=10)
                #print(plot_data)
                return plot_data
            elif func == "no_transaction_vs_harmonic":
                plot_data = transaction_perday_analysis.no_transaction_vs_harmonic_based_on_guild(merchant_data_df=dataframe, n_guild=10)
                print(plot_data)
                return plot_data
            elif func == "boxplot":
                segments_statistics = SegmentsStatistics(merchant_data_df=dataframe)
                plot_data = segments_statistics.get_sum_amounts_statistics(visualize=True)
                return plot_data
            # elif func="test2":
            #     a = test2(dataframe)
            #     plot_data = a
            #     return plot_data

def send_to_livy(func, request_data):
    date_from = "94/09/01"
    date_to = "94/11/30"

    #KIDDMMDF_query = "SELECT Merchantnumber,SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}".format("'"+str(date_from)+"'", "'"+str(date_to)+"'")
    #KIDayNumDF_query = "SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy as int) - 94)))- {0})*30 + (cast(dd as int)) as dayNum FROM KIDDMMView".format(date_from)

    month_number_from = 9
    if "date_from" in request_data and "date_to" in request_data:
        month_number_from = request_data['date_from']
        date_from, date_to = convert_date(int(request_data['date_from']), int(request_data['date_to']))

    code_data = {
        'code': textwrap.dedent("""
                from pyspark.sql.types import *
                pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqData")
                pqDF.createOrReplaceTempView("KIView")

                KIDDMMDF=spark.sql("SELECT Merchantnumber,SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}")
                KIDDMMDF.createOrReplaceTempView("KIDDMMView")

                #KIDayNumDF = ""SELECT Merchantnumber,Amount,(cast( mm as int)+(12*(cast(yy as int) - 94)))- {2})*30 + (cast(dd as int)) as dayNum FROM KIDDMMView""
                KIDayNumDF=spark.sql("SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy as int) - 94)))- {2} )*30 + (cast(dd as int)) as dayNum FROM KIDDMMView")
                KIDayNumDF.createOrReplaceTempView("KIDayNumView")

                KIDayNumGrDF=spark.sql("SELECT Merchantnumber,dayNum,Count(*) as TxnNo,Sum(Amount) as TxnSum FROM KIDayNumView group by Merchantnumber,dayNum")
                KIDayNumGrDF.createOrReplaceTempView("KIDayNumGrView")


                KIRFMDF=spark.sql("SELECT Merchantnumber as merchant_number, Sum(TxnNo) as all_transactions, Sum(TxnSum) as sum_amounts, Sum(1/cast(dayNum as float)*cast(TxnNo as float)) as harmonic FROM KIDayNumGrView group by Merchantnumber")

                kirfmdf_dataframe = KIRFMDF.toPandas()
                kirfmdf_dataframe.to_json()
                """.format("'"+date_from+"'", "'"+ date_to+"'", month_number_from))
    }
    #print(code_data)
    return run_code(func, code_data)

if __name__ == "__main__":
    data = {
        'code': textwrap.dedent("""
                from pyspark.sql.types import *
                pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqData")
                pqDF.createOrReplaceTempView("KIView")

                KIDDMMDF=spark.sql("SELECT Merchantnumber,SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}")

                KIDDMMDF.createOrReplaceTempView("KIDDMMView")

                KIDayNumDF=spark.sql("SELECT Merchantnumber,Amount,(cast( mm as int)-9)*30 + (cast(dd as int)) as dayNum FROM KIDDMMView")
                KIDayNumDF.createOrReplaceTempView("KIDayNumView")

                KIDayNumGrDF=spark.sql("SELECT Merchantnumber,dayNum,Count(*) as TxnNo,Sum(Amount) as TxnSum FROM KIDayNumView group by Merchantnumber,dayNum")
                KIDayNumGrDF.createOrReplaceTempView("KIDayNumGrView")

                KIRFMDF=spark.sql("SELECT Merchantnumber as merchant_number, Sum(TxnNo) as all_transactions, Sum(TxnSum) as sum_amounts, Sum(1/cast(dayNum as float)*cast(TxnNo as float)) as harmonic FROM KIDayNumGrView group by Merchantnumber")

                kirfmdf_dataframe = KIRFMDF.toPandas()
                kirfmdf_dataframe.to_json()
                """.format("'94/09/01'", "'94/11/30'"))
    }
    print(data)
    #run_code("heatmap", code_data=data)
    request_data = {'date_from': 9, 'date_to': 11}
    print(send_to_livy("heatmap", request_data=request_data))