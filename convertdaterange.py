import numpy as np

def convert_date(FromMonthNo, ToMonthNo):
    if (FromMonthNo < 10):
        FromMonthNo_Pre="0"+str(FromMonthNo)
    else:
        FromMonthNo_Pre=str(FromMonthNo)


    if(int(FromMonthNo)<=12):
        FromDate='94/'+FromMonthNo_Pre+'/01'
    else:
        FromDate='95/'+"0"+str(int(FromMonthNo)-12)+'/01'


    if (ToMonthNo>12):
        ToMonthNo_Pre="0"+str((int(ToMonthNo)-12))
        ToDate='95/'+ToMonthNo_Pre+'/30'
    else:
        if(ToMonthNo<10):
            ToMonthNo_Pre = "0" + str(ToMonthNo)
        else:
            ToMonthNo_Pre = str(ToMonthNo)
        ToDate = '94/' + ToMonthNo_Pre + '/30'

    return FromDate, ToDate
    # Query_Range = "SELECT Merchantnumber,SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}".format(FromDate, ToDate)
    #
    # Query_DayNum = "SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy) as int - 94)))- {0})*30 + (cast(dd as int)) as dayNum FROM KIDDMMView".format(FromMonthNo)
    #
    #
    # print(Query_Range)
    # print('******************')
    # print(Query_DayNum)

def convert_KIDDMMDF_query(from_month_number, to_month_number):
    from_date, to_date = convert_date(from_month_number, to_month_number)
    KIDDMMDF = "SELECT Merchantnumber, SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}".format(from_date, to_date)
    return KIDDMMDF

def convert_KIDayNumDF_query(from_month_number):
    KIDayNumDF = "SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy as int) - 94)))- {0})*30 + (cast(dd as int)) as dayNum FROM KIDDMMView".format(from_month_number)
    return KIDayNumDF

if __name__ == "__main__":
    print(convert_date(12,11))

print(convert_date(2,5))
