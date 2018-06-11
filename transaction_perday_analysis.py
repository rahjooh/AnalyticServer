import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn import datasets
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn import preprocessing

from readdata.crmdata import CRMData
from readdata.crmmerchant import CRMMerchant
from readdata.merchantdata import MerchantData
from visualization import plotlyvisualize
import os
import numpy as np

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"


def no_transaction_vs_harmonic():
    merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    data_reader = MerchantData(merchant_data_path)
    X = data_reader.get_clean_data()

    # Data Selection
    harmonic_amount = X[ :, 0 ]  # Recency
    no_transaction = X[ :, 1 ]  # Frequency

    # Plotting
    title = "Trans vs Harmonic"
    labels = ("No Transaction", "Harmonic Sum")
    plotlyvisualize.scatter(no_transaction, harmonic_amount, title, labels, out_path=PLOT_OUT_DIR)


def no_transactions_vs_sum_amounts():
    merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    data_reader = MerchantData(merchant_data_path)
    X = data_reader.get_clean_data()

    # Data Selection
    no_transaction = X[ :, 1 ]  # Frequency
    sum_amounts = X[ :, 2 ]  # Money

    # Plotting
    title = "No Transactions vs Sum Amounts"
    labels = ("No Transaction", "Sum Amounts")
    no_transaction = np.log(no_transaction)
    #sum_amounts = np.log(sum_amounts)/np.log(1.5)

    plotlyvisualize.scatter(no_transaction, sum_amounts, title, labels, out_path=PLOT_OUT_DIR)


# def no_transaction_per_merchant():
#     merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
#     data_reader = MerchantData(merchant_data_path)
#     transaction_per_merchant = data_reader.get_transaction_per_merchant()
#     plotlyvisualize.histogram(transaction_per_merchant)

def no_transaction_per_day():
    merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    data_reader = MerchantData(merchant_data_path)
    transaction_per_day = data_reader.get_transaction_per_day()
    plotlyvisualize.histogram(transaction_per_day)


def no_transaction_vs_harmonic_based_on_guild(merchant_data_df, n_guild=10):
    # Data Read
    # merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    # merchant_data_reader = MerchantData(merchant_data_path)
    # merchant_data_df = merchant_data_reader.all_processed_dataframe()

    crm_data_path = os.path.join(DATASET_DIR, "CRM-Senf-Merchant.xlsx")
    crm_data_reader = CRMData(crm_data_path)
    crm_df = crm_data_reader.get_all_data()

    crm_merchant_df = CRMMerchant(crm_df, merchant_data_df)
    guild_names_serie = crm_merchant_df.get_guild_names()

    top_guild_codes = crm_merchant_df.get_top_guilds_codes(n=n_guild)

    guild_transaction_harmonic_df = crm_merchant_df.get_guild_transaction_harmonic_df()

    guild_name_code_map = guild_names_serie.to_dict()
    data = [ ]
    for code in top_guild_codes:
        data.append({"df": guild_transaction_harmonic_df[guild_transaction_harmonic_df["senf_code"] == code][
            ["all_transactions", "harmonic"]], "senf_name": guild_name_code_map[ code ]})

    return plotlyvisualize.scatter_by_guild(data, columns=("all_transactions", "harmonic"), title="بخش بندی بر اساس تازگی تراکنش ها",
                                     labels=("No Transactions", "Harmonic Sum"),
                                     out_path=PLOT_OUT_DIR)


def no_transaction_vs_sum_amounts_based_on_guild(merchant_data_df, n_guild=10):
    # Data Read
    # merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    # merchant_data_reader = MerchantData(merchant_data_path)
    # merchant_data_df = merchant_data_reader.all_processed_dataframe()

    crm_data_path = os.path.join(DATASET_DIR, "CRM-Senf-Merchant.xlsx")
    crm_data_reader = CRMData(crm_data_path)
    crm_df = crm_data_reader.get_all_data()

    crm_merchant_df = CRMMerchant(crm_df, merchant_data_df)
    guild_names_serie = crm_merchant_df.get_guild_names()

    top_guild_codes = crm_merchant_df.get_top_guilds_codes(n=n_guild)

    guild_transaction_sum_amounts_df = crm_merchant_df.get_guild_transaction_sum_amounts_df()

    #print(guild_transaction_sum_amounts_df["city_name"])

    guild_name_code_map = guild_names_serie.to_dict()
    data = [ ]
    for code in top_guild_codes:
        data.append({"df": guild_transaction_sum_amounts_df[ guild_transaction_sum_amounts_df[ "senf_code" ] == code ][
            [ "all_transactions", "sum_amounts", "city_name" ] ], "senf_name": guild_name_code_map[ code ]})

    #print(data[0]["df"])
    return plotlyvisualize.scatter_by_guild(data, columns=("all_transactions", "sum_amounts"), title="Transaction VS Sum Amounts",
                                     labels=("No Transactions", "Sum Amounts"),
                                     out_path=PLOT_OUT_DIR)


#no_transaction_vs_sum_amounts_based_on_guild(n_senf=40)
# no_transaction_vs_harmonic()
#no_transactions_vs_sum_amounts()
# no_transaction_per_day()
