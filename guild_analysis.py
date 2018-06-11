import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn import datasets
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn import preprocessing
from readdata.crmmerchant import CRMMerchant
from readdata.merchantdata import MerchantData
from readdata.crmdata import CRMData
from visualization import plotlyvisualize
import os

DATASET_DIR = "dataset"
PLOT_OUT_DIR = "plotsout"

# Data Read
merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
merchant_data_reader = MerchantData(merchant_data_path)
merchant_df = merchant_data_reader.all_processed_dataframe()

crm_data_path = os.path.join(DATASET_DIR, "crm.xlsx")
crm_data_reader = CRMData(crm_data_path)
crm_df = crm_data_reader.get_all_data()

crm_merchant_df = CRMMerchant(crm_df, merchant_df)

# harmonic_amount = crm_merchant_df[["harmonic"]].as_matrix().astype(np.float)
# no_transaction = crm_merchant_df[["all_transactions"]].as_matrix().astype(np.float)
# sum_amounts = crm_merchant_df[["sum_amounts"]].as_matrix().astype(np.float)
# senf_code = crm_merchant_df["senf_code"].tolist()
# plotlyvisualize.histogram(senf_code)

mm = crm_data_reader.get_guild_series()
guild_all_amounts = crm_merchant_df.get_guild_all_amounts()
guild_all_transactions = crm_merchant_df.get_guild_all_transactions_series()
guild_names_serie = crm_merchant_df.get_guild_names()

# Remove the Max item from all
index_of_max = guild_all_transactions.idxmax()
#guild_all_transactions = guild_all_transactions.drop(index_of_max)



guild_codes = crm_merchant_df.get_all_guild_codes()
guild_name_code_map = guild_names_serie.to_dict()
guild_names = [ guild_name_code_map[code ] for code in guild_codes ]

# Filter base on Top senfs for number of transactions
top_guild_codes = crm_merchant_df.get_top_guilds_codes(n=20)
filtered_top_guild_names = [ guild_name_code_map[code ] for code in guild_codes if code in top_guild_codes and code != index_of_max ]
filtered_top_guild_all_transactions = [ guild_all_transactions[code ] for code in guild_codes if code in top_guild_codes and code != index_of_max ]
plotlyvisualize.bar_chart_plot(filtered_top_guild_names, filtered_top_guild_all_transactions, title="Guild vs Number of Transactions", out_path=PLOT_OUT_DIR)



# Filter base on Top senfs for sum of sum amounts
filtered_top_guild_all_transactions = [ guild_all_amounts[code ] / 10 for code in guild_codes if code in top_guild_codes and code != index_of_max ]
plotlyvisualize.bar_chart_plot(filtered_top_guild_names, filtered_top_guild_all_transactions, title="Guild vs Amount", out_path=PLOT_OUT_DIR)

# # Plotting
# title = "Trans vs Harmonic"
# labels = ("No Transaction", "Harmonic Sum")
# plotlyvisualize.scatter(no_transaction, harmonic_amount, title, labels, out_path= PLOT_OUT_DIR)
#
# transaction_per_day = merchant_data_reader.get_transaction_per_day()
# print(transaction_per_day)
# #plotlyvisualize.histogram(transaction_per_day)
#
#
# transaction_per_merchant = merchant_data_reader.get_transaction_per_merchant()
# print(transaction_per_merchant)
# #plotlyvisualize.histogram(transaction_per_merchant)
#
#
# # print(np.corrcoef(no_transaction, sum_amounts))