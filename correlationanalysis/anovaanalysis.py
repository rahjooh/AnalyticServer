import os

from readdata.crmdata import CRMData
from readdata.crmmerchant import CRMMerchant
from readdata.merchantdata import MerchantData
from visualization import plotlyvisualize
import numpy as np
import scipy.stats as stats

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"

class ANOVAAnalysis(object):
    def __init__(self):
        crm_data_path = os.path.join(DATASET_DIR, "crm.xlsx")
        crm_data_reader = CRMData(crm_data_path)
        crm_df = crm_data_reader.get_all_data()

        merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
        merchant_data_reader = MerchantData(merchant_data_path)
        merchant_df = merchant_data_reader.all_processed_dataframe()

        self.data_reader = CRMMerchant(crm_df, merchant_df)

    def visualize_guilds_boxplot(self):
        guild_all_transactions_series = self.data_reader.get_guild_all_transactions_list_series()
        top_guild_codes = self.data_reader.get_top_guilds_codes(10)
        guild_names_series = self.data_reader.get_guild_names()
        guild_names_map = guild_names_series.to_dict()
        guild_names = [ guild_names_map[ code ] for code in top_guild_codes]
        print(guild_all_transactions_series[top_guild_codes].values[0])

        plotlyvisualize.boxplot(list(range(3)),
                                guild_all_transactions_series[ top_guild_codes ].values[0:3],
                                title="ANOVA Analysis",
                                out_path=PLOT_OUT_DIR)

    def anova(self):
        pass

if __name__ == "__main__":
    #anova_analysis = ANOVAAnalysis()
    #anova_analysis.visualize_guilds_boxplot()
    print(stats.f_oneway([ 1, 2, 3, 4 ], [ 1, 2, 3, 4 ]))
