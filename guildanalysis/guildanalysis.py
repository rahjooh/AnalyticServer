import os
import pandas as pd
from clusteranalysis.clusternumbering import ClusterNumbering
from datatransformation.boxcoxtransformation import BoxCoxTransformation
from featuresegmentation.labeling import Labeling
from readdata.crmdata import CRMData
from readdata.crmmerchant import CRMMerchant
from readdata.merchantdata import MerchantData
from timing import timethis
from visualization import plotlyvisualize
from datatransformation.logtransformation import LogTransformation
import numpy as np


DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"

class GuildAnalysis(object):
    def __init__(self, merchant_data_df):
        if merchant_data_df is None:
            merchant_data_path = os.path.join(DATASET_DIR, "Merchants-PerDay-GRTOneThousand.txt")
            merchant_data_reader = MerchantData(merchant_data_path)
            self._merchant_data_df = merchant_data_reader.all_processed_dataframe()
            self._merchant_data_df = merchant_data_df.set_index("merchant_number")
            print(self._merchant_data_df)
        else:
            self._merchant_data_df = merchant_data_df
        #print(self._merchant_data_df)
        crm_data_path = os.path.join(DATASET_DIR, "CRM-Senf-Merchant.xlsx")
        self.crm_data_reader = CRMData(crm_data_path)
        self._crm_df = self.crm_data_reader.get_all_data()
        print(self._crm_df)
        self._crm_merchant_df = CRMMerchant(self._crm_df, self._merchant_data_df)

    @timethis
    def top_guilds_vs_cluster_number(self, n_clusters=5, n_top_guilds=10):
        cluster_numbering = ClusterNumbering()
        guild_code_series = self.crm_data_reader.get_guild_code_series()
        #labeling = Labeling()
        log_transformation = LogTransformation(self._merchant_data_df)
        #box_cox_transformation = BoxCoxTransformation()

        #all_merchant_labels_df = labeling.kmeans(num_clusters=5)
        all_merchant_labels_df = log_transformation.kmeans(num_clusters=5)
        #all_merchant_labels_df = box_cox_transformation.kmeans(num_clusters=5)#labeling.kmeans(num_clusters=10)

        all_merchant_labels_df = cluster_numbering.renumber(all_merchant_labels_df)
        #guild_cluster_number_df = pd.concat([ all_merchant_labels_df, guild_code_series ], axis=1, join='inner')
        guild_cluster_number_df = all_merchant_labels_df.join(guild_code_series)
        guild_cluster_number_count = guild_cluster_number_df.groupby([ "labels", "senf_code" ]).count()["sum_amounts"]
        guild_cluster_number_count = guild_cluster_number_count.unstack()
        guild_cluster_number_count.fillna(0, inplace=True)



        guild_names_serie = self._crm_merchant_df.get_guild_names()
        guild_names_code_map = guild_names_serie.to_dict()
        print("---------------------------")
        print(guild_code_series)
        top_guilds_codes = self._crm_merchant_df.get_top_guilds_codes(n=n_top_guilds)
        print(top_guilds_codes)
        top_guilds_names = [guild_names_code_map[code] for code in top_guilds_codes]
        print(top_guilds_names)

        filtered_guild_cluster_number_count = guild_cluster_number_count[top_guilds_codes]
        #print(guild_cluster_number_count)
        #filtered_guild_cluster_number_count = [guild_cluster_number_count[code] for code in top_guilds_codes]
        #filtered_guild_cluster_number_count = filtered_guild_cluster_number_count.div(filtered_guild_cluster_number_count.sum(axis=1), axis=0)

        filtered_guild_cluster_number_count = (filtered_guild_cluster_number_count - filtered_guild_cluster_number_count.mean())/(filtered_guild_cluster_number_count.max() - filtered_guild_cluster_number_count.min())
        print(filtered_guild_cluster_number_count)
        print(top_guilds_names)
        print(guild_cluster_number_count.index.values)
        return plotlyvisualize.heamap(z=filtered_guild_cluster_number_count.values,
                               x=top_guilds_names,
                               y=guild_cluster_number_count.index.values,
                               title="zHeatmap",
                               out_path=PLOT_OUT_DIR)
        # print(a)
        # a.to_csv("a.csv")
        # guild_cluster_number_count.to_csv("guild.csv")


if __name__ == "__main__":
    guild_analysis = GuildAnalysis(None)
    guild_analysis.top_guilds_vs_cluster_number(n_clusters=5, n_top_guilds=4)