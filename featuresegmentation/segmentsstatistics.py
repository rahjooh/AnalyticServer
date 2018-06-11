from featuresegmentation.hierarchicalsegmentation import HierarchicalSegmentation
from readdata.merchantdata import MerchantData
import os
import numpy as np
from functools import wraps

from visualization import plotlyvisualize

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"

class SegmentsStatistics():

    def __init__(self, merchant_data_df):
        self._hierarchical_segmentation = HierarchicalSegmentation()
        self.merchant_df = merchant_data_df


    # def _merchant_df(self, ):
    #     merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    #     data_reader = MerchantData(merchant_data_path)
    #     return data_reader.selected_dataframe()

    def get_sum_amounts_statistics(self, visualize=False):
        sum_amounts_breaks = self._hierarchical_segmentation.segment_sum_amounts(n_breaks=8, limit_percent=0.2)
        merchant_df = self.merchant_df
        sum_amount_series = merchant_df["sum_amounts"]

        sum_amounts_statistics = []
        sum_amounts_list = []
        size_of_each_segment = []
        for i in range(len(sum_amounts_breaks) - 1):
            sum_amounts = sum_amount_series[(sum_amount_series < sum_amounts_breaks[i + 1])
                                              & (sum_amount_series > sum_amounts_breaks[i])]
            sum_amounts_statistics.append(sum_amounts.describe())
            size_of_each_segment.append(len(sum_amounts))
            sum_amounts_list.append(sum_amounts)

        if visualize:
            return plotlyvisualize.boxplot(size_of_each_segment,
                                    sum_amounts_list,
                                    "Sum Amounts Statistics",
                                    PLOT_OUT_DIR)

        return sum_amounts_statistics

    def get_no_transactions_statistics(self, visualize=False):
        no_transactions_breaks = self._hierarchical_segmentation.segment_no_transactions(n_breaks=8, limit_percent=0.2)
        merchant_df = self.merchant_df
        no_transactions_series = merchant_df["all_transactions"]

        no_transactions_statistics = []
        no_transactions_list = []
        size_of_each_segment = []
        for i in range(len(no_transactions_breaks) - 1):
            no_transactions = no_transactions_series[(no_transactions_series < no_transactions_breaks[i + 1])
                                              & (no_transactions_series > no_transactions_breaks[i])]
            no_transactions_statistics.append(no_transactions.describe())
            size_of_each_segment.append(len(no_transactions))
            no_transactions_list.append(no_transactions)


        if visualize:
            plotlyvisualize.boxplot(size_of_each_segment,
                                    no_transactions_list,
                                    "No Transactions Statistics",
                                    PLOT_OUT_DIR)

        return no_transactions_statistics

    def get_harmonic_statistics(self, visualize=False):
        harmonic_breaks = self._hierarchical_segmentation.segment_harmonic(n_breaks=8, limit_percent=0.2)
        merchant_df = self.merchant_df
        sum_amount_series = merchant_df["harmonic"]

        harmonic_statistics = []
        harmonic_list = []
        size_of_each_segment = []
        for i in range(len(harmonic_breaks) - 1):
            sum_amounts = sum_amount_series[(sum_amount_series < harmonic_breaks[i + 1])
                                              & (sum_amount_series > harmonic_breaks[i])]
            harmonic_statistics.append(sum_amounts.describe())
            size_of_each_segment.append(len(sum_amounts))
            harmonic_list.append(sum_amounts)

        if visualize:
            plotlyvisualize.boxplot(size_of_each_segment,#np.arange(len(harmonic_list)),
                                    harmonic_list,
                                    "Harmonic Statistics")

        return harmonic_statistics


if __name__ == "__main__":
    segments_statistics = SegmentsStatistics()
    sum_amounts_statistics = segments_statistics.get_sum_amounts_statistics(visualize=True)
    print(sum_amounts_statistics)
    no_transactions_statistics = segments_statistics.get_no_transactions_statistics(visualize=True)
    print(no_transactions_statistics)
    harmonic_statistics = segments_statistics.get_harmonic_statistics(visualize=True)
    print(harmonic_statistics)
