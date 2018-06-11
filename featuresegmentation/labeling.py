from pandas.core.frame import DataFrame
from sklearn.cluster import KMeans
from featuresegmentation.breaking import Breaking
import os
from featuresegmentation.hierarchicalsegmentation import HierarchicalSegmentation
from readdata.merchantdata import MerchantData
import numpy as np
from visualization import plotlyvisualize
import pandas as pd
from timing import timethis

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"


class Labeling(object):
    @timethis
    def __init__(self, merchant_data_df):
        #merchant_data_path = os.path.join(DATASET_DIR, "Merchants-PerDay-GRTOneThousand.txt")
        #self.data_reader = MerchantData(merchant_data_path)
        #self._breaking = Breaking(merchant_data_df)
        self._hierarchical_segmentation = HierarchicalSegmentation(merchant_data_df)
        self._merchant_data_df = merchant_data_df
        #self._merchant_data_df = self._get_merchant_data_df()

    # @timethis
    # def _get_merchant_data_df(self, ):
    #     """
    #     get the clean data of merchant data as numpy array
    #     :return: clean data of merchant data
    #     :rtype:
    #     """
    #     return self.data_reader.selected_dataframe()

    @timethis
    def get_sum_amount_labels(self, n_breaks=5):
        """
        get the labels of each break with an integer number from 0 to (number of clusters-1)
        :param n_breaks: number of breaks for labels
        :type n_breaks: int
        :return: sum amount labels
        :rtype: pandas.core.series.Series
        """
        sum_amounts_series = self._merchant_data_df[ "sum_amounts" ]

        sum_amounts_breaks = self._hierarchical_segmentation.segment_sum_amounts(n_breaks=n_breaks)

        sum_amount_labels_series = pd.Series(index=sum_amounts_series.index)
        for index, row in sum_amounts_series.iteritems():
            sum_amount_labels_series[index] = int(np.argmax(sum_amounts_breaks > row))

        #TODO quick patch: adding one to label numbers
        sum_amount_labels_series = sum_amount_labels_series + 1
        return sum_amount_labels_series

    @timethis
    def get_no_transactions_labels(self, n_breaks=10):
        """
        get the labels of each break with an integer number from 0 to (number of clusters-1)
        :param n_breaks: number of breaks for labels
        :type n_breaks: int
        :return: no transactions labels
        :rtype: pandas.core.series.Series
        """
        no_transactions_series = self._merchant_data_df[ "all_transactions" ]

        no_transactions_breaks = self._hierarchical_segmentation.segment_no_transactions(n_breaks=n_breaks)

        no_transaction_labels_series = pd.Series(index=no_transactions_series.index)
        for index, row in no_transactions_series.iteritems():
            no_transaction_labels_series[ index ] = int(np.argmax(no_transactions_breaks > row))

        # TODO quick patch: adding one to label numbers
        no_transaction_labels_series = no_transaction_labels_series + 1
        return no_transaction_labels_series

    @timethis
    def get_harmonic_labels(self, n_breaks=10):
        """
        get the labels of each break with an integer number from 0 to (number of clusters-1)
        :param n_breaks: number of breaks for labels
        :type n_breaks: int
        :return: harmonic amount labels
        :rtype: pandas.core.series.Series
        """
        harmonic_series = self._merchant_data_df[ "harmonic" ]

        harmonic_breaks = self._hierarchical_segmentation.segment_harmonic(n_breaks=n_breaks)

        harmonic_labels_series = pd.Series(index=harmonic_series.index)
        for index, row in harmonic_series.iteritems():
            harmonic_labels_series[ index ] = int(np.argmax(harmonic_breaks > row))

        # TODO quick patch: adding one to label numbers
        harmonic_labels_series = harmonic_labels_series + 1
        return harmonic_labels_series

    @timethis
    def get_merchant_labels_df(self):
        """
        This method concat the series of sum amount labels, no transacions labels 
        and harmonic labels to one unified dataframe that can be sorted grouped etc.
        :return: The Dataframe of labels of sum amounts, no_transactions and harmonic in the segmentations
        :rtype: DataFrame
        """
        sum_amount_labels_series = self.get_sum_amount_labels()
        no_transactions_labels_series = self.get_no_transactions_labels()
        harmonic_labels_series = self.get_harmonic_labels()
        frames = [sum_amount_labels_series, no_transactions_labels_series, harmonic_labels_series]
        merchant_labels_df = pd.concat(frames, axis=1)
        merchant_labels_df.columns = ["sum_amounts", "no_transactions", "harmonic"]
        return merchant_labels_df

    @timethis
    def get_weighted_merchant_df(self, sum_amount_weight = 4, no_transactions_weight = 2, harmonic_weight = 1):
        sum_amount_labels_series = self.get_sum_amount_labels()
        no_transactions_labels_series = self.get_no_transactions_labels()
        harmonic_labels_series = self.get_harmonic_labels()
        frames = [sum_amount_weight * sum_amount_labels_series,
                  no_transactions_weight * no_transactions_labels_series,
                  harmonic_weight * harmonic_labels_series]
        merchant_labels_df = pd.concat(frames, axis=1)
        merchant_labels_df.columns = ["sum_amounts", "no_transactions", "harmonic"]
        return merchant_labels_df

    @timethis
    def save_all_merchants_to_csv(self):
        all_merchant_labels_df = self.get_weighted_merchant_df(sum_amount_weight=4,
                                               no_transactions_weight=2,
                                               harmonic_weight=1)
        #all_merchant_labels_df = all_merchant_labels_df.apply(lambda x: int(x))
        #all_merchant_labels_df = all_merchant_labels_df.as_type(int)
        out_file = os.path.join(DATASET_DIR, "all_merchants_df.csv")
        all_merchant_labels_df.to_csv(out_file)

    @timethis
    def kmeans(self, num_clusters=5, visualize=False):
        print(self._merchant_data_df.shape)
        all_merchant_labels_df = self.get_weighted_merchant_df(sum_amount_weight=4,
                                                               no_transactions_weight=2,
                                                               harmonic_weight=1)

        X = all_merchant_labels_df.as_matrix().astype(np.float)
        y_pred = KMeans(n_clusters=num_clusters).fit_predict(X)
        cluster_number_series = pd.Series(data=y_pred, index=all_merchant_labels_df.index)

        all_merchant_labels_df["labels"] = cluster_number_series
        #out_file = os.path.join(DATASET_DIR, "kmeans_result.csv")
        #all_merchant_labels_df.to_csv(out_file)
        #print(all_merchant_labels_df)
        kmeans_result_traces = []
        for cluster_num in range(num_clusters):
            kmeans_result_traces.append(
                all_merchant_labels_df[
                    all_merchant_labels_df["labels"] == cluster_num ]
            )
        #lengths = [len(x.index) for x in kmeans_result_traces]
        #print(lengths)

        if visualize:
            return plotlyvisualize.scatter3d(kmeans_result_traces, columns=["sum_amounts", "no_transactions", "harmonic" ],
                                               title="Kmeans for Real Scale4",
                                               out_path=PLOT_OUT_DIR)

        return all_merchant_labels_df

    @timethis
    def clusters_statistics(self, write_to_excel_file=False):
        number_of_clusters = 10
        labeling = Labeling()
        kmeans_result = labeling.kmeans(num_clusters=number_of_clusters)
        statistics_result = []
        for i in range(number_of_clusters):
            cluster_part = kmeans_result[kmeans_result["labels"] == i]
            cluster_part = cluster_part[["sum_amounts", "no_transactions", "harmonic"]]
            statistics_result.append(cluster_part.describe(include='all'))
        statistics_result_df = pd.concat(statistics_result, keys=list(range(number_of_clusters)))
        if write_to_excel_file:
            out_file = os.path.join(DATASET_DIR, "statistic_results.xlsx")
            statistics_result_df.to_excel(out_file)
        return statistics_result_df

    def visualize_labels(self):
        """
        this method count the number of each label(represents a cluster)
        and use stakced bar chart to visualzie the share of each label(cluster)
        in all the data
        """
        sum_amount_labels = labeling.get_sum_amount_labels()
        no_transactions_labels = labeling.get_no_transactions_labels()
        harmonic_labels = labeling.get_harmonic_labels()

        sum_amount_counts = [sum_amount_labels.count(label) for label in list(set(sum_amount_labels))]
        no_transactions_counts = [no_transactions_labels.count(label) for label in list(set(no_transactions_labels))]
        harmonic_counts = [harmonic_labels.count(label) for label in list(set(harmonic_labels))]

        features_counts = {"Sum Amount(Monetary)": sum_amount_counts,
                           "No Transactions(Frequency)": no_transactions_counts,
                           "Harmonic(Recency)": harmonic_counts,
                           }
        features_counts_df = DataFrame(features_counts, columns=["Sum Amount(Monetary)",
                                                                 "No Transactions(Frequency)",
                                                                 "Harmonic(Recency)"
                                                                  ])
        plotlyvisualize.stacked_bar_char(features_counts_df, "Label Counts", PLOT_OUT_DIR)


if __name__ == "__main__":
    labeling = Labeling()
    #labeling.visualize_labels()
    #all_merchant_labels_df = labeling.get_weighted_merchant_df(sum_amount_weight=4,
    #                                           no_transactions_weight=2,
    #                                           harmonic_weight=1)
    #print(all_merchant_labels_df)
    #labeling.save_all_merchants_to_csv()
    import time
    t1 = time.time()
    print(labeling.kmeans(num_clusters=5, visualize=True))
    t2 = time.time()
    print(t2-t1)
    #result = labeling.clusters_statistics(write_to_excel_file=True)
    #print(result)
    # for label in list(set(labels)):
    #     print(labels.count(label))
    #
    # labels = labeling.get_no_transactions_labels()
    # for label in list(set(labels)):
    #     print(labels.count(label))
    #
    # labels = labeling.get_harmonic_labels()
    # for label in list(set(labels)):
    #     print(labels.count(label))