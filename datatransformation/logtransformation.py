import os
import numpy as np
from readdata.merchantdata import MerchantData
from timing import timethis
from visualization import plotlyvisualize
import pandas as pd
from sklearn.cluster import KMeans

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"


class LogTransformation(object):
    """
    The idea behind this class is to Transform data into
    another space which is the log of the original space.
    Because data is assumed to be distributed like exponential dist.
    it's natural to take logs
    References:
        https://en.wikipedia.org/wiki/Data_transformation_(statistics)
    """
    @timethis
    def __init__(self, merchant_data_df):
        #merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
        self._merchant_df = merchant_data_df#MerchantData(merchant_data_path)

    @timethis
    def _get_merchant_df(self, ):
        return self._merchant_df

    @timethis
    def get_log_no_transactions(self):
        """
        :return: natural logarithm of number of transactions
        :rtype: pandas.core.series.Series
        """
        merchant_df = self._get_merchant_df()
        no_transactions_series = merchant_df[ "all_transactions" ]
        log_no_transactions_series = np.log(no_transactions_series)
        #log_no_transactions_series = (log_no_transactions_series - np.mean(log_no_transactions_series))/2*np.std(log_no_transactions_series)
        return log_no_transactions_series

    @timethis
    def get_log_sum_amounts(self):
        """
        :return: natural logarithm of sum amounts 
        :rtype: pandas.core.series.Series
        """
        merchant_df = self._get_merchant_df()
        sum_amount_series = merchant_df["sum_amounts"]
        log_sum_amount_series = np.log(sum_amount_series)
        #log_sum_amount_series = (log_sum_amount_series-np.mean(log_sum_amount_series))/2*np.std(log_sum_amount_series)
        return log_sum_amount_series

    @timethis
    def get_log_harmonic(self):
        merchant_df = self._get_merchant_df()
        harmonic_series = merchant_df["harmonic"]
        log_harmonic_series = np.log(harmonic_series)
        return log_harmonic_series

    @timethis
    def get_no_transactions_vs_sum_amounts_df(self):
        """
        :return: dataframe of number of all transactions and sum amounts
        :rtype: pandas.core.frame.DataFrame
        """
        # frames = [self.data_reader.get_sum_transactions_series(),
        #           self.data_reader.get_sum_amounts_series()]
        #selected_data = pd.concat(frames, axis=1)
        #selected_data.columns = ["all_transactions", "sum_amounts"]
        #return selected_data
        return self._merchant_df[["all_transactions", "sum_amounts"]]

    def get_all_merchants_df(self):
        # frames = [self.data_reader.get_sum_transactions_series(),
        #            self.data_reader.get_sum_amounts_series(),
        #            self.data_reader.get_harmonic_series()]
        # selected_data = pd.concat(frames, axis=1)
        # selected_data.columns = ["all_transactions", "sum_amounts", "harmonic"]
        # return selected_data
        return self._merchant_df

    @timethis
    def get_log_log_transactions_vs_sum_amount(self, visualize=False):
        """
        :return: the tuple of logarithm of number of all transactions and lograithm of sum amounts
        :rtype: tuple
        :param visualize: to visualize results or not
        :type visualize: bool
        """
        log_no_transactions_series = self.get_log_no_transactions()
        log_sum_amount_series = self.get_log_sum_amounts()

        if visualize:
            return plotlyvisualize.scatter(log_no_transactions_series,
                                    log_sum_amount_series,
                                    title="Log Log No Transactions vs Sum Amounts",
                                    axis_labels=("Log No Transactions", "Log Sum Amounts"),
                                    out_path=PLOT_OUT_DIR)

        return log_no_transactions_series, log_sum_amount_series

    @timethis
    def kmeans_no_transactions_sum_amounts(self, num_clusters=2, visualize=False, visualize_log=False):
        """
        This function use kmeans to cluster sum amount and number of transactions in log transformed space
        :param num_clusters: number of clusters for kmeans
        :type num_clusters: int
        :param visualize: to visualize in real scale space or not
        :type visualize: bool
        :param visualize_log: to visualize in log space or not 
        :type visualize_log: bool
        :return: labels to each datapoint
        :rtype: pandas.core.series.Series
        """
        random_state = 170
        log_no_transactions_series = self.get_log_no_transactions()
        log_sum_amounts_series = self.get_log_sum_amounts()
        frames = [log_no_transactions_series, log_sum_amounts_series]
        log_log_no_transactions_vs_sum_amounts_df = pd.concat(frames, axis=1)
        X = log_log_no_transactions_vs_sum_amounts_df.as_matrix().astype(np.float)
        y_pred = KMeans(n_clusters=num_clusters, random_state=random_state).fit_predict(X)
        labels_series = pd.Series(data=y_pred, index=log_log_no_transactions_vs_sum_amounts_df.index)

        log_log_no_transactions_vs_sum_amounts_df["labels"] = labels_series
        log_traces = []
        for cluster_num in range(num_clusters):
            log_traces.append(
                log_log_no_transactions_vs_sum_amounts_df[log_log_no_transactions_vs_sum_amounts_df["labels"]==cluster_num]
            )

        no_transactions_vs_sum_amounts_df = self.get_no_transactions_vs_sum_amounts_df()
        no_transactions_vs_sum_amounts_df["labels"] = labels_series
        traces = [ ]
        for cluster_num in range(num_clusters):
            traces.append(
                no_transactions_vs_sum_amounts_df[
                    no_transactions_vs_sum_amounts_df[ "labels" ] == cluster_num ]
            )

        if visualize_log:
            return plotlyvisualize.scatter_by_cluster(log_traces, columns=["all_transactions", "sum_amounts", "labels"],
                                               title="Kmeans for LogLog Scale",
                                               axis_labels=["Log No Transacions", "Log Sum Amounts"],
                                               out_path=PLOT_OUT_DIR)

        if visualize:
            return plotlyvisualize.scatter_by_cluster(traces, columns=[ "all_transactions", "sum_amounts", "labels" ],
                                               title="Kmeans for Real Scale",
                                               axis_labels=[ "No Transacions", "Sum Amounts" ],
                                               out_path=PLOT_OUT_DIR)


        return labels_series

    @timethis
    def kmeans(self, num_clusters=2, visualize_real_scale=False, visualize_log_scale=False):
        """
        This function use kmeans to cluster sum amount and number of transactions in log transformed space
        :param num_clusters: number of clusters for kmeans
        :type num_clusters: int
        :param visualize_real_scale: to visualize_real_scale in real scale space or not
        :type visualize_real_scale: bool
        :param visualize_log_scale: to visualize_real_scale in log space or not 
        :type visualize_log_scale: bool
        :return: labels to each datapoint
        :rtype: pandas.core.series.Series
        """
        random_state = 170
        log_no_transactions_series = self.get_log_no_transactions()
        log_sum_amounts_series = self.get_log_sum_amounts()
        log_harmonic_series = self.get_log_harmonic()
        frames = [ log_no_transactions_series, log_sum_amounts_series , log_harmonic_series]
        log_all_merchants_df = pd.concat(frames, axis=1)
        X = log_all_merchants_df.as_matrix().astype(np.float)
        y_pred = KMeans(n_clusters=num_clusters, random_state=random_state).fit_predict(X)
        labels_series = pd.Series(data=y_pred, index=log_all_merchants_df.index)

        log_all_merchants_df["labels"] = labels_series
        log_traces = []
        for cluster_num in range(num_clusters):
            log_traces.append(
                log_all_merchants_df[
                    log_all_merchants_df["labels"] == cluster_num ]
            )

        all_merchants_df = self.get_all_merchants_df()
        all_merchants_df["labels" ] = labels_series
        traces = [ ]
        for cluster_num in range(num_clusters):
            traces.append(
                all_merchants_df[
                    all_merchants_df[ "labels" ] == cluster_num ]
            )

        if visualize_log_scale:
            return plotlyvisualize.scatter_by_cluster(log_traces, columns=[ "all_transactions", "sum_amounts", "labels" ],
                                               title="Kmeans for LogLog Scale",
                                               axis_labels=[ "Log No Transacions", "Log Sum Amounts" ],
                                               out_path=PLOT_OUT_DIR)

        if visualize_real_scale:
            return plotlyvisualize.scatter3d(traces, columns=["all_transactions", "sum_amounts", "harmonic", "labels"],
                                      title="Kmeans for Real Scale",
                                      out_path=PLOT_OUT_DIR)

        return all_merchants_df

if __name__ == "__main__":
    log_transformation = LogTransformation()
    #log_transformation.get_log_log_transactions_vs_sum_amount(visualize=True)
    #log_transformation.kmeans_no_transactions_sum_amounts(num_clusters=5, visualize=True, visualize_log=True)
    log_transformation.kmeans(num_clusters=5, visualize_real_scale=True)
