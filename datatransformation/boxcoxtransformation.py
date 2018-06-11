import os
import numpy as np
from sklearn.preprocessing.data import MinMaxScaler
from readdata.merchantdata import MerchantData
from timing import timethis
from visualization import plotlyvisualize
import pandas as pd
from sklearn.cluster import KMeans
from scipy import stats

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"


class BoxCoxTransformation(object):
    """
    The idea behind this class is to Transform data into
    another space which is the log of the original space.
    Because data is assumed to be distributed like exponential dist.
    it's natural to take logs
    References:
        https://en.wikipedia.org/wiki/Data_transformation_(statistics)
    """
    @timethis
    def __init__(self):
        merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
        self.data_reader = MerchantData(merchant_data_path)
        self.scaler = MinMaxScaler()

    @timethis
    def _merchant_df(self, ):
        return self.data_reader.selected_dataframe()

    @timethis
    def get_boxcox_no_transactions(self):
        """
        :return: natural logarithm of number of transactions
        :rtype: pandas.core.series.Series
        """
        merchant_df = self._merchant_df()
        no_transactions_series = merchant_df[ "all_transactions" ]
        #no_transactions_series = pd.Series(data=preprocessing.normalize(no_transactions_series),
         #                                  index=no_transactions_series.index)
        #print(no_transactions_series)
        boxcox_no_transactions, _ = stats.boxcox(no_transactions_series)
        boxcox_no_transactions_series = pd.Series(data=boxcox_no_transactions, index=no_transactions_series.index)
        return boxcox_no_transactions_series

    @timethis
    def get_boxcox_sum_amounts(self):
        """
        :return: natural logarithm of sum amounts 
        :rtype: pandas.core.series.Series
        """
        merchant_df = self._merchant_df()
        sum_amount_series = merchant_df["sum_amounts"]
        #sum_amount_series = pd.Series(data=self.scaler.fit_transform(sum_amount_series),
        #                              index=sum_amount_series.index)
        boxcox_sum_amount, _ = stats.boxcox(sum_amount_series)
        boxcox_sum_amount_series = pd.Series(data=boxcox_sum_amount, index=sum_amount_series.index)
        return boxcox_sum_amount_series

    @timethis
    def get_boxcox_harmonic(self):
        merchant_df = self._merchant_df()
        harmonic_series = merchant_df["harmonic"]
        boxcox_harmonic, _ = stats.boxcox(harmonic_series)
        boxcox_harmonic_series = pd.Series(data=boxcox_harmonic, index=harmonic_series.index)
        return boxcox_harmonic_series

    @timethis
    def kolmogrov_smirnov_on_no_transactions_test(self):
        boxcox_no_transactions = self.get_boxcox_no_transactions()
        #return stats.kstest(boxcox_no_transactions, 'norm')
        return stats.shapiro(boxcox_no_transactions[0:4000])

    @timethis
    def kolmogrov_smirnov_on_sum_amounts_test(self):
        boxcox_sum_amounts = self.get_boxcox_sum_amounts()
        #return stats.kstest(boxcox_sum_amounts, 'norm')
        return stats.shapiro(boxcox_sum_amounts[0:4000])

    @timethis
    def get_no_transactions_vs_sum_amounts_df(self):
        """
        :return: dataframe of number of all transactions and sum amounts
        :rtype: pandas.core.frame.DataFrame
        """
        frames = [self.data_reader.get_sum_transactions_series(),
                  self.data_reader.get_sum_amounts_series()]
        selected_data = pd.concat(frames, axis=1)
        selected_data.columns = ["all_transactions", "sum_amounts"]
        return selected_data

    def get_all_merchants_df(self):
        frames = [ self.data_reader.get_sum_transactions_series(),
                   self.data_reader.get_sum_amounts_series(),
                   self.data_reader.get_harmonic_series()]
        selected_data = pd.concat(frames, axis=1)
        selected_data.columns = [ "all_transactions", "sum_amounts", "harmonic" ]
        return selected_data

    @timethis
    def get_boxcox_transactions_vs_sum_amount(self, visualize=False):
        """
        :return: the tuple of logarithm of number of all transactions and lograithm of sum amounts
        :rtype: tuple
        :param visualize: to visualize results or not
        :type visualize: bool
        """
        log_no_transactions_series = self.get_boxcox_no_transactions()
        log_sum_amount_series = self.get_boxcox_sum_amounts()

        if visualize:
            plotlyvisualize.scatter(log_no_transactions_series,
                                    log_sum_amount_series,
                                    title="Box Cox Transformation for No Transactions vs Sum Amounts",
                                    axis_labels=("BoxCox No Transactions", "BoxCox Sum Amounts"),
                                    out_path=PLOT_OUT_DIR)

        return log_no_transactions_series, log_sum_amount_series

    @timethis
    def kmeans_no_transactions_sum_amounts(self, num_clusters=2, visualize_real_scale=False, visualize_boxcox_scale=False):
        """
        This function use kmeans to cluster sum amount and number of transactions in log transformed space
        :param num_clusters: number of clusters for kmeans
        :type num_clusters: int
        :param visualize_real_scale: to visualize in real scale space or not
        :type visualize_real_scale: bool
        :param visualize_boxcox_scale: to visualize in log space or not 
        :type visualize_boxcox_scale: bool
        :return: labels to each datapoint
        :rtype: pandas.core.series.Series
        """
        random_state = 170
        boxcox_no_transactions_series = self.get_boxcox_no_transactions()
        boxcox_sum_amounts_series = self.get_boxcox_sum_amounts()
        frames = [boxcox_no_transactions_series, boxcox_sum_amounts_series]
        boxcox_no_transactions_vs_sum_amounts_df = pd.concat(frames, axis=1)
        boxcox_no_transactions_vs_sum_amounts_df.columns = [ 'all_transactions', 'sum_amounts' ]
        X = boxcox_no_transactions_vs_sum_amounts_df.as_matrix().astype(np.float)
        y_pred = KMeans(n_clusters=num_clusters, random_state=random_state).fit_predict(X)
        labels_series = pd.Series(data=y_pred, index=boxcox_no_transactions_vs_sum_amounts_df.index)

        boxcox_no_transactions_vs_sum_amounts_df["labels"] = labels_series
        boxcox_traces = []
        for cluster_num in range(num_clusters):
            boxcox_traces.append(
                boxcox_no_transactions_vs_sum_amounts_df[boxcox_no_transactions_vs_sum_amounts_df["labels"]==cluster_num]
            )

        no_transactions_vs_sum_amounts_df = self.get_no_transactions_vs_sum_amounts_df()
        no_transactions_vs_sum_amounts_df["labels"] = labels_series
        traces = [ ]
        for cluster_num in range(num_clusters):
            traces.append(
                no_transactions_vs_sum_amounts_df[
                    no_transactions_vs_sum_amounts_df[ "labels" ] == cluster_num ]
            )

        if visualize_boxcox_scale:
            plotlyvisualize.scatter_by_cluster(boxcox_traces, columns=[ "all_transactions", "sum_amounts", "labels" ],
                                               title="Kmeans for BoxCox Scale",
                                               axis_labels=[ "BoxCox No Transacions", "BoxCox Sum Amounts" ],
                                               out_path=PLOT_OUT_DIR)

        if visualize_real_scale:
            plotlyvisualize.scatter_by_cluster(traces, columns=[ "all_transactions", "sum_amounts", "labels" ],
                                               title="Kmeans for Real Scale",
                                               axis_labels=[ "No Transacions", "Sum Amounts" ],
                                               out_path=PLOT_OUT_DIR)

        return no_transactions_vs_sum_amounts_df

    @timethis
    def kmeans(self, num_clusters=2, visualize_real_scale=False, visualize_boxcox_scale=False):
        random_state = 170
        boxcox_no_transactions_series = self.get_boxcox_no_transactions()
        boxcox_sum_amounts_series = self.get_boxcox_sum_amounts()
        boxcox_harmoinc_series = self.get_boxcox_harmonic()
        frames = [ boxcox_no_transactions_series, boxcox_sum_amounts_series, boxcox_harmoinc_series ]
        boxcox_all_merchants_df = pd.concat(frames, axis=1)
        boxcox_all_merchants_df.columns = [ "all_transactions", "sum_amounts", "harmonic" ]
        X = boxcox_all_merchants_df.as_matrix().astype(np.float)
        y_pred = KMeans(n_clusters=num_clusters, random_state=random_state).fit_predict(X)
        labels_series = pd.Series(data=y_pred, index=boxcox_all_merchants_df.index)

        boxcox_all_merchants_df[ "labels" ] = labels_series
        boxcox_traces = [ ]
        for cluster_num in range(num_clusters):
            boxcox_traces.append(
                boxcox_all_merchants_df[
                    boxcox_all_merchants_df[ "labels" ] == cluster_num ]
            )

        all_merchants_df = self.get_all_merchants_df()
        all_merchants_df["labels"] = labels_series
        traces = []
        for cluster_num in range(num_clusters):
            traces.append(
                all_merchants_df[
                    all_merchants_df["labels"] == cluster_num ]
            )

        if visualize_boxcox_scale:
            plotlyvisualize.scatter3d(boxcox_traces, columns=["all_transactions", "sum_amounts", "harmonic", "labels"],
                                               title="Kmeans for BoxCox Scale",
                                               out_path=PLOT_OUT_DIR)

        if visualize_real_scale:
            plotlyvisualize.scatter3d(traces, columns=["all_transactions", "sum_amounts", "harmonic", "labels"],
                                      title="Kmeans for Real Scale",
                                      out_path=PLOT_OUT_DIR)

        return all_merchants_df


if __name__ == "__main__":
    boxcox_transformation = BoxCoxTransformation()
    boxcox_transformation.kmeans(num_clusters=5, visualize_real_scale=True)
    #boxcox_transformation.get_boxcox_transactions_vs_sum_amount(visualize=True)
    #log_transformation.get_log_no_transactions()
    #boxcox_transformation.kmeans(num_clusters=10, visualize=True, visualize_boxcox=True)
    #print(boxcox_transformation.kolmogrov_smirnov_on_no_transactions_test())
    #print(boxcox_transformation.kolmogrov_smirnov_on_sum_amounts_test())
