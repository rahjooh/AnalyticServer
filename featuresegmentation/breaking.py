import json
from jenks import jenks
#from featuresegmentation.jenks import jenks
from readdata.merchantdata import MerchantData
import matplotlib.pyplot as plt
import os
from visualization import plotlyvisualize
import numpy as np


DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"


class Breaking(object):
    def __init__(self, merchant_data_df):
        X = merchant_data_df.as_matrix().astype(np.float)
        self._merchant_data = self.reject_outliers(X, [0, 1, 2])

    @staticmethod
    def reject_outliers(data, features):
        data = np.array(data)
        n_data_points = data.shape[0]
        non_outlier_indices = abs(data - np.mean(data, 0)) < 2 * np.std(data, 0)
        indices = np.ones((n_data_points), dtype=bool)
        for feature in features:
            indices = np.logical_and(indices, non_outlier_indices[ :, feature ])
        return data[ indices, : ]

    # def _merchant_data(self, ):
    #     """
    #     :return: clean data of merchant data
    #     :rtype:
    #     """
    #     merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    #     data_reader = MerchantData(merchant_data_path)
    #     return data_reader.get_clean_data()

    def get_sum_amounts_segments(self, n_breaks=10, visualize=False):
        """
        segments the sum amounts array
        :param n_breaks: number of segments to break for jenks algorithm
        :type n_breaks: int
        :param visualize: to visualize the output
        :type visualize: bool
        :return: the array containing the numbers in the list for breaking
        :rtype: numpy.core.numeric.array
        """
        x = self._merchant_data

        # Data Selection
        sum_amounts = x[:, 2].tolist()  # Money
        sum_amounts.sort()
        sum_amounts_breaks = jenks(sum_amounts, n_breaks)

        if visualize:
            plotlyvisualize.segments_plot(sum_amounts,
                                          vertical_lines=sum_amounts_breaks,
                                          title="Segmentations of Sum Amount With Jenks Natural Breaks",
                                          out_path=PLOT_OUT_DIR)
        return np.array(sum_amounts_breaks)

    def get_no_transaction_segments(self, n_breaks=10, visualize=False):
        """
        segments the number of transactions array
        :param n_breaks: number of segments to break for jenks algorithm
        :type n_breaks: int
        :param visualize: to visualize the output
        :type visualize: bool
        :return: the array containing the numbers in the list for breaking
        :rtype: numpy.core.numeric.array
        """
        x = self._merchant_data

        # Data Selection
        no_transaction = x[:, 1].tolist()  # Frequency
        no_transaction.sort()
        no_transactions_breaks = jenks(no_transaction, n_breaks)

        if visualize:
            plotlyvisualize.segments_plot(no_transaction,
                                          vertical_lines=no_transactions_breaks,
                                          title="Segmentations of Number of Transactions With Jenks Natural Breaks",
                                          out_path=PLOT_OUT_DIR)
        return np.array(no_transactions_breaks)

    def get_harmonic_segments(self, n_breaks=10, visualize=False):
        """
        segments the harmonic number calculated in dataframe
        :param n_breaks: number of segments to break for jenks algorithm
        :type n_breaks: int
        :param visualize: to visualize the output
        :type visualize: bool
        :return: the array containing the numbers in the list for breaking
        :rtype: numpy.core.numeric.array
        """
        x = self._merchant_data

        # Data Selection
        harmonic = x[:, 0].tolist()  # Recency
        harmonic.sort()
        harmonic_breaks = jenks(harmonic, n_breaks)

        if visualize:
            plotlyvisualize.segments_plot(harmonic,
                                          vertical_lines=harmonic_breaks,
                                          title="Segmentations of Harmonic sum With Jenks Natural Breaks",
                                          out_path=PLOT_OUT_DIR)
        return np.array(harmonic_breaks)


if __name__ == "__main__":
    breaking = Breaking()
    breaking.get_sum_amounts_segments(n_breaks=10, visualize=True)
    #breaking.get_no_transaction_segments(n_breaks=10, visualize=True)
    #breaking.get_harmonic_segments(n_breaks=10, visualize=True)
