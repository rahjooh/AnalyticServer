from featuresegmentation.breaking import Breaking
from readdata.merchantdata import MerchantData
import numpy as np
import pandas as pd
from jenks import jenks
import os

DATASET_DIR = "../dataset"
PLOT_OUT_DIR = "../plotsout"


class HierarchicalSegmentation(object):
    def __init__(self, merchant_data_df):
        self.breaking = Breaking(merchant_data_df)
        self.least_range_population = 200
        X = merchant_data_df.as_matrix().astype(np.float)
        self._merchant_data = self.reject_outliers(X, [0, 1, 2])

    # def _merchant_data(self, ):
    #     merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
    #     data_reader = MerchantData(merchant_data_path)
    #     return data_reader.get_clean_data()

    @staticmethod
    def reject_outliers(data, features):
        data = np.array(data)
        n_data_points = data.shape[ 0 ]
        non_outlier_indices = abs(data - np.mean(data, 0)) < 2 * np.std(data, 0)
        indices = np.ones((n_data_points), dtype=bool)
        for feature in features:
            indices = np.logical_and(indices, non_outlier_indices[ :, feature ])
        return data[ indices, : ]

    def _merge_ranges(self, data_series, breaks):
        """
        This function tries to merge adjacent ranges with low population together.
        to do this purpose algorithm tries to merge and find the size of merged ranges. 
        if the merge ranges exceeds the least_range_population it stopped otherwise will continue by recurring.
        merging will done by removing the unnecessary break from breaks
        :param data_series: data series to find the interval population
        :type data_series: pandas.core.series.Series
        :param breaks: the list of breaks for segmentation
        :type breaks: 
        :return: merged breaks containing breaks that only have high population(more than least_range_population)
        :rtype: 
        """
        for i in range(len(breaks) - 1):
            interval_population = data_series[ (data_series < breaks[ i + 1 ]) & (data_series > breaks[ i ]) ].size
            if interval_population < self.least_range_population:
                del breaks[i + 1]
                return self._merge_ranges(data_series, breaks)
        return breaks

    def segment_sum_amounts(self, n_breaks, limit_percent=0.2):
        """
        First use segments to create breaks for sum amounts
        and then merge those segments with the low population.
        :param n_breaks: number of breaks
        :type n_breaks: int
        :param limit_percent: the lowest amount of population in a segment
        :type limit_percent: float
        :return: breaks
        :rtype: 
        """
        x = self._merchant_data
        sum_amounts = x[:, 2]  # Money
        sum_amounts.sort()
        sum_amounts_breaks = self.segmentation(sum_amounts,
                                               n_breaks=n_breaks,
                                               all_breaks=[],
                                               limit=limit_percent * sum_amounts.size)
        r = self._merge_ranges(sum_amounts, sum_amounts_breaks)
        del r[0] # a weird quick patch
        return r#self._merge_ranges(sum_amounts, sum_amounts_breaks)

    def segment_no_transactions(self, n_breaks, limit_percent=0.2):
        """
        First use segments to create breaks for no transactions
        and then merge those segments with the low population.
        :param n_breaks: number of breaks
        :type n_breaks: int
        :param limit_percent: the lowest amount of population in a segment
        :type limit_percent: float
        :return: breaks
        :rtype: 
        """
        x = self._merchant_data
        no_transactions = x[:, 1]
        no_transactions.sort()
        no_transactions_breaks = self.segmentation(no_transactions,
                                                   n_breaks=n_breaks,
                                                   all_breaks=[ ],
                                                   limit=limit_percent * no_transactions.size)
        r = self._merge_ranges(no_transactions, no_transactions_breaks)
        del r[0]
        return r

    def segment_harmonic(self, n_breaks, limit_percent=0.2):
        """
        First use segments to create breaks for harmonic
        and then merge those segments with the low population.
        :param n_breaks: number of breaks
        :type n_breaks: int
        :param limit_percent: the lowest amount of population in a segment
        :type limit_percent: float
        :return: breaks
        :rtype: 
        """
        x = self._merchant_data
        harmonic = x[:, 0]
        harmonic.sort()
        harmonic_breaks = self.segmentation(harmonic,
                                            n_breaks=n_breaks,
                                            all_breaks=[ ],
                                            limit=int(limit_percent * harmonic.size))
        return self._merge_ranges(harmonic, harmonic_breaks)

    def _find_biggest_break(self, breaks):
        """

        :param breaks: 
        :type breaks: 
        :return: 
        :rtype: 
        """
        biggest_interval_length = 0
        start_interval = 0
        end_interval = 0
        for i in range(len(breaks) - 1):
            interval_length = breaks[ i + 1 ] - breaks[ i ]
            if interval_length > biggest_interval_length:
                biggest_interval_length = interval_length
                start_interval = breaks[ i ]
                end_interval = breaks[ i + 1 ]
        return start_interval, end_interval

    def _find_most_populous_break(self, breaks, data_series):
        """
        find the most populous break in the data sereies within each break
        :param breaks: breaks of all the population
        :type breaks: 
        :param data_series: data points in series
        :type data_series: pandas.core.series.Series
        :return: start and end interval of the most populous break
        :rtype: tuple
        """
        biggest_population = 0
        start_interval = 0
        end_interval = 0
        for i in range(len(breaks) - 1):
            interval_population = data_series[ (data_series < breaks[ i + 1 ]) & (data_series > breaks[ i ]) ].size
            if interval_population > biggest_population:
                biggest_population = interval_population
                start_interval = breaks[i]
                end_interval = breaks[i + 1]
        return start_interval, end_interval

    def _merge_breaks(self, all_breaks, breaks):
        """
        breaks are the breaks inside one of intervals in all_breaks.
        to merge all of them together we simply extent them to all_breaks and remove duplicate breaks
        (first and last elements of breaks) and finally sort them
        for example:
        all_breaks = [2, 5.5, 7, 8]
        breaks = [5.5, 6, 6.7, 7]
        5.5 and 7 are duplicates when we want the result to be: [2, 5.5, 6, 6.7, 7, 8]
        :param all_breaks: the list of given breaks that is larger than breaks
        :type all_breaks: 
        :param breaks: the list of new breaks within a break inside all_breaks
        :type breaks: 
        :return: the merged breaks
        :rtype: 
        """
        all_breaks.extend(breaks)
        all_breaks = list(set(all_breaks))  # remove duplicates (first and last element of breaks)
        all_breaks.sort()
        return all_breaks

    def segmentation(self, data_series, n_breaks, all_breaks=[], limit=1000):
        """
        the method tries to segment data_series. first it find breaks with n_breaks
        then it tries to find the most populous break and using the start_interval and end_interval.
        of the most populous break find its exact population size. then it tries to merge breaks with 
        all_breaks it has found. Then, if the most populous segment contains more than limit size it tries
        to segment it recursively.
        :param data_series: data series that need to be segmented
        :type data_series: pandas.core.series.Series
        :param n_breaks: number of breaks in each try of algorithm using jenks(not equal to all the breaks it finally find)
        :type n_breaks: int
        :param all_breaks: auxiliary list that contains all the breaks algorithm will find. set it [] always.
        :type all_breaks: 
        :param limit: least number of population in each break, if it exceeds algorithm will recur
        :type limit: int
        :return: all the breaks
        :rtype: 
        """
        breaks = jenks(data_series.tolist(), n_breaks)
        start_interval, end_interval = self._find_most_populous_break(breaks, data_series)
        most_populous_chunk_series = data_series[(data_series > start_interval) & (data_series < end_interval)]
        all_breaks = self._merge_breaks(all_breaks, breaks)
        if most_populous_chunk_series.size > limit and int(n_breaks/2)>=1:
            return self.segmentation(most_populous_chunk_series, int(n_breaks/2), all_breaks, limit)
        else:
            return all_breaks


if __name__ == "__main__":
    hierarchical_segmentation = HierarchicalSegmentation()
    breaks = hierarchical_segmentation.segment_harmonic(n_breaks=10)
