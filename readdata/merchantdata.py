import pandas as pd
import numpy as np


class MerchantData(object):
    def __init__(self, csv_file):
        self.all_data_df = pd.read_csv(csv_file, encoding='utf-8')

    def create_merchant_number_group(self):
        merchant_number_group = self.all_data_df.groupby(["merchant_number"])["merchant_number", "amount", "no_transaction", "daynum"]
        return merchant_number_group

    def create_day_group(self):
        day_group = self.all_data_df.groupby(["daynum"])["merchant_number", "amount", "no_transaction", "daynum"]
        return day_group

    def get_transaction_per_merchant(self):
        merchant_number_group = self.create_merchant_number_group()
        transaction_per_merchant_df = merchant_number_group.apply(lambda x: sum(x["no_transaction"]))
        transaction_per_merchant = transaction_per_merchant_df.values
        return transaction_per_merchant

    def get_dataframe(self,):
        return self.all_data_df

    def get_harmonic_series(self):
        merchant_number_group = self.create_merchant_number_group()
        harmonic_series = merchant_number_group.apply(lambda x: sum(x["no_transaction"] / (91 - x["daynum"])))
        #harmonic_series = merchant_number_group.apply(lambda x: sum(1 / (91 - x["daynum"])))
        #harmonic_df = harmonic_series.reset_index()
        return harmonic_series

    def get_transaction_per_day(self):
        day_group = self.create_day_group()
        transaction_per_day_df = day_group.apply(lambda x: sum(x["no_transaction"]))
        transaction_per_day = transaction_per_day_df.values
        return transaction_per_day

    def get_sum_transactions_series(self):
        merchant_number_group = self.create_merchant_number_group()
        sum_transactions_series = merchant_number_group.apply(lambda x: sum(x["no_transaction"]))
        return sum_transactions_series

    def get_sum_amounts_series(self):
        merchant_number_group = self.create_merchant_number_group()
        sum_amounts_series = merchant_number_group.apply(lambda x: sum(x["amount"]))
        return sum_amounts_series

    def all_processed_dataframe(self):
        frames = [self.get_harmonic_series(),
                  self.get_sum_transactions_series(),
                  self.get_sum_amounts_series()]
        selected_data = pd.concat(frames, axis=1).reset_index()
        selected_data.columns = ['merchant_number','harmonic', 'all_transactions', 'sum_amounts']
        return selected_data

    def selected_dataframe(self):
        frames = [self.get_harmonic_series(), self.get_sum_transactions_series(), self.get_sum_amounts_series()]
        selected_data = pd.concat(frames, axis=1)
        selected_data.columns = ['harmonic', 'all_transactions', 'sum_amounts']
        #selected_data = selected_data.sort_values(by=["all_transactions"])
        return selected_data

    def get_range_filtered_df(self, ):
        selected_data_df = self.selected_dataframe()
        # range_filtered_df = selected_data_df[(selected_data_df['all_transactions'] < 15000) &
        #                         (selected_data_df['sum_amounts'] < 0.5e10) ]#&
        #                         #(selected_data_df['sum_amounts'] > 1e3)]
        range_filtered_df = selected_data_df
        return range_filtered_df

    def get_array(self):
        filtered_dataframe = self.get_range_filtered_df()
        X = filtered_dataframe.as_matrix().astype(np.float)
        return X

    @staticmethod
    def reject_outliers(data, features):
        data = np.array(data)
        n_data_points = data.shape[0]
        non_outlier_indices = abs(data - np.mean(data, 0)) < 2 * np.std(data, 0)
        indices = np.ones((n_data_points), dtype=bool)
        for feature in features:
            indices = np.logical_and(indices, non_outlier_indices[:, feature])
        return data[indices, :]

    def get_clean_data(self):
        X = self.get_array()
        rejected_outliers_X = self.reject_outliers(X, [0, 1, 2])
        return rejected_outliers_X
