import pandas as pd
import numpy as np


class CRMData(object):
    def __init__(self, csv_file):
        self.all_data_df = pd.read_excel(csv_file, encoding='utf-8')

    def create_merchant_number_group(self):
        merchant_number_group = self.all_data_df.groupby(["merchant_number"])["merchant_number", "senf_code", "city_name", "senf_name"]
        return merchant_number_group

    def create_guild_name_group(self):
        guild_name_group = self.all_data_df.groupby(["senf_name"])["senf_name", "merchant_number", "city_name"]
        return guild_name_group

    def get_guild_series(self):
        merchant_number_group = self.create_merchant_number_group()
        guild_series = merchant_number_group.first()["senf_name"]
        return guild_series

    def get_guild_code_series(self):
        merchant_number_group = self.create_merchant_number_group()
        guild_code_series = merchant_number_group.first()["senf_code"]
        return guild_code_series

    def get_all_data(self):
        return self.all_data_df

    def apply_range_filter(self, ):
        selected_data = self.selected_dataframe()
        dataset = selected_data[(selected_data['all_transactions'] < 15000) &
                                (selected_data['sum_amounts'] < 0.5e10) ]
        return dataset

    def selected_dataframe(self):
        frames = [self.get_harmonic_df(), self.get_sum_transactions(), self.get_sum_amounts()]
        selected_data = pd.concat(frames, axis=1)
        selected_data.columns = ['harmonic', 'all_transactions', 'sum_amounts']
        selected_data = selected_data.sort_values(by=["all_transactions"])
        return selected_data

    def get_array(self):
        filtered_dataframe = self.apply_range_filter()
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
