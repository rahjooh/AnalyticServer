import pandas as pd

class CRMMerchant(object):
    def __init__(self, crm_df, merchant_df):
        #self.crm_merchant_df = merchant_df.merge(crm_df)
        #self.crm_merchant_df = pd.concat([crm_df, merchant_df])
        self.crm_merchant_df = merchant_df.reset_index().merge(crm_df)
        self.crm_merchant_df.set_index("merchant_number")

    def create_guild_code_group(self):
        guild_code_group = self.crm_merchant_df.groupby(["senf_code"])["senf_code", "senf_name", "all_transactions", "sum_amounts", "city_name"]
        return guild_code_group

    def get_guild_all_transactions_series(self):
        guild_code_group = self.create_guild_code_group()
        guild_all_transactions_serie = guild_code_group.sum()["all_transactions"]
        return guild_all_transactions_serie

    def get_guild_all_transactions_list_series(self):
        guild_code_group = self.create_guild_code_group()
        guild_all_transactions_serie = guild_code_group.aggregate(lambda x: list(x))[ "all_transactions" ]
        return guild_all_transactions_serie

    def get_guild_all_amounts(self):
        guild_code_group = self.create_guild_code_group()
        guild_all_amounts_serie = guild_code_group.sum()["sum_amounts"]
        return guild_all_amounts_serie

    def get_guild_names(self):
        guild_name_group = self.create_guild_code_group()
        guild_names_serie = guild_name_group.first()["senf_name"]
        return guild_names_serie

    def get_all_guild_codes(self):
        guild_all_transactions = self.get_guild_all_transactions_series()
        all_guild_codes = guild_all_transactions.index.values
        return all_guild_codes

    def get_top_guilds_codes(self, n=10):
        guild_name_group = self.create_guild_code_group()
        sorted_guild_codes = guild_name_group.sum()["all_transactions"].copy()
        sorted_guild_codes.sort_values(inplace=True, ascending=False)
        top_sorted_guild_codes = sorted_guild_codes[:n].index.values
        return top_sorted_guild_codes

    def get_guild_transaction_harmonic_df(self):
        return self.crm_merchant_df[["senf_code", "all_transactions", "harmonic", "city_name"]]

    def get_guild_transaction_sum_amounts_df(self):
        return self.crm_merchant_df[["senf_code", "all_transactions", "sum_amounts", "city_name"]]