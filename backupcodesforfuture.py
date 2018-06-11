import numpy as np
import pandas as pd

def reject_outliers(data):
    res = abs(data - np.mean(data, 0)) < 2 * np.std(data, 0)
    indices = np.logical_and(res[:, 0], res[:, 1])
    #indices = np.logical_and(indices, res[:,2])
    return data[indices, :]


# Data reading
alldata_df = pd.read_csv("MerchantSumAmountPerDay.txt")

merchant_number_group = alldata_df.groupby(["merchant_number"])["merchant_number", "amount", "no_transaction", "daynum"]
harmonic_df = merchant_number_group.apply(lambda x: sum(x["no_transaction"] / (91 - x["daynum"])))
#print(harmonic_dataframe.rename(columns=['merchant_number', "harmonic_value"]))
#print(harmonic_dataframe)
#harmonic_dataset = merchant_number_group.agg([("ss",lambda x: sum(x["amount"] * x["no_transaction"] / (91 - x["daynum"]))), ("sq", np.mean)])
#print(harmonic_dataset)
all_transactions_df = merchant_number_group.apply(lambda x: sum(x["no_transaction"]))
print(all_transactions_df)
frames = [harmonic_df, all_transactions_df]
all_data = pd.concat(frames, axis=1)
all_data.columns = ['harmonic', 'all_transactions']
all_data = all_data.sort_values(by=["all_transactions"])


#print(all_data.shape)
dataset = all_data[(all_data['all_transactions']<15000) ]
#print(dataset.shape)
X = dataset.as_matrix().astype(np.float)
print(X[:, 0])
X = reject_outliers(X)