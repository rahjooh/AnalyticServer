from readdata.merchantdata import MerchantData
import numpy as np
import os
from visualization import plotlyvisualize

DATASET_DIR = "dataset"
PLOT_OUT_DIR = "plotsout"

merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
data_reader = MerchantData(merchant_data_path)
X = data_reader.get_clean_data()


# Data Selection
harmonic_amount = X[:, 0]
no_transaction = X[:, 1]
sum_amounts = X[:, 2]

all_features = np.vstack((harmonic_amount, no_transaction, sum_amounts))
corr_no_transactions_sum_amounts = np.corrcoef(all_features)
print(corr_no_transactions_sum_amounts)

labels = ["Harmonic Amount", "No Transaction", "Sum Amounts"]
plotlyvisualize.heamap(labels, labels, corr_no_transactions_sum_amounts.T, title="sdfs", out_path=".")