import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
from sklearn.neighbors import KernelDensity
import os
from readdata.merchantdata import MerchantData

DATASET_DIR = "dataset"
PLOT_OUT_DIR = "plotsout"

merchant_data_path = os.path.join(DATASET_DIR, "MerchantSumAmountPerDay.txt")
data_reader = MerchantData(merchant_data_path)
X = data_reader.get_clean_data()

# Data Selection
no_transaction = X[:, 1]  # Frequency
sum_amounts = X[:, 2]  # Money



# Plot a 1D density example
N = 100
np.random.seed(1)
N = no_transaction.shape[0]
X = no_transaction[:,np.newaxis]#np.random.normal(0, 1, 0.3 * N)[:, np.newaxis]
X_plot = np.linspace(np.min(X), np.max(X), 1000)[:, np.newaxis]


fig, ax = plt.subplots()

for kernel in ['gaussian', 'tophat', 'epanechnikov']:
    kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
    log_dens = kde.score_samples(X_plot)
    ax.plot(X_plot[:, 0], np.exp(log_dens), '-',
            label="kernel = '{0}'".format(kernel))

ax.text(6, 0.38, "N={0} points".format(N))

ax.legend(loc='upper left')
#ax.plot(X[:, 0], -0.005 - 0.01 * np.random.random(X.shape[0]), '+k')

#ax.set_xlim(-4, 9)
#ax.set_ylim(-0.02, 0.4)
plt.show()