class ClusterNumbering(object):
    def __init__(self):
        pass

    def renumber(self, all_merchant_labels_df, func= lambda x: sum(x)):
        num_clusters = len(all_merchant_labels_df["labels"].unique())
        cluster_scores = []
        for cluster_num in range(num_clusters):
            merchant_per_cluster = all_merchant_labels_df[all_merchant_labels_df["labels"] == cluster_num]
            #cluster_scores.append(merchant_per_cluster.sum().sum())
            cluster_scores.append(merchant_per_cluster.apply(func).sum())

        indices = sorted(range(len(cluster_scores)), key=lambda k: cluster_scores[k])
        all_merchant_labels_df["labels"] = all_merchant_labels_df["labels"].apply(lambda x: indices[x])
        return all_merchant_labels_df
