# import json

# read from JSON
# with open("C:/Users/shana/Desktop/metadata.json", 'r') as file:
#     data = json.load(file)

# Extract the information about the users
# users = data['server']['users']

# Iterate over each user and calculate the number of buckets they have
# bucket_counts =[]
# for user, user_info in users.items():
#     bucket_count = len(user_info.get('buckets', {}))
#     bucket_counts.append(bucket_count)
# print(len(bucket_counts))

import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

data = { 
    'Bucket_Count': [15, 100, 45, 10, 25, 600, 35, 60, 100, 20],
    'Session_Time': [5, 600, 900, 40, 500, 800, 700, 10, 100, 400],
    'role': ['admin', 'admin', 'user', 'admin', 'user', 'user', 'admin', 'admin', 'user', 'admin']
}

df = pd.DataFrame(data)

df_encoded = pd.get_dummies(df, columns=['role'])

# Normalize the data
scaler = StandardScaler()
scaled_data = scaler.fit_transform(df_encoded)

print(scaled_data)

import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.datasets import make_blobs

# Create sample data for demonstration purposes
X, _ = make_blobs(n_samples=300, centers=4, cluster_std=0.6, random_state=0)

# List to store the within-cluster sum of squares (WCSS)
wcss = []

# Testing different values for K (e.g., from 1 to 10)
for i in range(1, 10):
    kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0)
    kmeans.fit(X)
    wcss.append(kmeans.inertia_)


plt.plot(range(1, 10), wcss, marker='o')
plt.title('Elbow Method')
plt.xlabel('Number of clusters (K)')
plt.ylabel('WCSS')
plt.show()


from sklearn.cluster import KMeans

# The number of clusters we want to find
n_clusters = 4

# Create a K-Means model
kmeans = KMeans(n_clusters=n_clusters, random_state=0)

# Fit the model to the data
kmeans.fit(scaled_data)

# Get the labels for each data point
labels = kmeans.labels_

# Add the labels to the DataFrame
df['Cluster'] = labels

print(df)

import matplotlib.pyplot as plt

# Plot the results
plt.scatter(df['Bucket_Count'], df['Session_Time'], c=df['Cluster'], cmap='viridis')
plt.xlabel('Feature 1')
plt.ylabel('Feature 2')
plt.title('K-Means Clustering')
plt.colorbar(label='Cluster')
plt.show()
