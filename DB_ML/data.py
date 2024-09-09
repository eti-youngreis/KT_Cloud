import numpy as np
import pandas as pd

# קונפיגורציה
num_queries = 100  # מספר השאילתות הסינתטיות

# יצירת נתונים סינתטיים
query_ids = np.arange(1, num_queries + 1)
execution_times = np.random.uniform(10, 1000, num_queries)  # זמן ביצוע בין 10 ל-1000 מילישניות
result_sizes = np.random.uniform(1000, 50000, num_queries)  # גודל תוצאה בין 1000 ל-50000 בתים
usage_frequencies = np.random.poisson(10, num_queries)  # תדירות שימוש לפי התפלגות פואסון

# יצירת DataFrame
data = {
    'query_id': query_ids,
    'execution_time': execution_times,
    'result_size': result_sizes,
    'usage_frequency': usage_frequencies
}

df = pd.DataFrame(data)

print(df.head())
