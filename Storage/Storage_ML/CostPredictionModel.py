import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.tree import export_graphviz
from graphviz import Source

# File path configuration
FILE_PATH = "C:\\Users\\User\\Desktop\\large_cost_data.csv"


def load_data(file_path):
    """
    Loads the dataset from the specified file path.
    """
    return pd.read_csv(file_path)


def preprocess_data(df):
    """
    Preprocesses the dataset by scaling the feature set.
    """
    X = df[['Instances_Used', 'Storage_GB', 'Data_Transfer_GB', 'CPU_Hours', 'Memory_GB', 'Infra_Management_Cost',
            'Licensing_Cost']]
    y = df['Total_Cost']

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return train_test_split(X_scaled, y, test_size=0.3, random_state=42)


def train_model(X_train, y_train, n_estimators=100):
    """
    Trains a RandomForestRegressor model with the given training data.
    """
    model = RandomForestRegressor(n_estimators=n_estimators, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    return model


def evaluate_model(model, X_test, y_test):
    """
    Evaluates the model using Mean Squared Error and returns the prediction.
    """
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse}")
    return y_pred


def perform_grid_search(X_train, y_train):
    """
    Performs grid search to tune hyperparameters for RandomForestRegressor.
    """
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10]
    }
    grid_search = GridSearchCV(
        estimator=RandomForestRegressor(random_state=42),
        param_grid=param_grid,
        cv=5,
        n_jobs=-1,
        verbose=2
    )
    grid_search.fit(X_train, y_train)
    print(f"Best Parameters: {grid_search.best_params_}")
    return grid_search.best_estimator_


def plot_predictions(y_test, y_pred):
    """
    Plots the comparison between real values and predictions.
    """
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=y_test, y=y_pred)
    plt.plot([min(y_test), max(y_test)], [min(y_test), max(y_test)], color='red', lw=2)  # Ideal line
    plt.xlabel('Real Values')
    plt.ylabel('Predictions')
    plt.title('Comparison of Predictions vs Real Values')
    plt.show()


def export_tree(model, feature_names, max_depth=4, output_file='decision_tree'):
    """
    Exports the first decision tree from the random forest model and saves it as a PDF.
    """
    tree = model.estimators_[0]
    dot_data = export_graphviz(tree,
                               feature_names=feature_names,
                               filled=True,
                               rounded=True,
                               special_characters=True,
                               max_depth=max_depth)

    # Render the tree and export it to PDF
    graph = Source(dot_data)
    graph.render(filename=output_file, format='pdf', cleanup=True)


# Main execution
if __name__ == "__main__":
    df = load_data(FILE_PATH)
    X_train, X_test, y_train, y_test = preprocess_data(df)

    # Initial model training
    model = train_model(X_train, y_train)

    # Model evaluation
    y_pred = evaluate_model(model, X_test, y_test)

    # Hyperparameter tuning via GridSearch
    best_model = perform_grid_search(X_train, y_train)

    # Plot predictions vs actual values
    plot_predictions(y_test, y_pred)

    # Export one tree from the Random Forest
    feature_names = ['Instances_Used', 'Storage_GB', 'Data_Transfer_GB', 'CPU_Hours', 'Memory_GB',
                     'Infra_Management_Cost', 'Licensing_Cost']
    export_tree(best_model, feature_names)
