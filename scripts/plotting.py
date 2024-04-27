import matplotlib.pyplot as plt
import seaborn as sns

def plot_top_values(df, column, top_n = 10):
    """
    Plot the top n values of a specified column in a DataFrame.
    
    Parameters:
    - df: DataFrame containing the data
    - column: Name of the column to plot
    - top_n: Number of top values to plot (default is 10)
    """
    # Calculate the frequency of each value in the specified column
    column_counts = df.groupBy(column).count().orderBy("count", ascending=False)
    
    # Extract the top n values
    top_values = column_counts.limit(top_n).toPandas()
    
    # Plotting the bar plot
    plt.figure(figsize=(12, 6))
    sns.barplot(x="count", y=column, data=top_values, palette='viridis')
    plt.title(f"Top {top_n} {column.capitalize()}")
    plt.xlabel('Frequency')
    plt.ylabel(column.capitalize())
    
    # Add numbers on top of each bar
    for i, value in enumerate(top_values["count"]):
        plt.text(value, i, str(value), ha='left', va='center', fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
def plot_vertical_barplot(df, column):
    """
    Plot a vertical barplot for a given column in the dataframe
    
    Parameters:
    - df: DataFrame containing the data
    - column: Name of the column to plot

    """
    # Calculate the frequency of each value in the specified column
    column_counts = df.groupBy(column).count().orderBy("count", ascending=False).toPandas()
    
    # Plotting the bar plot
    plt.figure(figsize=(12, 6))
    sns.barplot(x=column, y="count", data=column_counts, palette='viridis')
    plt.xlabel(column.capitalize())
    plt.ylabel('Frequency')
    
    # Add numbers for each bar
    for i, value in enumerate(column_counts["count"]):
        plt.text(i, value + 100, str(value), ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.show()
    
def plot_histogram(df, column):
    """
    Plot a histogram for a given column in the dataframe
    
    Parameters:
    - df: DataFrame containing the data
    - column: Name of the column to plot

    """
    
    plt.figure(figsize=(8, 6))
    sns.histplot(df.select(column).toPandas(), bins=20, kde=True)
    plt.title(f'Distribution of {column}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    plt.show()

def plot_top_selling_prices(df, col_name, top_n=10):
    """
    Plot the top selling prices for each category in the specified column.
    
    Parameters:
        df (DataFrame): The DataFrame containing the data.
        col_name (str): The name of the categorical column.
        top_n (int): The number of top categories to plot (default is 10).
    """
    # Calculate average selling price for each category
    agg_df = df.groupBy(col_name).agg({'sellingprice': 'mean'}).toPandas()
    agg_df.columns = ['category', 'mean_sellingprice']  

    # Plot top N values 
    plt.figure(figsize=(12, 6))
    sns.barplot(x='mean_sellingprice', y='category', data=agg_df.nlargest(top_n, 'mean_sellingprice'))
    plt.title(f'Top {top_n} Selling Prices by {col_name}')
    plt.xlabel('Average Selling Price')
    plt.ylabel(col_name)
    plt.show()
