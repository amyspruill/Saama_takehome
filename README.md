# Saama_takehome
Takehome exercise for the Saama Data Engineer panel.

I built this Python class using the Pandas and SQLAlchemy libraries, and a MySQL target database. I chose these primarily because they're tools I'm very familiar working with, and I've had success using them to write similar code in the past.

Pandas provides excellent tools for easily and efficiently processing and manipulating data using the DataFrame class. I started with the idea of one DataFrame containing values from the current dataset, and one DataFrame containing values from the new dataset, and built out from there. Pandas includes functions for efficiently reading data from .csv files, as well as integration with SQLAlchemy (via pandas.read_sql_query() and DataFrame.to_sql()) for both reading from and inserting to SQL tables. This makes the initial data import and processing easy and efficient.

However, when comparing datasets, it's far more efficient to use a simpler data structure (such as a list) rather than comparing one DataFrame with another. Since inserts and deletions can both be identified by their primary keys appearing in one dataset but not the other, comparing lists of primary keys for each dataset was an efficient way to identify such rows. The result of comparing such lists could then be used to efficiently filter the DataFrames down to the relevant rows. Similarly, comparing lists of values (for primary keys that appeared in both current and new datasets) would identify rows that had been changed, ie, updates.

For SQL insertions, the pandas.to_sql() function works quite well, however, SQL updates are not well supported by Pandas. To process updates, I transformed the relevant dataframe into a JSON list to give the values the proper format, then used the SQLAlchemy Table object to execute update statements.

Rather than executing SQL updates, another option for changed values would have been to SQL delete the old row values and then SQL insert the new row values. However, this would have been less efficient because it involves executing two SQL commands for each changed row, rather than one. Using updates rather than delete/insert also results in cleaner code.

Any function which works with a dataset must consider the edge case that the dataset is empty. When reading data out of a .csv or database table using Pandas, this is a non-issue, as the result will simply be an empty DataFrame. The two comparison functions will be unaffected by receiving an empty DataFrame as an input. When updating or inserting to the SQL database, there is a check for an empty DataFrame, to avoid unnecessary processing if there is no data to insert or update.

To ensure my code is efficient and scalable for large datasets, I implemented chunking whenever reading data from its source or commiting inserts/updates to the database. Pandas.read_sql_query(), pandas.read_csv(), and pandas.to_sql() all include a chunksize variable for this purpose, which I set at 2000. However, when executing updates using SQLAlchemy, I needed to split the dataset into chunks myself. I did so by dividing large DataFrames into a list of smaller DataFrames containing no more than 2000 rows, and executing updates using a for loop over the list of smaller "chunk" DataFrames, so no more than 2000 database rows were updated at a time. 

I've also included four sample inputs - first_load.csv and second_load.csv mimic the example dataset in the prompt, while third_load.csv and fourth_load.csv demonstrate how the class works with larger datasets (~50000 rows).
