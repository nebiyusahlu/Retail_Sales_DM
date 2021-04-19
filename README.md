Retail Sales Data Mart

The source of this data set was excel sheets. I used SSIS to create a simple package to extract and load the data into a staging table in SQL Server. After some cleaning and Transforming the data, I created a stored procedure that extract the data into a data warehouse which I designed and created in a star schema model and used SCD type 2 to load all the dimension tables.
