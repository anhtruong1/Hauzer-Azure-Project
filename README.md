# Hauzer-Azure-Project

###

There is currently a lot of data being generated from the Hauzer machines and are stored locally on csv files. The dataset consists of 129 variables such as pressure, temperature and many more entities. The number of datapoint per machine per day runs up to 11.145.600 . All the data from the machine are being stored for an av erage of three weeks, which is then being overwritten after this three weeks time-lapse. ​

With the high demand in business intellengence and data insights, Hauzer would like  to  add value  to their clients by providing them with the ability to tract the batches’performance, make comparisons and bring about improvement to the customer’s recipes made with the various machines supplied by Hauzer.  ​

​

The end goal was to develope a structure that is useful for data analysis, which had requirements to be done by several programmatic solutions based on python programming language 3.9 by extracting the data, transforming it and loading the data Also, the developement of a storage solution for the data had to be done in the public Cloud.​

Firstly, research was carried out by the project group into the various azure data analytics services, such as Azure Data Factory and Azure Databricks. Azure (file) storage services like Azure Datalake and blobstorage . Also Azure database service like MS SQL, MYSQL and CosmoDB.  Secondly the data flow was researched and illustrated on how to get the data from the machine to Azure and finally to the database.  Finally,a highlevel design was made on the dataflow and the deliverable was Microsoft Azure Cloud platform with the temporal blob storage where the csv files are being uploaded into the azure environment. This csv files get picked up into the runtime, after which the rows are read and sent to MYSQL. This was done with an azure function. Within the database, calculations were done on the data with the KPI ‘s like temperature difference and pressure and placed in a separate table. Furthermore, a function was made on time based event every hour to caarry out the query execution. ​

​###


![2023-05-30_23h16_19](https://github.com/anhtruong1/Hauzer-Azure-Project/assets/91118397/02fc3edd-0d7a-4838-9ab7-b9a30a091842)

​
