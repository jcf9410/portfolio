ETL files extract data using spotify API, process it and loads to DynamoDB.

"TrackData" ETL is executed locally, as the process can take up to 30 min depending on the length of the user library, and if there is already uploaded data. For this process to be executed in the cloud, a simple Lambda service is not enough, and a more sophisticaded solution should be studied. The other ETLs can be executed directly in an AWS Lambda.

Analysis contains notebooks that extract DynamoDB data, performs analysis, and saves HTML plots and CSVs to show in web and CSV files to load in Tableau.