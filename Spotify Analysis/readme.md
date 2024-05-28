To do: 

    - Add CI/CD so ETL files are automatically uploaded to corresponding lambda (right now, they are copied manually)
    - Add logging

ETL files extract data using spotify API, process it and loads to DynamoDB.

Analysis contains a notebook that extract DynamoDB data, performs analysis, and saves HTML plots to show in web and CSV files to load in Tableau.