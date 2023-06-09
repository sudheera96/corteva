# corteva

## Project Architecture
The data present in wx_data of all .txt files are stored in to weather Records table of Weather Database. On top of that table, data analysis are performed and results are stored to Weather Stats table. The information of those two tables are seen via Flask API, api/weather and api/weather/stats respectively. 

![](https://github.com/sudheera96/corteva/blob/main/architecture.PNG?raw=true)

```
wx_data folder - contains txt files
```

## Data Model
This code defines two SQLAlchemy models for storing weather data and statistics in a relational database:

### WeatherData: 
This model represents weather records and has the following columns:
```
id: Integer primary key for the record

ingestion_time: DateTime column that stores the time the record was added to the database

station_id: Integer column that stores the ID of the weather station

date: Date column that stores the date of the weather record

max_temp: Float column that stores the maximum temperature for the day

min_temp: Float column that stores the minimum temperature for the day

precipitation: Float column that stores the amount of precipitation for the day
```

### WeatherStats: 
This model represents weather statistics calculated for each station and year, and has the following columns:
```
id: Integer primary key for the record

analysis_ingestion_time: DateTime column that stores the time the statistics were added to the database

station_id: Integer column that stores the ID of the weather station

year: Integer column that stores the year for which the statistics were calculated

avg_max_temp: Float column that stores the average maximum temperature for the year

avg_min_temp: Float column that stores the average minimum temperature for the year

total_precipitation: Float column that stores the total precipitation for the year
```
The code assumes that there is one weather record per day per station, the same station ID is used consistently across records, and the year is stored as an integer in the WeatherStats table.


## Data Ingestion
In weather_records wx_data is ingested. 
- Station_id are extracted from files
- The maximum temperature for that day (in tenths of a degree Celsius) /10
- The minimum temperature for that day (in tenths of a degree Celsius) /10
- The amount of precipitation for that day (in tenths of a millimeter) /10
- Missing values are indicated by the value -9999.
- Add the record to the list if it doesn't already exist in the database
- The script will read each file in the wx_data directory and extract the station name and state from the file name. It will then check if the station already exists in the stations table. If not, it will insert a new station record.

For each line in the file, the script will parse the observation data and insert a new observation record. It will use the station id obtained from the stations table to associate the observation with the correct station.

To prevent duplicates, the script will use a unique constraint on the (station_id, date) pair in the observations table.
- It's important to note that if you are ingesting data from the same data directory and the data files have not changed, then running the ingestion process multiple times will result in the same set of records being added to the database each time. In this case, you may want to modify the code to check if the data files have been updated before running the ingestion process again

## Data Analysis

Iterate over the station IDs and calculate summary statistics for each year on top of data ingestion table i.e weather records table
and store them weather_stats table

## API Design
In this example, we're using the flask_swagger_ui package to create the Swagger UI blueprint 
We then define two routes, one for getting the weather data and one for getting the weather stats. The data is queried from the database and formatted as JSON. Finally, we return the JSON response using Flask's jsonify function.
```
api/weather
```
provide Page Number, Number of items per Page, Station Id, Year 
![image](https://user-images.githubusercontent.com/22390581/232992055-8921a8d9-6e1c-4718-8c42-9be36dd0f2fd.png)


```
api/weather/stats
```
provide station id, year 

![image](https://user-images.githubusercontent.com/22390581/232990714-7d598954-6dcb-4b7f-847b-ce8218ae049d.png)


To see the Swagger UI documentation, you can navigate to http://localhost:5000/

## Run Project

```
Add wx_data folder to this project 
Run Data Ingestion, Data Analyser then Run app
```

## AWS Architecture

API: To deploy an API on AWS, I would use Amazon API Gateway. API Gateway makes it easy to create, deploy, and manage APIs at any scale. 

Database: For the database, I would use Amazon Relational Database Service (RDS). RDS is a fully managed database service that makes it easy to set up, operate, and scale a relational database in the cloud. I would choose the appropriate database engine for my use case, such as MySQL, PostgreSQL, or Aurora, and then set up the database instance on RDS.

Data ingestion: For the scheduled version of data ingestion code, I would use AWS Lambda. Lambda is a serverless computing service that lets you run code without provisioning or managing servers. I would create a Lambda function that retrieves data from the source and inserts it into the RDS database.

Scheduled execution: To schedule the Lambda function to run on a regular basis, I would use AWS CloudWatch Events. CloudWatch Events enables me to create rules that match incoming events and route them to one or more target functions, such as the Lambda function that performs data ingestion. I can set up the rule to run at a specific time, or on a regular schedule, such as every hour or every day.

Data Collection: Data is collected from the RDS database using AWS Glue, which can extract data from relational databases like MySQL, PostgreSQL, or Aurora and transform it into a format suitable for analysis.

Data Preparation: The collected data is prepared for analysis using Pandas or Apache Spark to perform tasks like cleaning, filtering, aggregating, or joining data from multiple sources.

Data Storage: The prepared data is stored in Amazon S3, a highly scalable and durable object storage service.

Athena Integration: Athena is used to query and analyze data stored in Amazon S3, using standard SQL queries.

Results Storage: The results of the data analysis are stored in a storage option like Amazon S3 or Amazon Redshift, depending on the size and complexity of the data.

Pipeline Monitoring: The performance of the data analysis pipeline is monitored using AWS CloudWatch to monitor metrics like CPU usage, memory utilization, and data transfer rates. Alerts can also be set up to notify in case of issues or anomalies in the data.

API Integration:The results of the data analysis can be integrated into the API Gateway, allowing users to query and access the data through API requests.

Visualization: Data visualization can be done using tools like Tableau or QuickSight, allowing users to create interactive visualizations and dashboards to explore the data.

Infrastructure as code: To manage the AWS resources that make up the API, database, and data ingestion code, I would use AWS CloudFormation. CloudFormation allows me to define my infrastructure as code, which means that I can create, update, and delete AWS resources in a predictable and repeatable way. I can define the API Gateway, RDS database, Lambda function, and CloudWatch Events rule in a CloudFormation template, which can be versioned, tested, and deployed just like any other code.
