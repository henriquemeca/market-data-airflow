# Market data projects orquestrated in Ariflow
This project uses Airflow and GCP services to scrap market data from websites, clean and load structured information in BigQuery

## The motivation of the project

I'm really interested in investments and in the financial market and for that reason I'm always looking for better ways to find good investment products.
This search led me to find websites I consider to have relevant data about my asset acquisition strategy. Hence this project's purpose is to load data from 2 websites (so far):
* [Investidor 10](https://investidor10.com.br/), which has relevant and exciting Fundamentals about many Brazilian assets. In this project, we focus only on the stock market
* [Zacks](https://www.zacks.com/) has a really interessting ranking system for companies, which I would like to study more deeply its correlation with acquisition timing.

### Core technologies

* **Python**: Main programming language of the project. 
* **Pyspark**: Main processing framework to cleaning data
* **Airflow**: Used for daily scheduling and task orchestration
* **Docker** and **Docker-compose**: Used to customize the Airflow base image and setup airflow with code, without using the UI
* **GCP-Cloud Storage**: Used for storing raw and cleaned data
* **GCP-Cloud Dataproc**: Used as the execution environment for pyspark scripts
* **GCP-BigQuery**: Used as a datawarehouse to store the curated data
* **Other Technologies**: Yaml, Makefile, Aiohttp, GCP-Cloud Compute, GCP-IAM, GCP-VPC Network, Pandas

## Zacks Dag
There are two dags for the Zacks project, that serve different purposes:
* zacks_rank: Runs daily and loads scraped data from the website straight to BigQuery
* zacks_rank_bach: Runs on command and was used once to load historical data to BigQuery

The daily pipeline runs async requests to get data from relevant tickers and load them to Bigquery.
The scrapper parses the HTML string for the website the get the desired values. This method has proven to be faster than other libs like beautiful soup.

## Investidor 10
>DAG for scrapping data from the website using a RAW > Cleaned > Curated architecture
![Investidor 10 DAG](/dags/investidor_10/dag-screenshot.png)

After analyzing the website were found BFFs (Backend for frontend API) that the frontend used to request JSON data from the server. This allowed for integration directly with these endpoints without the need to scrape data HTML code.

### 1º Setting up ticker keys
For using all the desired BFFs it's needed:
* A list of tickers. That can be downloaded from [Dados Merdado](https://www.dadosdemercado.com.br/bolsa/acoes).
* The system ticker_id, a number used by the website to identify the tickers. It can be scraped from the HTML code.
* A company_id that is also related to the ticker. It also can be scraped from the HTML code.

### 2º Requesting data from BFFs
Now, with the needed ticket keys on a file in Cloud Storage, we can structure a request for all the BFFs and load the returned JSON data to a RAW layer in Cloud Storage

It's important to note that the requests are configured by a yaml file and are made asynchronously using the Aiohttp lib with retries to deal with server errors. A field identifying the ticket is added to the JSON.

### 3º Cleaning the data
First, a task creates a Dataproc cluster that will be used to run the cleaning pyspark scripts.
With the cluster up and the BFF loading process finished the cleaning step can happen

Out of 7 data schemas, 5 of them can be cleaned with the same default scripts that use a flatten function that handles Struct and Array types in the schema.
The other two cases have different cleaning scripts because the historic_kpis data has non-allowed characters in the column names that need to be cleaned. And the prices_profit data requires an unpivoting process 

The cleaned data is saved in parquet in the Cleaned Layer

### 4º Uploading Data to BigQuey
In this last step the hard work is already done and the data in the cleaned layer is read and loaded to BigQuery where it can the explored for insights and analysis like stock screening.

## Getting Started

### Prerequisites
Will need:
* Install Docker Desktop
* Install Docker-compose
* Load a `gcp_account.json` on the root of your repository as a local key file to access GCP. You can generate this key file on [generate a service account keys](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

### Executing the project
After completing the prerequisites, we are ready to execute the project with a simple command inside the project directory:
`docker-compose up --build -d`

Now access on your browser:
`localhost:8080`

### Makefile
To load your scripts to Cloud Storage so they can be accessed in Dataproc jobs, run on the terminal:
`make sync-scripts`

### Caveats
You may need to set up a network configuration on the VPC network to allow "Google Private Acess" in the same Region used in this project.