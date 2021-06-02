import pandas as pd
from apiclient.discovery import build
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import socket
from google.cloud import bigquery
import pandas_gbq

# Authentication for Google Analytics & BigQuery API calls

scope = 'https://www.googleapis.com/auth/analytics.readonly'
credentials = service_account.Credentials.from_service_account_file('credentials.json') #Must put your credentials file at this file folder with the name credentials.json
service = build('analytics', 'v3', credentials=credentials)

# Date variables -- Used for specifying date range of data pull
today = datetime.now().date()
yesterday = today - timedelta(days=1)

start_date = '2020-06-01'
start_date = datetime.strptime(start_date , '%Y-%m-%d').date()
end_date = '2020-09-01'

def analytics_api_query(client_id, index, start_date, end_date, dimensions, metrics): #builds query pull
	
	# 7 dimensions, 10 metrics limit
	data = service.data().ga().get(
		ids='ga:' + client_id,
		start_date=start_date,
		end_date=end_date,
		metrics=metrics,
		dimensions=dimensions,
		start_index=index,
		max_results=10000).execute()

	return data


def analytics_get_report(client_id, start_date, end_date, dimensions, metrics): #grabs data
	
	index = 1
	totalResults = 2
	output = []

	while totalResults > index:
		a = analytics_api_query(client_id, index, start_date, end_date, dimensions, metrics)
		totalResults = a.get('totalResults')
		index = index + 10000

		try:
			for row in a.get('rows'):
				output.append(row)
		except:
			pass

	return output


def main(client_id, traffic_columns, DIMENSIONS):

	base_df = pd.DataFrame()
	columns = traffic_columns
	#start_date = '2019-01-01'
	#start_date = datetime.strptime(start_date , '%Y-%m-%d').date()

	print('Running Google Analytics Report.')
	
	
	#change based the type and number of dimensions pulled. These are set 
	dimensions = 'ga:'+(',ga:'.join(columns[:DIMENSIONS]))
	metrics = 'ga:'+(',ga:'.join(columns[DIMENSIONS:]))
    
    #Here you can change the query data like start_date and end_date 
	results = analytics_get_report(client_id, str(yesterday), str(yesterday), dimensions, metrics)
	df = pd.DataFrame(results, columns=columns)

	df['view_id'] = client_id
	df['view_name'] = 'John Foos ECommerce: John Foos E-Commerce'
	base_df = base_df.append(df)

	base_df = base_df.applymap(str)
	base_df = base_df[base_df.date != '(other)'] #remove if date column has '(other)' as a value
	base_df['date'] = pd.to_datetime(base_df['date'], format='%Y%m%d') #format date
	
	# Powershell output
	#print(base_df.head())
	#print('base_df:', base_df)

	return base_df

def uploadPandasToBigQuery(data, tableId):
    #bigquery output
    data.to_gbq(
    'bq_dataset_name.{}'.format(tableId), #dataset name + table name
    'bq_proyect', #project name
    chunksize=10000,
    reauth=False,
    if_exists='append',
    credentials=credentials
    )
    print('Uploaded')


#Report Example (max: 7 dimension - 10 metrics)
USERS = [
	'date', #dimension
	'city', #dimension
	'country', #dimension
	'dataSource', #dimension
	'continent', #dimension
	'language', #dimension
	'region', #dimension
	'bounces', #metric
	'newUsers', #metric
	'pageviews', #metric
	'sessionDuration', #metric
	'sessions', #metric
	'timeOnPage', #metric
	] 



data = main('195453613', USERS, 7) #GA View Id, columns of the report, amount of dimensions in report
#Setting metrics data types because this api returns metrics as strings
data[["bounces", "newUsers",'pageviews', 'sessionDuration', 'sessions', 'sessions', 'timeOnPage']] = data[["bounces", "newUsers",'pageviews', 'sessionDuration', 'sessions', 'sessions', 'timeOnPage']].apply(pd.to_numeric)
uploadPandasToBigQuery(data,'BigQueryTableId')