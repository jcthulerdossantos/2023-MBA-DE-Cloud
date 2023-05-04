#airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator


#other packages imports
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import pymysql
import boto3


#Acessos

#RDS
host = 'xxxx'
port = 'xxxx'
user = 'xxxx'
password = 'xxxx'
db_name = 'xxxx'

#S3 | AWS Credentials
AWS_ACCESS_KEY_ID = 'xxxx'
AWS_ACCESS_KEY_SECRET = 'xxxx'
AWS_S3_BUCKET = 'xxxx'


#Default args
default_args = { 
	'owner': 'BOOKCLUB ADMIN'
	, 'depends_on_past': False
	, 'start_date': datetime(2023, 5, 3)
	, 'email_on_failure': False 
	, 'email_on_retry': False
	, 'retries': 1
	, 'retry_delay': timedelta(minutes=1)
}


#Definição da DAG
dag = DAG(
	'book_club_gold' ### nome da dag
	, description = 'DAG que extrai dados do site via web scraping, escreve em RDS e depois replica em S3 em camadas raw e public transformada' ###descrição da dag
	, default_args = default_args
	, schedule = "@daily"
)

def web_scraping_to_my_sql():
    
	root_website = "http://books.toscrape.com/"

	############ PAGINATION

	result = requests.get(root_website)
	content = result.text
	soup = BeautifulSoup(content, 'lxml')

	#Início da paginação
	pages = [root_website]
	i = 1
	page_number = [i]

	while True:
		
		try:
			
			website = pages[-1]
			result = requests.get(website)
			content = result.text
			soup = BeautifulSoup(content, 'lxml')
			
			if i == 1:
		
				pages.append(root_website + soup.find('li', class_ = 'next').find('a', href = True)['href'])
				i = i+1
				page_number.append(i)
				
			else:
				
				pages.append(f'{root_website}catalogue/' + soup.find('li', class_ = 'next').find('a', href = True)['href'])
				i = i+1
				page_number.append(i)
				
			
		except:
			
			pass
			
			break

	############ BOOKS

	books_reference_list = []

	for page in pages: 
		
		book_links = []
		
		website = page
		result = requests.get(website)
		content = result.text
		soup = BeautifulSoup(content, 'lxml')
		
		for i in range(len(soup.find_all('article', class_ = 'product_pod'))):
			
			if page_number[pages.index(page)] == 1:
		
				book_links.append(soup.find_all('article', class_ = 'product_pod')[i].find('a', href = True)['href'])
			
			else:
				
				book_links.append("catalogue/" 
								+ soup.find_all('article', class_ = 'product_pod')[i].find('a', href = True)['href'])
		
		books_reference_list.append([page, pages.index(page) + 1, book_links])
    
	books_dataframe = pd.DataFrame(books_reference_list).explode(2).reset_index(drop = True).rename(columns = {0: 'Website', 1: 'Pagination', 2: 'Book'})

	############ BOOK INFORMATION

	book_info = []

	for book in books_dataframe.Book.values:
		
		try:
			
			root_website = "http://books.toscrape.com/"    
			result = requests.get(root_website + "/" + book)
			content = result.text
			soup = BeautifulSoup(content, 'lxml')
			
			#Título
			title = soup.find('title').get_text(strip = True, separator = ' ').split(" | ")[0]
						
			#Categoria
			category = soup.find_all('a', href = True)[3].get_text()
			
			#Sales Info
			key = []
			value = []

			for index in range(len(soup.find_all('table', class_ = 'table table-striped')[0].find_all('th'))):
		
				key.append(soup.find_all('table', class_ = 'table table-striped')[0].find_all('th')[index].get_text())
				value.append(soup.find_all('table', class_ = 'table table-striped')[0].find_all('td')[index].get_text())
		
			sales_info = dict(zip(key,value))   
			
			upc = sales_info['UPC']
										
			price_wo_tax = sales_info['Price (excl. tax)']
			
			tax = sales_info['Tax']
			
			price_w_tax = sales_info['Price (incl. tax)']
			
			review = sales_info['Number of reviews']
			
			stock = sales_info['Availability']
		
			rating = soup.find('p', class_ = "star-rating").attrs["class"][1]
		
			book_dict = {'Link': book, 'Title': title, 'Category': category, 'UPC': upc, 'Price_WO_Tax': price_wo_tax, 
						'Tax': tax, 'Price_W_Tax': price_w_tax, 'Review': review, 'Stock': stock, 'Rating': rating}
			
		except:
			
			try:
			
				root_website = "http://books.toscrape.com/"    
				result = requests.get(root_website + "/" + book)
				content = result.text
				soup = BeautifulSoup(content, 'lxml')
				
				time.sleep(1)
			
				#Título
				title = soup.find('title').get_text(strip = True, separator = ' ').split(" | ")[0]
							
				#Categoria
				category = soup.find_all('a', href = True)[3].get_text()
			
				#Sales Info
				key = []
				value = []

				for index in range(len(soup.find_all('table', class_ = 'table table-striped')[0].find_all('th'))):
		
					key.append(soup.find_all('table', class_ = 'table table-striped')[0].find_all('th')[index].get_text())
					value.append(soup.find_all('table', class_ = 'table table-striped')[0].find_all('td')[index].get_text())
		
				sales_info = dict(zip(key,value))  
			
				upc = sales_info['UPC']
										
				price_wo_tax = sales_info['Price (excl. tax)']
			
				tax = sales_info['Tax']
			
				price_w_tax = sales_info['Price (incl. tax)']
			
				review = sales_info['Number of reviews']
			
				stock = sales_info['Availability']
		
				rating = soup.find('p', class_ = "star-rating").attrs["class"][1]
		
				book_dict = {'Link': book, 'Title': title, 'Category': category, 'UPC': upc, 'Price_WO_Tax': price_wo_tax, 
							'Tax': tax, 'Price_W_Tax': price_w_tax, 'Review': review, 'Stock': stock, 'Rating': rating}
			
			except:
			
				book_dict = {'Link': book, 'Title': "ERROR", 'Category': "ERROR", 'UPC': book, 'Price_WO_Tax': "ERROR", 
							'Tax': "ERROR", 'Price_W_Tax': "ERROR", 'Review': "ERROR", 'Stock': "ERROR", 'Rating': "ERROR"}
		
		book_info.append(book_dict)  

	############ FINAL DF TRANSFORMATION

	final_df = books_dataframe.merge(book_information, how = 'left', left_on = 'Book', right_on = 'Link', validate = 'm:1')
	final_df = final_df.drop(columns = ['Link']) 

	extraction_time = datetime.now(timezone.utc)

	final_df["Extracted_At"] = extraction_time

	final_df['ID'] = final_df['UPC'] + "_" + extraction_time.strftime("%Y%m%d%H%M%S")

	############ WRITE DF IN RDS DATABASE

	df_to_write = final_df.copy()

	batch_row_length = 250

	if len(final_df)%batch_row_length == 0:
		
		batches = len(df_to_write)//batch_row_length
		batch_list = []
		for i in range(batches):
			batch_list = batch_list + [i+1]*batch_row_length
			
	else:
		
		batches = len(df_to_write)//batch_row_length
		batch_list = []
		for i in range(batches):
			batch_list = batch_list + [i+1]*batch_row_length
		batches = batches + 1
		batch_list = batch_list + [batches]*(len(df_to_write)%batch_row_length)
		
	df_to_write['batch'] = batch_list
		
	for loop in range(batches):
		
		print("\nLOOP", loop + 1)
		
		rows_to_write = df_to_write[df_to_write['batch'] == loop + 1]
		
		sql_write_products = ''

		for i,row in rows_to_write.iterrows():
			
			row.Title = row.Title.replace('"', '_')
			
			sql_write_products += f'("{row[13]}","{row[0]}","{row[1]}","{row[2]}","{row[5]}","{row[3]}","{row[4]}","{row[6]}","{row[7]}","{row[8]}","{row[9]}","{row[10]}","{row[11]}","{row[12]}"),\n'

		sql_write_products = "INSERT INTO products (ID,collected_website,pagination,book_website,upc,title,category,price_without_tax,tax,price_with_tax,reviews,stock,rating,extracted_at)\nVALUES\n" + sql_write_products[:-2] + ";"

		#print(sql_write_products)
		
		connection = pymysql.connect(host = host,
									 user = user,
									 database=db_name,
									 password = password)

		mycursor = connection.cursor()
		
		mycursor.execute(sql_write_products)
		mycursor.execute('commit')
		
		print("OK for LOOP", loop + 1)


def from_rds_to_s3_raw():

	connection = pymysql.connect(host = host,
								user = user,
								database=db_name,
								password = password,
								cursorclass = pymysql.cursors.DictCursor)

	mycursor = connection.cursor()

	read_table_sql = ''' SELECT * from bookclub.products '''

	data = []

	mycursor.execute(read_table_sql)
	for row in mycursor:
		data.append(row)
		
	raw_df = pd.DataFrame(data)

	raw_df['extracted_date'] = raw_df.extracted_at.dt.date

	s3_client = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_ACCESS_KEY_SECRET)
	s3_resource = boto3.resource('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_ACCESS_KEY_SECRET)

	for distinct_date in raw_df.extracted_date.unique():
		
		temp_df = raw_df[raw_df.extracted_date == distinct_date]
		
		filename = 'raw-data/bookclub' + '_DT_INGESTION_' + distinct_date.strftime("%Y%m%d") + ".csv"
		
		temp_df.to_csv('data/' + filename, index = False)
		
		s3_client.upload_file('data/' + filename, AWS_S3_BUCKET, filename)


def from_s3_raw_trasform_data_to_s3_public():

	objects = []

	for obj in s3_resource.Bucket(AWS_S3_BUCKET).objects.all():
		
		if 'raw-data/' in obj.key and obj.key != 'raw-data/':
			objects.append(obj.key)
		else:
			pass
    
	for file in objects:
		
		s3_client.download_file(AWS_S3_BUCKET, file, 'data/' + file)
		
		raw_data = pd.read_csv('data/' + file)
		
		transformed_data = raw_data.copy()
		
		#pagination
		transformed_data["pagination"] = raw_data.pagination.astype('int32')
		
		#title
		for index in transformed_data[transformed_data.title.str.contains('_') == True].title.index:
		
			transformed_data.loc[index, 'title'] = transformed_data.loc[index, 'title'].replace("_", "'")
		
		#price without tax
		transformed_data["price_without_tax"] = transformed_data.price_without_tax.str[2:].astype('float32')
		
		#tax
		transformed_data["tax"] = transformed_data.tax.str[2:].astype('float32')
		
		#price with tax
		transformed_data["price_with_tax"] = transformed_data.price_with_tax.str[2:].astype('float32')
		
		#reviews
		transformed_data["reviews"] = transformed_data.reviews.astype('int32')
		
		#stock
		transformed_data["stock"] = transformed_data.stock.str.split(" ").str[2].str.split("(").str[1].astype("int32")
		
		#rating
		transformed_data["rating"] = transformed_data.rating.replace(
										{"One":1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}).astype("int32")
		
		transformed_data = transformed_data.drop(columns = ['ID', 'collected_website', 'book_website'])
		
		public_parquet_filename = file.replace("raw", "public").replace("csv","parquet")
		
		transformed_data.to_parquet('data/' + public_parquet_filename)
		
		s3_client.upload_file('data/' + public_parquet_filename, AWS_S3_BUCKET, public_parquet_filename)


web_scraping_to_my_sql = PythonOperator(
	task_id = "web_scraping_to_my_sql"
	, python_callable = web_scraping_to_my_sql
	, dag = dag
)

from_rds_to_s3_raw = PythonOperator(
	task_id = "from_rds_to_s3_raw"
	, python_callable = from_rds_to_s3_raw
	, dag = dag
)

from_s3_raw_trasform_data_to_s3_public = PythonOperator(
	task_id = "from_s3_raw_trasform_data_to_s3_public"
	, python_callable = from_s3_raw_trasform_data_to_s3_public
	, dag = dag
)

web_scraping_to_my_sql >> from_rds_to_s3_raw >> from_s3_raw_trasform_data_to_s3_public