
# dags/midsize_data_transformation
from airflow.decorators import task, dag
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta 
import pandas as pd
import pyarrow.parquet as pq
import s3fs, geopy, os
from shapely.wkt import loads


dag_owner = 'grayson.stream'

geolocator = geopy.Nominatim(user_agent='myusername') #My OpenMap username

def get_location_meta(row):
    polygon = loads(row)

    # Get the center point of the polygon lat and longs
    lon, lat = polygon.centroid.x, polygon.centroid.y
    
    try:
		# return a pandas series of the location dict from the geolocator
        location = geolocator.reverse((lat, lon))
        return pd.Series({'state': location.raw['address']['state'], 'city': location.raw['address'].get('city', location.raw['address'].get('town')), 'county': location.raw['address']['county'], 'postal_code': location.raw['address']['postcode'] })
    
    except (AttributeError, KeyError, ValueError):
        print('error', lat, lon)
        return None

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

@dag(dag_id='midsize_data_transformation',
        default_args=default_args,
        description='',
        start_date=datetime(2023,1,4),
        schedule_interval='0 14 * * *',
        catchup=False,
        tags=['pyarrow', 'data_transformation', 'xcom_backend']
)

def pyarrow_pull():

	exec_date = "{{ ds_nodash }}"

	@task(queue='large-wq')
	def load_parquet_source():
		fs = s3fs.S3FileSystem(anon=True,use_ssl=True)
		
		bucket_uri =  's3://ookla-open-data/parquet/performance/type=mobile/year=2022/quarter=4'
		
		# Open the Parquet file with ookla Open Data
		pf = pq.ParquetDataset(bucket_uri, filesystem=fs)
		table = pf.read()

		return table

	@task(queue='large-wq')
	def transform_data(table):
		# convert my PyArrwo Table to a pandas Dataframe
		df = table.to_pandas()

		# filter my data down to a sample area that took more than 9 tests 
		tests_filter = df['tests'] > 9
		filtered_df = df[tests_filter]

		# look for top ten locations with the highest average download speeds.
		filtered_df = filtered_df.nlargest(10, 'avg_d_kbps')

		#apply a UDF to take the Ookla POLYGON data and convert it to a sity, state, country and zip
		filtered_df[['state', 'city', 'county', 'postal_code']] = filtered_df['tile'].apply(get_location_meta)

		# udf to convert the kbps to a human readable MB format
		filtered_df['avg_d_mbps'] = filtered_df['avg_d_kbps'].apply(lambda x: "{:.2f} MB".format(x/1024))
		filtered_df['avg_u_mbps'] = filtered_df['avg_u_kbps'].apply(lambda x: "{:.2f} MB".format(x/1024))

		# drop columns not needed for analysis
		preped_df = filtered_df.drop(columns=['quadkey', 'tile'])

		return preped_df
	
	# @task()
	# def load_preped_file(preped_df):

		# create temporary file for my summary csv file
		# with NamedTemporaryFile(mode="wb", delete=False) as temp:
		# 	# Get the temporary file's name
		# 	print(f"The temporary file's name is: {temp.name}")
		# 	preped_df.to_csv(temp.name, index=False)

		# 	# The temporary file will be deleted when the `with` block is exited
		# 	key = f"ookla_speed_test.csv"

		# #  create connection to my blob store
		# hook = WasbHook(wasb_conn_id="wasb_docker")

		# # Load the newly transformed lightweight analysis file for consumptuon for the data teams
		# hook.load_file(
        #         file_path=f"{temp.name}",
        #         container_name='preped-data',
        #         blob_name=f"{exec_date}/{key}",
        #         overwrite=True
        #     )
		# # clean up any residule os link to temp file
		# os.unlink(temp.name)

	@task()
	def load_preped_file(preped_df):
		with NamedTemporaryFile(mode="wb", delete=False) as temp:
			# Get the temporary file's name
			print(f"The temporary file's name is: {temp.name}")
			preped_df.to_csv(temp.name, index=False)

			# The temporary file will be deleted when the `with` block is exited
			key = f"ookla_speed_test.csv"
			remote_filepath = '/home/fe_shepard/summary/text.csv'
			SFTPHook_hook = SFTPHook(ssh_conn_id='sftp_docker')
			SFTPOperator_task = SFTPOperator(
				task_id='SFTPOperator_task',
				sftp_hook=SFTPHook_hook,
				local_filepath=temp.name,
				remote_filepath=remote_filepath,
				confirm=True,
				create_intermediate_dirs=False,
			).execute()
		return 

	source_data = load_parquet_source()
	preped_table = transform_data(source_data)
	load_preped_file(preped_table)

pyarrow_pull = pyarrow_pull()


