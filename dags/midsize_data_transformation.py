from airflow.decorators import task, dag
from airflow.decorators import task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta 
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from shapely.wkt import loads
import geopy


dag_owner = 'grayson.stream'

geolocator = geopy.Nominatim(user_agent='myusername') #My OpenMap username

def get_location_meta(row):
    polygon = loads(row)
    # Get the center point of the polygon
    lon, lat = polygon.centroid.x, polygon.centroid.y
    
    try:
        location = geolocator.reverse((lat, lon))
        loc = location.raw
        return pd.Series({'state': loc['address'].get('state', 'None'), 
                          'city': loc['address'].get('city', loc['address'].get('town', 'None')), 
                          'county': loc['address'].get('county', 'None'), 
                          'postal_code': loc['address'].get('postcode', 'None') })
    
    except (AttributeError, KeyError, ValueError):
        print('error', lat, lon)
        return None, None, None, None

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
)

def pyarrow_pull():

	@task(queue='large-wq')
	def load_parquet_source():
		fs = s3fs.S3FileSystem(anon=True,use_ssl=True)
		
		bucket_uri =  's3://ookla-open-data/parquet/performance/type=mobile'
		
		# Open the Parquet file
		pf = pq.ParquetDataset(bucket_uri, filesystem=fs)
		table = pf.read(use_threads=True)

		return table

	@task(queue='large-wq')
	def transform_data(table):
		df = table.to_pandas()
		tests_filter = df['tests'] > 9

		filtered_df = df[tests_filter]
		filtered_df = filtered_df.nlargest(10, 'avg_d_kbps')

		filtered_df[['state', 'city', 'county', 'postal_code']] = filtered_df.apply(lambda x: get_location_meta(x['tile']), axis=1)

		filtered_df['avg_d_mbps'] = filtered_df['avg_d_kbps'].apply(lambda x: "{:.2f} MB".format(x/1024))
		filtered_df['avg_u_mbps'] = filtered_df['avg_u_kbps'].apply(lambda x: "{:.2f} MB".format(x/1024))
		filtered_df['avg_d_gbps'] = filtered_df['avg_d_kbps'].apply(lambda x: "{:.2f} GB".format(x/1000000))
		filtered_df['avg_u_gbps'] = filtered_df['avg_u_kbps'].apply(lambda x: "{:.2f} GB".format(x/1000000))

		preped_df = filtered_df.drop(columns=['quadkey', 'tile'])

		return preped_df
	
	@task()
	def load_preped_file(preped_df,**context):
		with NamedTemporaryFile(mode="wb", delete=True) as temp:
			# Get the temporary file's name
			print(f"The temporary file's name is: {temp.name}")
			preped_df.to_csv(temp.name, index=False)

			# The temporary file will be deleted when the `with` block is exited
			key_file = f"{context.get('ds_nodash')}_ookla_speed_test.csv"
			remote_filepath = f"/home/fe_shepard/speed_tests/{key_file}"
			SFTPHook_hook = SFTPHook(ssh_conn_id='sftp_docker')
			SFTPOperator(
				task_id='SFTPOperator_task',
				sftp_hook=SFTPHook_hook,
				local_filepath=temp.name,
				remote_filepath=remote_filepath,
				confirm=True,
				create_intermediate_dirs=False,
			).execute(context=context)
		return 

	source_data = load_parquet_source()
	preped_table = transform_data(source_data)
	load_preped_file(preped_table)

pyarrow_pull = pyarrow_pull()