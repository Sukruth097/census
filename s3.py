import boto3
import pandas as pd
import os

client = boto3.client(
    's3',
    aws_access_key_id="",
    aws_secret_access_key="",
)

# response = client.get_object(
#     Bucket='census-project-files',
#     Key='data/2023_04_01_2023_04_19_finance_complaint.json',
# )

# s3_data =response.get('Body').read().decode()
bucket_name='census-project-files'
prefix = 'data/'

response = client.list_objects_v2(Bucket=bucket_name,Prefix=prefix)
# print(response)
if 'Contents' in response:
    objects = response['Contents']
else:
    objects = f"No objects or files found in the s3://{bucket_name}"
# print(objects)
keys = [obj['Key'] for obj in objects]
print(keys)
# key=[key.split('.')[0] for key in keys]
# print(os.path.join('client-data1', key[0].split('/')[-1]))

# download each JSON object in the list
for obj in objects:
    key = obj['Key']
    filename = os.path.join('client-data', key)  # include 'client-data' prefix
    print(filename)
    # check if the file is a JSON file
    if filename.endswith('.json'):
        # create the directory if it does not exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        # download the file
        client.download_file(bucket_name, key, filename)
        print(f'{filename} downloaded successfully')

ORIGINAL_BUCKET_NAME='census-project-files'
DESTINATION_BUCKET_NAME='archive-census'
# logger.info(f"Archiving file from data source: s3://{BUCKET_NAME}/inbox  to archive: s3://{BUCKET_NAME}/archive ")
os.system(f"aws s3 mv s3://{ORIGINAL_BUCKET_NAME}/data/ s3://{DESTINATION_BUCKET_NAME}/  --recursive --exclude '*' --include '*.json'")
# os.system(f"aws s3 sync s3://{BUCKET_NAME}/data s3://{BUCKET_NAME}/archive-census/")
# aws s3 mv s3://<source-bucket>/data/ s3://<destination-bucket>/archive-census/ --recursive --exclude "*" --include "*.json"

# print(f"File is successfully archived.")
# os.system(f"aws s3 rm s3://{BUCKET_NAME}/data/ --recursive")
# s3_data.__filename__
# df = pd.read_json(s3_data)
# # print(df.columns)
# print(df.shape)
# client_data_path = os.path.join('client_data','s3_data.json')
# data_dir=os.path.dirname(client_data_path)
# os.makedirs(data_dir,exist_ok=True)
# #Write the JSON-formatted data to the file
# with open(client_data_path, 'w') as f:
#     f.write(df.to_json())



