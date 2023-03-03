from kafka import KafkaConsumer
from json import loads
import boto3
import json
import os
from dotenv import load_dotenv
import tempfile
import time
import shutil

load_dotenv()

s3 = boto3.client(
    service_name='s3',
    region_name=os.environ.get('region_name'),
    aws_access_key_id=os.environ.get('access_key'),
    aws_secret_access_key=os.environ.get('secret_key')
)

# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    'pinterest',
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message.decode('utf-8')),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

def save_data_into_s3(bucket_name:str):
    """
    This portion saves the messages from the consumer to aws s3
    """
    for i, info in enumerate(data_stream_consumer): #Loops through the consumer and gets the messages
        info = info.value # Get the value of the message
        file_name = 'stored_data'
        
        with tempfile.TemporaryDirectory() as temp_dir: #Creates a temporary folder
            with open (f'{temp_dir}/{file_name}', mode = 'a+', encoding='utf-8-sig') as opened_file: #opoens a temporary folder 
                json.dump(info, opened_file, indent=4, ensure_ascii=False) #stores the data in the created file
                opened_file.write('\n')  #skips to a new line
                opened_file.flush()  #clears memory
                time.sleep(2) #sleeps for 2 seconds
                s3.upload_file(f'{temp_dir}/{file_name}',bucket_name,f'{file_name}_{i}.json') #uploads file into s3
            
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
if __name__ == "__main__":
    save_data_into_s3('pinterestmax')
            
            
            
            
            
    

