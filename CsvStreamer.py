import pandas as pd
import os
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.futures import Future
import json
import sys

class CsvStreamer:
    def __init__(self, project_id, topic_id, file_name):
        self.project_id = project_id
        self.topic_id = topic_id
        self.file_path = os.path.join(os.getcwd(), file_name)
        self.batch_settings = pubsub_v1.types.BatchSettings(max_messages=100, max_bytes=1024)
        self.publisher = pubsub_v1.PublisherClient(self.batch_settings)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []
        self.i = 0
        self.l = []
        self.mapping = {
                        'FIPSST': 'State FIPS Code',
                        'STRATUM': 'Sampling Stratum',
                        'HHID' : 'Unique Household ID',
                        'FORMTYPE':'Form Type',
                        'TOTKIDS_R':'Number of Children in Household',
                        'TENURE':'The Conditions under Which Land or Buildings Are Held or Occupied',
                        'HHLANGUAGE': 'Primary Household Language',
                        'SC_AGE_YEARS': 'Age of Selected Child - In Years',
                        'SC_SEX' : 'Sex of Selected Child',
                        'K2Q35A_1_YEARS': 'Autism ASD - First Told Age in Years',
                        'BIRTH_MO' : 'Birth Month',
                        'BIRTH_YR' : 'Birth Year'
                }

    def stream_csv_data(self):
        df = pd.read_csv(self.file_path)
        for idx, row in df.iterrows():
            data = json.loads(row.to_json())
            d = {}
            for k, v in self.mapping.items():
                d[v] = data[k]

            self.publish_data_to_pubsub(d)
            
    def get_callback(self, publish_future, data):
        def callback(publish_future):
            try:
                # Wait 60 seconds for the publish call to succeed.
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")

        return callback


    def publish_data_to_pubsub(self, data):
        if self.i == 100:
            s_data = json.dumps(self.l)
            publish_future = self.publisher.publish(self.topic_path, data=s_data.encode('utf-8'))
           # print(s_data)
           # print(type(publish_future))
            publish_future.add_done_callback(self.get_callback(publish_future, data))
            self.publish_futures.append(publish_future)
            self.i = 0
            self.l = []
            return
        else:
            self.i += 1
            self.l.append(data)
            return

project_id = sys.argv[1]
topic_id = sys.argv[2]
file_id = sys.argv[3]
csvStreamer = CsvStreamer(project_id, topic_id, file_id)
csvStreamer.stream_csv_data()


            



