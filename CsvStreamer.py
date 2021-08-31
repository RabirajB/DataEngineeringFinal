import pandas as pd
import os
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.futures import Future
import json
import sys

class CsvStreamer:
    def __init__(self, project_id, topic_id, path):
        self.project_id = project_id
        self.topic_id = topic_id
        self.path = path
        self.batch_settings = pubsub_v1.types.BatchSettings(max_messages=100, max_bytes=1024)
        self.publisher = pubsub_v1.PublisherClient(self.batch_settings)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []
        self.i = 0
        self.l = []

    def stream_csv_data(self):
        df = pd.read_csv(self.path)
        for idx, row in df.iterrows():
            self.pubish_data_to_pubsub(row)

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
            print(type(publish_future))
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
file_name = sys.argv[3]
csvStreamer = CsvStreamer(project_id, topic_id)
csvStreamer.stream_csv_data()


            



