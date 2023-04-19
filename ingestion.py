"""
ingestion.py for ingesting weather data in to data model

Revision History

   1, Sri Sudheera Chitipolu, 2023-04-15: First import.

"""
# Import required modules

import os
import datetime
from sqlalchemy import create_engine,func
from sqlalchemy.orm import sessionmaker
from data_model import Base, WeatherData


class WeatherDataIngestor:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def ingest(self, data_dir):
        session = self.Session()
        last_ingestion_time = session.query(func.max(WeatherData.ingestion_time)).scalar()
        last_modified_time = datetime.datetime.fromtimestamp(os.path.getmtime('wx_data'))
        if last_ingestion_time and last_modified_time and last_modified_time <= last_ingestion_time:
            print("Data directory has not been modified since last ingestion. Skipping ingestion.")
            return
        # list all files in the data directory
        files = os.listdir(data_dir)
        # create a list to hold the weather data records
        records = []
        # iterate over the files and extract the records
        for file in files:
            with open(os.path.join(data_dir, file), 'r') as f:
                station_id = os.path.splitext(file)[0]

                for line in f:
                    # parse the line and create a new WeatherData object
                    parts = line.strip().split('\t')
                    date = datetime.datetime.strptime(parts[0], '%Y%m%d').date()
                    max_temp = float(parts[1]) / 10.0 if parts[1] != '-9999' else parts[1]
                    min_temp = float(parts[2]) / 10.0 if parts[2] != '-9999' else parts[2]
                    precipitation = float(parts[3]) / 10.0 if parts[3] != '-9999' else parts[3]
                    record = WeatherData(station_id=station_id, date=date, max_temp=max_temp, min_temp=min_temp,
                                         precipitation=precipitation)
                    # add the record to the list if it doesn't already exist in the database
                    if not self.check_duplicate(record):
                        records.append(record)

        # write the records to the database
        start_time = datetime.datetime.now()
        session.add_all(records)
        # session.bulk_save_objects(record)
        session.commit()
        session.close()

        # log the number of records ingested and the start/end time
        num_records = len(records)
        end_time = datetime.datetime.now()
        self.update_last_ingestion_time(end_time)

        print(
            f'Start time of Ingestion {start_time}. The number of records Ingested {num_records} into the database at '
            f'End time {end_time}')

    def check_duplicate(self, record):
        # check if a record already exists in the database
        session = self.Session()
        exists = session.query(WeatherData).filter(
            WeatherData.date == record.date,
            WeatherData.max_temp == record.max_temp,
            WeatherData.min_temp == record.min_temp,
            WeatherData.precipitation == record.precipitation
        ).first() is not None
        session.close()
        return exists


    def update_last_ingestion_time(self, timestamp):
        # update the last_ingestion field in the IngestionMetadata table of the database
        session = self.Session()
        ingestion_metadata = session.query(WeatherData).first()
        if ingestion_metadata:
            ingestion_metadata.last_ingestion = timestamp
        else:
            ingestion_metadata = WeatherData(ingestion_time=timestamp)
            session.add(ingestion_metadata)
        session.commit()
        session.close()
