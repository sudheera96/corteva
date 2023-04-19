from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from data_model import Base,WeatherData, WeatherStats


class WeatherAnalyzer:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def calculate_statistics(self):
        session = self.Session()

        # get a list of all distinct station IDs in the database

        station_ids = session.query(WeatherData.station_id).distinct().all()

        # iterate over the station IDs and calculate summary statistics for each year
        if station_ids is not None:

            for station_id in station_ids:
                station_id = station_id[0]
                years = session.query(func.extract('year', WeatherData.date)).distinct().all()
                for year in years:
                    year = year[0]
                    # calculate the summary statistics for the given year and station ID
                    avg_max_temp = session.query(func.avg(WeatherData.max_temp)).filter(
                        WeatherData.station_id == station_id,
                        func.extract('year', WeatherData.date) == year,
                        WeatherData.max_temp != -9999
                    ).scalar()
                    avg_min_temp = session.query(func.avg(WeatherData.min_temp)).filter(
                        WeatherData.station_id == station_id,
                        func.extract('year', WeatherData.date) == year,
                        WeatherData.min_temp != -9999
                    ).scalar()
                    total_precipitation = session.query(func.sum(WeatherData.precipitation)).filter(
                        WeatherData.station_id == station_id,
                        func.extract('year', WeatherData.date) == year,
                        WeatherData.precipitation != -9999
                    ).scalar()

                    # create a new WeatherSummary object and insert it into the database
                    weather_stats = WeatherStats(
                        station_id=station_id,
                        year=year,
                        avg_max_temp=avg_max_temp,
                        avg_min_temp=avg_min_temp,
                        total_precipitation=total_precipitation)
                    # Insert the object into the database
                    session.add(weather_stats)
                    session.commit()
                    session.close()
