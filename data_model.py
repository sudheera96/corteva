"""
data_model.py for weather data and weather stats

Revision History

   1, Sri Sudheera Chitipolu, 2023-04-15: First import.

"""

# Import required modules
# This code defines two SQLAlchemy models for storing weather data and statistics
# in a relational database. The WeatherData model represents weather records, while
# the WeatherStats model represents weather statistics calculated for each station
# and year.
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Date, DateTime

Base = declarative_base()


class WeatherData(Base):
    # Table for storing weather records

    __tablename__ = 'weather_records'

    id = Column(Integer, primary_key=True)
    ingestion_time = Column(DateTime, default=datetime.utcnow)
    station_id = Column(Integer)
    date = Column(Date)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)


# Assumes that there is one weather record per day per station
# Assumes that the same station ID is used consistently across records
# Assumes that the year is stored as an integer in the WeatherStats table


class WeatherStats(Base):
    __tablename__ = 'weather_stats'
    id = Column(Integer, primary_key=True)
    analysis_ingestion_time = Column(DateTime, default=datetime.utcnow)
    station_id = Column(Integer)
    year = Column(Integer)
    avg_max_temp = Column(Float, nullable=True)
    avg_min_temp = Column(Float, nullable=True)
    total_precipitation = Column(Float, nullable=True)
