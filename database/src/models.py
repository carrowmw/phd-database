from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    JSON,
    LargeBinary,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


# base class for all models in the database - declarative_base is a factory function that constructs a base class for declarative class definitions
Base = declarative_base()


class Sensor(Base):
    __tablename__ = "sensors"

    id = Column(Integer, primary_key=True)
    sensor_name = Column(String, unique=True)
    centroid_longitude = Column(Float)
    centroid_latitude = Column(Float)
    sensor_metadata = Column(JSON)  # For storing additional sensor information


class RawData(Base):
    __tablename__ = "raw_data"

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"))
    timestamp = Column(DateTime, index=True)
    value = Column(Float)
    raw_metadata = Column(JSON)  # For storing flags, units, etc.

    sensor = relationship("Sensor", back_populates="raw_data")


class ProcessedData(Base):
    __tablename__ = "processed_data"

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"))
    timestamp = Column(DateTime, index=True)
    data = Column(LargeBinary)  # Storing pickled preprocessed DataFrame
    process_metadata = Column(JSON)  # For storing preprocessing parameters

    sensor = relationship("Sensor", back_populates="processed_data")


class EngineeredFeatures(Base):
    __tablename__ = "engineered_features"

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"))
    timestamp = Column(DateTime, index=True)
    data = Column(LargeBinary)  # Storing pickled engineered DataFrame
    engineered_metadata = Column(JSON)  # For storing feature engineering parameters

    sensor = relationship("Sensor", back_populates="engineered_features")


class ModelArtifact(Base):
    __tablename__ = "model_artifacts"

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"))
    model_type = Column(String)
    creation_date = Column(DateTime)
    model_data = Column(LargeBinary)  # Storing pickled model
    model_metadata = Column(
        JSON
    )  # For storing model parameters, performance metrics, etc.

    sensor = relationship("Sensor", back_populates="models")


Sensor.raw_data = relationship("RawData", back_populates="sensor")
Sensor.processed_data = relationship("ProcessedData", back_populates="sensor")
Sensor.engineered_features = relationship("EngineeredFeatures", back_populates="sensor")
Sensor.models = relationship("ModelArtifact", back_populates="sensor")
