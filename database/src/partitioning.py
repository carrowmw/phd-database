import os
import time
from tqdm import tqdm
from datetime import datetime, timedelta
import logging
from api.api_client import APIClient
from typing import Dict, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Partitioning:
    def __init__(self):
        self.client = APIClient()
        self.client.store_metadata()
        if not os.path.exists("database/logs"):
            os.makedirs("database/logs")
        self.progress_file_path = os.path.join("database/logs", "progress.txt")
        if not os.path.exists(self.progress_file_path):
            with open(self.progress_file_path, "w") as f:
                f.write("theme starttime endtime\n")
        self.themes = self.client.get_themes_df()
        self.theme_iterator = self._theme_iterator()
        self.length_time_iterator = 300
        self.starttime_iterator = self._starttime_iterator()
        self.endtime_iterator = self._endtime_iterator()
    
    def get_flattend_measurements(self, theme: Optional[str] = None, starttime: Optional[str] = None, endtime: Optional[str] = None) -> Dict:
        """Get flattened measurements"""
        try:
            self.client._update_config_explicitly(
                time_slice_params={
                    'starttime': starttime,
                    'endtime': endtime
                },
                sensor_params={
                    'theme': theme,
                }
            )
            json_data = self.client.get_raw_sensor_data()
            cleaned_json_data = [item['data'] for item in json_data.get('sensors', []) if item['data'] and item['data'] != {}]
            flattend_measurements = []
            for item in cleaned_json_data:
                for key, value in item.items():
                    for x in value:
                        flattend_measurements.append(x)

            return pd.DataFrame(flattend_measurements)
        except Exception as e:
            logger.error(f"Error getting flattened measurements: {e}")
            return None
        
    def _update_progress_file(self, theme: str, starttime: str, endtime: str, base_path: str = "database/logs"):
        progress_file = os.path.join(base_path, "progress.txt")
        with open(progress_file, "a") as f:
            f.write(f"{theme} {starttime} {endtime}\n")

    def save_partitioned_data(self, df: pd.DataFrame, theme: str, starttime: str, endtime: str, base_path: str = "./database/partitioned_data"):
        try:
            self.client._update_config_explicitly(
                time_slice_params={
                    'starttime': starttime,
                    'endtime': endtime
                },
                sensor_params={
                    'theme': theme,
                }
            )
            partition_path = f"{base_path}/{theme}/{starttime}/"
            
            os.makedirs(os.path.dirname(partition_path), exist_ok=True)

            file_name = f"measurements_{theme}_{starttime}_{endtime}.parquet"
            full_path = os.path.join(partition_path, file_name)

            if not os.path.exists(full_path):
                df.to_parquet(
                    full_path,
                    compression="snappy", # Good balance between compression and speed
                    index=False
                )
                logger.info(f"Saved {theme} data to {full_path}")
                self._update_progress_file(theme, starttime, endtime)
            else:
                logger.info(f"Data for {theme} already exists in {partition_path}")
        except Exception as e:
            logger.error(f"Error saving {theme} data: {e}")
    
    def _theme_iterator(self):
        return iter(list(self.themes["Name"]))

    def _starttime_iterator(self):
        todays_date = datetime.today()
        # The first starttime is todays date minus 7 days
        list_of_starttimes = [(todays_date - timedelta(days=x*7-7)).strftime("%Y%m%d") for x in range(self.length_time_iterator)]
        return iter(list_of_starttimes)
    
    def _endtime_iterator(self):
        todays_date = datetime.today()
        list_of_endtimes = [(todays_date - timedelta(days=x*7)).strftime("%Y%m%d") for x in range(self.length_time_iterator)]
        return iter(list_of_endtimes)
    
    def build_partitioned_data(self):
        for i in range(len(self.themes)):
            theme = next(self.theme_iterator)
            for i in range(self.length_time_iterator):
                starttime = next(self.starttime_iterator)
                endtime = next(self.endtime_iterator)
                df = self.get_flattend_measurements(theme, starttime, endtime)
                if df is not None:
                    self.save_partitioned_data(df, theme, starttime, endtime)
                    time.sleep(100)
                else:
                    logger.error(f"No data found for {theme} {starttime} {endtime}")

if __name__ == "__main__":
    partitioning = Partitioning()
    partitioning.build_partitioned_data()