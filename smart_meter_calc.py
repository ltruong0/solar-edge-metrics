import csv
import pytz
import os
import glob
import logging
from datetime import datetime
from dotenv import dotenv_values
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

def setup_logger():
    logger = logging.getLogger("solar")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("debug.log")
    sh = logging.StreamHandler()
    logger.addHandler(fh)
    logger.addHandler(sh)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    return logger

def post_metrics_to_influx(token, org, bucket, influx_url, filename):

    with open(filename, newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter=',')
        data = [row for row in csv_reader]

    consumption = 0
    surplus = 0
    json_body = []

    for meter_data in data:
        start_time = meter_data.get('USAGE_START_TIME', '').strip()
        kwh = float(meter_data.get('USAGE_KWH', 0.0))
        usage_type = meter_data.get('CONSUMPTION_SURPLUSGENERATION')

        timestamp = f'{meter_data.get("USAGE_DATE")} {start_time}'
        timestamp_date_object = datetime.strptime(timestamp, '%m/%d/%Y %H:%M')

        if usage_type == 'Consumption':
            consumption += kwh
            json_body.append(
                {
                    "measurement": "energyConsumedLast15",
                    # convert to utc
                    "time": timestamp_date_object.astimezone(pytz.UTC).isoformat(),
                    "fields": {
                        "energy": kwh,
                    },
                }
            )

        elif usage_type == 'Surplus Generation':
            surplus += kwh
            json_body.append(
                {
                    "measurement": "energySurplusLast15",
                    # convert to utc
                    "time": timestamp_date_object.astimezone(pytz.UTC).isoformat(),
                    "fields": {
                        "energy": kwh,
                    },
                }
            )
        # print(json.dumps(json_body, indent=4))

    # open connection to influx
    with InfluxDBClient(url=influx_url, token=token, org=org) as client:
        # write data to influxdb
        logger.debug("Attempting to Write to InfluxDB")
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket, org, json_body)



def main():
    # influx db
    token = config.get('INFLUX_TOKEN')
    org = config.get('INFLUX_ORG')
    bucket = config.get('INFLUX_BUCKET')
    influx_url = config.get("INFLUX_URL")

    for filename in glob.glob('./smartmeter/Interval*.CSV'):
        post_metrics_to_influx(token, org, bucket, influx_url, filename)

if __name__ == '__main__':
    ''''''
    logger = setup_logger()
    config = dotenv_values(os.getenv("SLR_CONFIG", ".env"))
    main()
