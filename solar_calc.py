import requests
import asyncio
from datetime import datetime, timedelta
from dotenv import dotenv_values
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pytz
import logging


def setup_logger():
    logger = logging.getLogger("solar")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("debug.log")
    sh = logging.StreamHandler()
    logger.addHandler(fh)
    logger.addHandler(sh)
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    return logger


async def get_power_details(siteId: int, apiKey: str, startTime: str, endTime: str):
    ''''''
    url = f"https://monitoringapi.solaredge.com/site/{siteId}/powerDetails?api_key={apiKey}&startTime={startTime}&endTime={endTime}&meters=PRODUCTION"
    headers = {
        'Accept': 'application/json'
    }
    response = requests.request("GET", url, headers=headers)

    return response


async def get_energy_details(siteId: int, apiKey: str, startTime: str, endTime: str, timeUnit: str):
    ''''''
    url = f"https://monitoringapi.solaredge.com/site/{siteId}/energyDetails?api_key={apiKey}&startTime={startTime}&endTime={endTime}&meters=PRODUCTION&timeUnit={timeUnit}"

    headers = {
        'Accept': 'application/json'
    }
    response = requests.request("GET", url, headers=headers)

    return response


async def get_last_hour_energy(siteId: int, apiKey: str):
    # get energy for last hour

    # get current time stamp, rounded to hour
    current_timestamp = datetime.now()
    logger.info(f"Current time is {current_timestamp}")

    current_timestamp = current_timestamp.replace(
        second=0, minute=0, microsecond=0)

    # subtract timestamp by 1 hour
    hour_ago_timestamp = current_timestamp - timedelta(hours=1)

    logger.info(
        f"Gathering Energy Data From {hour_ago_timestamp} to {current_timestamp}")

    # get energy details
    response = await get_energy_details(siteId=siteId, apiKey=apiKey, startTime=str(hour_ago_timestamp), endTime=str(current_timestamp), timeUnit="HOUR")
    logger.debug(f"Response status code: {response.status_code}")
    logger.debug(response.json())

    last_hour_energy = 0
    if response.status_code == 200:
        last_hour_energy = response.json(
        )['energyDetails']['meters'][0]['values'][0].get('value', 0)

    # return energy info and timestamp
    return last_hour_energy, hour_ago_timestamp


async def main():
    ''''''
    # load config
    logger.info("Setting Configuration Variables")
    # solaredge
    apiKey = config.get('APIKEY')
    siteId = int(config.get('SITEID'))
    # influx db
    token = config.get('INFLUX_TOKEN')
    org = config.get('INFLUX_ORG')
    bucket = config.get('INFLUX_BUCKET')
    influx_url = config.get("INFLUX_URL")

    last_hour_energy, hour_ago_timestamp = await get_last_hour_energy(siteId=siteId, apiKey=apiKey)

    # open connection to influx
    with InfluxDBClient(url=influx_url, token=token, org=org) as client:
        json_body = [
            {
                "measurement": "energyProducedLastHour",
                # convert to utc
                "time": hour_ago_timestamp.astimezone(pytz.UTC).isoformat(),
                "fields": {
                    "energy": last_hour_energy,
                },
            }
        ]
        logger.debug(f"Metric Info {json_body}")
        # write data to influxdb
        logger.debug("Attempting to Write to InfluxDB")
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket, org, json_body)

    logger.info("Metric Written to InfluxDB")
    logger.info(json_body)

if __name__ == "__main__":
    logger = setup_logger()

    logger.info("Loading Config")
    config = dotenv_values(".env")

    logger.info("Running Main App")
    asyncio.run(main())
