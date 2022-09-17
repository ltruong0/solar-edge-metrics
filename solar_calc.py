import requests
import asyncio
from datetime import datetime, timedelta
from dotenv import dotenv_values
config = dotenv_values(".env")

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
    current_timestamp = current_timestamp.replace(second=0, minute=0, microsecond=0)

    # subtract timestamp by 1 hour
    hour_ago_timestamp = current_timestamp - timedelta(hours=1)

    # get energy details
    response = await get_energy_details(siteId=siteId, apiKey=apiKey, startTime=str(hour_ago_timestamp),endTime=str(current_timestamp), timeUnit="HOUR")
    last_hour_energy = 0
    if response.status_code == 200:
        last_hour_energy = response.json()['energyDetails']['meters'][0]['values'][0].get('value')
    return last_hour_energy


async def main():
    ''''''
    apiKey = config.get('APIKEY')
    siteId = int(config.get('SITEID'))
    last_hour_energy = await get_last_hour_energy(siteId=siteId, apiKey=apiKey)


if __name__ == "__main__":
    asyncio.run(main())