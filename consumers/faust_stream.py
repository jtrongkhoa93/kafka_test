"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)

INPUT_TOPIC_NAME = 'org.chicago.cta.station.stations'
OUTPUT_TOPIC_NAME = 'org.chicago.cta.station.transformed-stations.v1'

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

    def __init__(self,station_id,station_name,order):      
        self.station_id=station_id
        self.station_name=station_name
        self.order=order


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
station_topic = app.topic(INPUT_TOPIC_NAME, value_type=Station)
transformed_station_topic = app.topic(OUTPUT_TOPIC_NAME, partitions=1, value_type=TransformedStation)

transformed_station_table = app.Table(
   "stations",
   default=Station,
   partitions=1,
   changelog_topic=transformed_station_topic,
)

# Define a stream processing agent
@app.agent(station_topic)
async def process(stations):
    async for station in stations:
        transformed_station = Station(station.station_id, station.station_name, station.order)
        if station.red:
            transformed_station.line="red"
        elif station.blue:
            transformed_station.line="blue"
        else:
            transformed_station.line="green"

        transformed_station_table[station.station_id] = transformed_station

        # Publish the user information to the output topic
        await transformed_station_topic.send(key=int(round(time.time() * 1000)), value=transformed_station)

if __name__ == "__main__":
    app.main()
