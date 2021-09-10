#references :
# 1. https://github.com/jahyun-dev/udacity-optimizing-public-transportation
# 2. https://github.com/peter-de-boer/optimizing-public-transportation
# 3. https://github.com/Kshankar94/Optimizing-Public-Transportation--Kafka-Streaming--Udacity-
"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


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


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("connect-postgres-stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, value_type = TransformedStation)

# TODO: Define a Faust Table
transformed_table = app.Table(
   "org.chicago.cta.stations.table.v1",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)



# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
@app.agent(topic)
async def transform_stations(station_events):
    async for station_event in station_events:
        if station_event.red == True:
            line = "red"
        elif station_event.blue == True:
            line = "blue"
        elif station_event.green == True:
            line = "green"

        
        transformed_station = TransformedStation(station_id = station_event.station_id, station_name = station_event.station_name, order = station_event.order, line = line)
        
        transformed_table[transformed_station.station_id] = transformed_station
        


if __name__ == "__main__":
    app.main()
