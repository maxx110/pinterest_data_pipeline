from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()


# Configure our producer which will send data to  the MLdata topic
kafka_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: dumps(x).encode("utf-8")
) 




class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    # Send our ml data to the MLData topic
    kafka_producer.send(topic="pinterest", value=data)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
