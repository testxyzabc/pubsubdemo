from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub import SchemaServiceClient
from google.pubsub_v1.types import Schema
from google.cloud import pubsub_v1
from google.cloud.pubsub import PublisherClient
import json
from google.pubsub_v1.types import Encoding

publisher = pubsub_v1.PublisherClient()
publisher_client = PublisherClient()

project_id = "gcplayproject"
schema_id = "userdetails"
avsc_file = "/home/googlecloud/mynewdata/avro.avsc"
topic_id = "schematopic"
topic_path = publisher.topic_path(project_id, topic_id)

project_path = f"projects/{project_id}"

# Read a JSON-formatted Avro schema file as a string.
with open(avsc_file, "rb") as f:
    avsc_source = f.read().decode("utf-8")

schema_client = SchemaServiceClient()
schema_path = schema_client.schema_path(project_id, schema_id)
schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=avsc_source)

def createschema():
    try:
        result = schema_client.create_schema(
            request={"parent": project_path, "schema": schema, "schema_id": schema_id}
        )
        print(f"Created a schema using an Avro schema file:\n{result}")
    except AlreadyExists:
        print(f"{schema_id} already exists.")

def getschema():
    result = schema_client.get_schema(request={"name": schema_path})
    print(f"Got a schema:\n{result}")


def listschema():
    for schema in schema_client.list_schemas(request={"parent": project_path}):
        print(schema)

def deleteschema():
    schema_client.delete_schema(request={"name": schema_path})
    print(f"Deleted a schema:\n{schema_path}")

def createtopicwithschema():
    encoding = Encoding.ENCODING_UNSPECIFIED
    response = publisher.create_topic(
        request={
            "name": topic_path,
            "schema_settings": {"schema": schema_path, "encoding": "BINARY"}
        }
    )
    print(f"Created a topic:\n{response}")

def sendmessage():
    topic = publisher_client.get_topic(request={"topic": topic_path})
    encoding = topic.schema_settings.encoding
    print(encoding)

    data = {"Username": "Alaska", "ID": 123456, "City": "Bangalore"}
    data = json.dumps(data).encode("utf-8")
    print(data)
    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")





if __name__=="__main__":
    #createschema()
    #listschema()
    #deleteschema()
    #getschema()
    #createtopicwithschema()
    sendmessage()
