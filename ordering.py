from google.cloud import pubsub_v1

#publisher = pubsub_v1.PublisherClient()
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
publisher = pubsub_v1.PublisherClient(
    publisher_options=publisher_options, client_options=client_options
)
subscriber = pubsub_v1.SubscriberClient()

project_id = "gcplayproject"
topic_id = "firsttopic"
subscription_id = "ordersub"

project_path = f"projects/{project_id}"
topic_path = publisher.topic_path(project_id, topic_id)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def publishwithorder():
    for message in [
        ("message1", "key1"),
        ("message2", "key2"),
        ("message3", "key1"),
        ("message4", "key2"),
    ]:
        # Data must be a bytestring
        data = message[0].encode("utf-8")
        ordering_key = message[1]
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data=data, ordering_key=ordering_key)
        print(future.result())


def createorderedsub():
    with subscriber:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "enable_message_ordering": True,
            }
        )


if __name__=="__main__":
    publishwithorder()
    #createorderedsub()
