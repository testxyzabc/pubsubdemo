from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# TODO(developer)
project_id = "gcplayproject"
topic_id = "firsttopic"
subscription_id = "pullsub"

topic_path = publisher.topic_path(project_id, topic_id)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def createsub():
    with subscriber:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )


def getmessage():
        response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 1,
        }
        )
        #print(response)
        ack_ids = []
        for msg in response.received_messages:
            #b.decode('UTF-8')
            print("Received message:", msg.message.data)
        # print("Received message:", (msg.message.data).decode('UTF-8'))
            print("msgids:", msg.message.message_id)
            print("attributes:", msg.message.attributes)
            print("publishtime:", msg.message.publish_time)
            # first way to acknowlege message 
           # msg.message.ack()
            ack_id = msg.ack_id
            print(ack_id)
         #   ack_ids.append(msg.ack_id)
        #subscriber.acknowledge(
         #   request={"subscription": subscription_path, "ack_ids": ack_ids}
       # )
        
def asyncgetmessage():
    def callback(message):
        print(f"Received {message}.")
        message.ack()  
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    # Wrap subscriber in a 'with' block to automatically call close() when done.
   # print(streaming_pull_future)
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=10)
            #redice the timeout to 1 and show that it cancels it
        except TimeoutError:
            streaming_pull_future.cancel()          


if __name__=="__main__":
   # createsub():
    #getmessage()
    asyncgetmessage()

    