from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

project_id = "gcplayproject"
topic_id = "firsttopic"
project_path = f"projects/{project_id}"
topic_path = publisher.topic_path(project_id, topic_id)

def createtopic():
    publisher.create_topic(request={"name": topic_path})

def deletetopic():
   # publisher.delete_topic(request={"topic": topic_path})
    publisher.delete_topic(topic=topic_path)

def listtopics():
    for topic in publisher.list_topics(request={"project": project_path}):
        print(topic)

def sendmessage():
    data = "i am message"
  #  data = b"i am message"
  #  print(data)
    data = data.encode("utf-8")
    print(data)
    publisher.publish(topic_path, data)

def sendmessagewithattribute():
    data = "first message"
    data = data.encode("utf-8")
    publisher.publish(topic_path, data, name='himanshu')

#deb batchsettings():
  #  from google.cloud import pubsub
  #  from google.cloud.pubsub import types

   # client = pubsub.PublisherClient(
  #      batch_settings=types.BatchSettings(max_messages=500),
   # )


if __name__=="__main__":
 #   deletetopic()
    #createtopic()
    #listtopics()
    #sendmessage()
    sendmessagewithattribute()