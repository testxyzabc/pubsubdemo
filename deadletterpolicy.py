from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import DeadLetterPolicy

# TODO(developer)
# project_id = "your-project-id"
# endpoint = "https://my-test-project.appspot.com/push"
# TODO(developer): This is an existing topic that the subscription
# with dead letter policy is attached to.
# topic_id = "your-topic-id"
# TODO(developer): This is an existing subscription with a dead letter policy.
# subscription_id = "your-subscription-id"
# TODO(developer): This is an existing dead letter topic that the subscription
# with dead letter policy will forward dead letter messages to.
# dead_letter_topic_id = "your-dead-letter-topic-id"
# TODO(developer): This is the maximum number of delivery attempts allowed
# for a message before it gets delivered to a dead letter topic.
# max_delivery_attempts = 5

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic_path = publisher.topic_path(project_id, topic_id)
subscription_path = subscriber.subscription_path(project_id, subscription_id)
dead_letter_topic_path = publisher.topic_path(project_id, dead_letter_topic_id)

dead_letter_policy = DeadLetterPolicy(
    dead_letter_topic=dead_letter_topic_path,
    max_delivery_attempts=max_delivery_attempts,
)

with subscriber:
    request = {
        "name": subscription_path,
        "topic": topic_path,
        "dead_letter_policy": dead_letter_policy,
    }
    subscription = subscriber.create_subscription(request)

print(f"Subscription created: {subscription.name}")
print(
    f"It will forward dead letter messages to: {subscription.dead_letter_policy.dead_letter_topic}."
)
print(
    f"After {subscription.dead_letter_policy.max_delivery_attempts} delivery attempts."
)