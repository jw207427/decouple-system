from google.cloud import pubsub_v1
import json
from google.auth import jwt

project_id = 'mktg-python'


service_account_info = json.load(open("mktg-python-ac5f64bb8297.json"))
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=audience
)

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)


topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_id,
    topic='test',  # Set this to something appropriate.
)
subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project_id,
    sub='test-pull',  # Set this to something appropriate.
)
# subscriber.create_subscription(
#     name=subscription_name, topic=topic_name)


def callback(message):
    x = 0
    for i in range(1):
        x+=i

    print(message.data, ' {}'.format(str(x)))
    message.ack()

# Substitute PROJECT and SUBSCRIPTION with appropriate values for your
# application.
subscription_path = subscriber.subscription_path(project_id, 'test-pull')
# sync
# response = subscriber.pull(subscription_path, max_messages=5)
#
# for msg in response.received_messages:
#     print("Received message:", msg.message.data)

# ack_ids = [msg.ack_id for msg in response.received_messages]
# subscriber.acknowledge(subscription_path, ack_ids)

# Async
future = subscriber.subscribe(subscription_path, callback)

# Open the subscription, passing the callback.
future = subscriber.subscribe(subscription_name, callback)

try:
    future.result()
except Exception as ex:
    subscriber.close()
    raise