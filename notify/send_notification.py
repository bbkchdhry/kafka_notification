import json
import os
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)


channel = os.getenv("channel")
username = os.getenv("username")
hook_url = os.getenv("hook_url")

bootstrap_servers = os.getenv("bootstrap_servers")
auto_offset_reset = os.getenv("auto_offset_reset")
topic = os.getenv("topic")


consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset=auto_offset_reset,
)


def send(text):
    payload = {
        "channel": '@' + channel,
        "username": '%s' % username,
        "icon_url": '',
        "attachments": [{
            "pretext": "---- Kafka Notification ----",
            "text": text
        }]
    }
    headers = {'content-type': 'application/json'}
    r = requests.post(hook_url, data=json.dumps(payload), headers=headers)
    return r


if __name__ == "__main__":
    print("started notification queue!!!")
    try:
        for msg in consumer:
            try:
                if msg.value is None:
                    continue

                # Handle UTF
                try:
                    data = msg.value.decode()
                except Exception:
                    data = msg.value

                # Try to parse the message as JSON
                try:
                    app_msg = json.loads(data)
                except Exception:
                    print('Could not parse JSON, will just use raw message contents')
                    app_msg = data

                sc_response = send(app_msg['TEXT'])

                if sc_response.status_code != 200:
                    print('\t** FAILED: {}'.format(sc_response['error']))
            except Exception as e:
                print(e)
    except KeyboardInterrupt:
        print("notification stopeed!!!")
