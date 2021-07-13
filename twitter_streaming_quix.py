import requests
import os
import json
import pandas as pd
from datetime import datetime
from quixstreaming import *

certificatePath = "../certificates/ca.cert"

# use the values given in your starter project here
username = "USER_NAME"
password = "PASSWORD"
broker = "kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093"

security = SecurityOptions(certificatePath, username, password)
client = StreamingClient(broker, security)

# connect to the output topic
output_topic = client.open_output_topic("THE_TOPIC_ID_TO_WRITE_TO")


# define code to create the output stream
# you can change this to whatever you want
def create_stream():
    stream = output_topic.create_stream()
    stream.properties.name = "dogetweet_stream_results"
    return stream


# define the code to create the headers for the http connection
# dont forget to create the BEARER_TOKEN environment variable at deployment time
def create_headers(bearer_token):
    if bearer_token is None:
        raise Exception("Bearer token not set")

    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


# define the code to get the existing rules from the twitter api
def get_rules(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


# code to delete the rules..
def delete_all_rules(headers, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


# code to create the rules..
# in this example were searching for tweets about Bitcoin...
def set_rules(headers, delete):
    # You can adjust the rules if needed
    twitter_search = os.environ.get("twitter_search")
    sample_rules = [
        {"value": twitter_search, "tag": "DOGE tweets"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


# here were going to get the stream and handle its output
# we'll do this by streaming the results into Quix
def get_stream(headers, set, quix_stream):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))

            # get the data
            data = json_response["data"]
            # i want to store the tag in quix too so get the rules used to obtain this data
            matching_rules = json_response["matching_rules"]

            # write this data to quix
            quix_stream.parameters.buffer.add_timestamp(datetime.now()) \
                .add_tag("tag", matching_rules[0]["tag"]) \
                .add_value("tweet_id", data["id"]) \
                .add_value("text", data["text"]) \
                .write()


# start everything going..
def main():
    bearer_token = os.environ.get("bearer_token")
    # bearer_token = ""
    headers = create_headers(bearer_token)
    rules = get_rules(headers)
    delete = delete_all_rules(headers, rules)
    set = set_rules(headers, delete)
    quix_stream = create_stream()
    get_stream(headers, set, quix_stream)


if __name__ == "__main__":
    main()
{"mode":"full","isActive":false}
