from transformers import pipeline
import unicodedata
from quixstreaming import *
import signal
import threading
from bs4 import BeautifulSoup
import re
import itertools
import emoji
import traceback
import numpy as np
import os
import datetime
from dateutil import parser

input_label = 'tweet-text'
classifier = pipeline('sentiment-analysis')

# Create a client factory. Factory helps you create StreamingClient (see below) a little bit easier
security = SecurityOptions("../certificates/ca.cert", "USER_NAME", "PASSWORD")
client = StreamingClient('kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093', security)

# To get more info about consumer group,
# see https://documentation.dev.quix.ai/quix-main/demo-quix-docs/concepts/kafka.html
consumer_group = "twitter-sentiment-model"

# input_topic = client.open_input_topic('mc2568-sentimentanalysis-twitter-stream', consumer_group)
input_topic = client.open_input_topic('TWEETS_TOPIC_ID')
output_topic = client.open_output_topic('SENTIMENT_ANALYSIS_STATS_TOPICID')

stream = output_topic.create_stream("sentiment-stats")

# Give the stream human readable name. This name will appear in data catalogue.
stream.properties.name = "Sentiment Results"


def strip_accents(text):
    # length_initial=len(text)
    # initial_text=text
    if 'ø' in text or 'Ø' in text:
        return text
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)


def load_dict_smileys():
    return {
        ":‑)": "smiley",
        ":-]": "smiley",
        ":-3": "smiley",
        ":->": "smiley",
        "8-)": "smiley",
        ":-}": "smiley",
        ":)": "smiley",
        ":]": "smiley",
        ":3": "smiley",
        ":>": "smiley",
        "8)": "smiley",
        ":}": "smiley",
        ":o)": "smiley",
        ":c)": "smiley",
        ":^)": "smiley",
        "=]": "smiley",
        "=)": "smiley",
        ":-))": "smiley",
        ":‑D": "smiley",
        "8‑D": "smiley",
        "x‑D": "smiley",
        "X‑D": "smiley",
        ":D": "smiley",
        "8D": "smiley",
        "xD": "smiley",
        "XD": "smiley",
        ":‑(": "sad",
        ":‑c": "sad",
        ":‑<": "sad",
        ":‑[": "sad",
        ":(": "sad",
        ":c": "sad",
        ":<": "sad",
        ":[": "sad",
        ":-||": "sad",
        ">:[": "sad",
        ":{": "sad",
        ":@": "sad",
        ">:(": "sad",
        ":'‑(": "sad",
        ":'(": "sad",
        ":‑P": "playful",
        "X‑P": "playful",
        "x‑p": "playful",
        ":‑p": "playful",
        ":‑Þ": "playful",
        ":‑þ": "playful",
        ":‑b": "playful",
        ":P": "playful",
        "XP": "playful",
        "xp": "playful",
        ":p": "playful",
        ":Þ": "playful",
        ":þ": "playful",
        ":b": "playful",
        "<3": "love"
    }


def load_dict_contractions():
    return {
        "ain't": "is not",
        "amn't": "am not",
        "aren't": "are not",
        "can't": "cannot",
        "'cause": "because",
        "couldn't": "could not",
        "couldn't've": "could not have",
        "could've": "could have",
        "daren't": "dare not",
        "daresn't": "dare not",
        "dasn't": "dare not",
        "didn't": "did not",
        "doesn't": "does not",
        "don't": "do not",
        "e'er": "ever",
        "em": "them",
        "everyone's": "everyone is",
        "finna": "fixing to",
        "gimme": "give me",
        "gonna": "going to",
        "gon't": "go not",
        "gotta": "got to",
        "hadn't": "had not",
        "hasn't": "has not",
        "haven't": "have not",
        "he'd": "he would",
        "he'll": "he will",
        "he's": "he is",
        "he've": "he have",
        "how'd": "how would",
        "how'll": "how will",
        "how're": "how are",
        "how's": "how is",
        "I'd": "I would",
        "I'll": "I will",
        "I'm": "I am",
        "I'm'a": "I am about to",
        "I'm'o": "I am going to",
        "isn't": "is not",
        "it'd": "it would",
        "it'll": "it will",
        "it's": "it is",
        "I've": "I have",
        "kinda": "kind of",
        "let's": "let us",
        "mayn't": "may not",
        "may've": "may have",
        "mightn't": "might not",
        "might've": "might have",
        "mustn't": "must not",
        "mustn't've": "must not have",
        "must've": "must have",
        "needn't": "need not",
        "ne'er": "never",
        "o'": "of",
        "o'er": "over",
        "ol'": "old",
        "oughtn't": "ought not",
        "shalln't": "shall not",
        "shan't": "shall not",
        "she'd": "she would",
        "she'll": "she will",
        "she's": "she is",
        "shouldn't": "should not",
        "shouldn't've": "should not have",
        "should've": "should have",
        "somebody's": "somebody is",
        "someone's": "someone is",
        "something's": "something is",
        "that'd": "that would",
        "that'll": "that will",
        "that're": "that are",
        "that's": "that is",
        "there'd": "there would",
        "there'll": "there will",
        "there're": "there are",
        "there's": "there is",
        "these're": "these are",
        "they'd": "they would",
        "they'll": "they will",
        "they're": "they are",
        "they've": "they have",
        "this's": "this is",
        "those're": "those are",
        "'tis": "it is",
        "'twas": "it was",
        "wanna": "want to",
        "wasn't": "was not",
        "we'd": "we would",
        "we'd've": "we would have",
        "we'll": "we will",
        "we're": "we are",
        "weren't": "were not",
        "we've": "we have",
        "what'd": "what did",
        "what'll": "what will",
        "what're": "what are",
        "what's": "what is",
        "what've": "what have",
        "when's": "when is",
        "where'd": "where did",
        "where're": "where are",
        "where's": "where is",
        "where've": "where have",
        "which's": "which is",
        "who'd": "who would",
        "who'd've": "who would have",
        "who'll": "who will",
        "who're": "who are",
        "who's": "who is",
        "who've": "who have",
        "why'd": "why did",
        "why're": "why are",
        "why's": "why is",
        "won't": "will not",
        "wouldn't": "would not",
        "would've": "would have",
        "y'all": "you all",
        "you'd": "you would",
        "you'll": "you will",
        "you're": "you are",
        "you've": "you have",
        "Whatcha": "What are you",
        "luv": "love"
    }


def tweet_cleaning_for_sentiment_analysis(tweet):
    # Escaping HTML characters
    tweet = BeautifulSoup(tweet, features="html.parser").get_text()
    tweet = tweet.replace('\x92', "'")

    # REMOVAL of hastags/account
    tweet = ' '.join(re.sub("(@[A-Za-z0-9_]+)|(#[A-Za-z0-9_]+)", " ", tweet).split())
    # Removal of address
    tweet = ' '.join(re.sub("(\w+:\/\/\S+)", " ", tweet).split())
    ## LOWER CASE
    tweet = tweet.lower()

    # Apostrophe Lookup #https://en.wikipedia.org/wiki/Contraction_%28grammar%29
    APPOSTOPHES = load_dict_contractions()
    tweet = tweet.replace("’", "'")
    words = tweet.split()
    reformed = [APPOSTOPHES[word] if word in APPOSTOPHES else word for word in words]
    tweet = " ".join(reformed)
    tweet = ''.join(''.join(s)[:2] for _, s in itertools.groupby(tweet))

    # Deal with EMOTICONS
    # https://en.wikipedia.org/wiki/List_of_emoticons
    SMILEY = load_dict_smileys()  # {"<3" : "love", ":-)" : "smiley", "" : "he is"}
    words = tweet.split()
    reformed = [SMILEY[word] if word in SMILEY else word for word in words]
    tweet = " ".join(reformed)
    tweet = emoji.demojize(tweet)

    # Strip accents
    tweet = strip_accents(tweet)
    tweet = tweet.replace(":", " ")
    tweet = ' '.join(tweet.split())
    return tweet


# read streams
def read_stream(new_stream: StreamReader):
    buffer = new_stream.parameters.create_buffer()

    def on_parameter_data_handler(data: ParameterData):

        df = data.to_panda_frame()

        # We iterate all rows and log the scores
        for index, row in df.iterrows():
            thetweet = row["text"]
            finaltext = unicodedata.normalize('NFKD', thetweet).encode('ascii', 'ignore')
            finaltext = finaltext.decode('utf8', 'replace')
            finaltext = tweet_cleaning_for_sentiment_analysis(finaltext)
            
            # Make sure we don't try to evaluate any reteets
            if (finaltext[0:3] != 'rt '):
                try:
                    sent = classifier(finaltext)
                    label = sent[0]['label']
                    score = sent[0]['score']

                    # Invert the negative score so that graph looks better
                    if (label == 'NEGATIVE'):
                        score = score - (score * 2)

                    # Calculate a very basic rolling average
                    global readings
                    readings = np.append(readings, score)
                    avgscore = np.mean(readings)

                    if len(readings) == max_samples:
                        readings = np.delete(readings, 0)

                    print(label, score, avgscore, finaltext)

                    # For every X (max_samples) tweets we save average sentiment values.
                    stream.parameters.buffer.add_timestamp(datetime.datetime.now()) \
                        .add_value("avgscore", avgscore) \
                        .add_value("score", score) \
                        .write()

                except Exception:
                    print(traceback.format_exc())

    buffer.on_read += on_parameter_data_handler


readings = np.array([])
max_samples = os.environ.get("max_samples")

# Hook up events before initiating read to avoid losing out on any data
input_topic.on_stream_received += read_stream
input_topic.start_reading()  # initiate read

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

event = threading.Event()


def signal_handler(sig, frame):
    print('Exiting...')
    event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
event.wait()
{"mode":"full","isActive":false}
