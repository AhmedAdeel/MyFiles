import sys
import json
import pandas as pd
import datetime as dt
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from copy import deepcopy

es = Elasticsearch(
    ["https://iwriter:iwriter786@e50d58dc07884bb5956a4a36d7c15aca.us-east-1.aws.found.io:9243"]
    ,timeout=60)

tmkt_index = "tmktcalllogs"
start_datetime = str(dt.datetime.now().today().date() - dt.timedelta(days= 2))
traindex = "traindex"

def get_next_call_time_and_disposition(call_id, number):
    c = [ doc['_source'] for doc in es.search(index= tmkt_index, body={
        'size': 10,
        "sort" : [
            {
          "start_timestamp": {
                "order": "asc"
              }
            }
          ],
        'query': {
            'bool': {
                'must': [
                    {'terms': {
                        'type_name.keyword': [
                            'Outbound', 'Manual'
                        ]
                    }},{'range': {
                        'call_id': {
                            'gt': call_id
                        }
                    }},{'terms': {
                        'DNIS.keyword': [number]
                    }}
                ]
            }
        },
        "_source": ["call_id","start_timestamp","end_timestamp","DNIS","disposition_name"]
    })['hits']['hits']]

    if len(c) > 0:
        return c[0]

    return None


def get_duration_in_seconds(d1, d2):
    try:

        if isinstance(d2, str): return 'N/A'

        if isinstance(d1, dt.datetime):
            seconds = (d2-d1).total_seconds()
        return seconds
    except Exception as e:
        print(e, 'duration')
        print(d1, d2)
        return 'N/A'


def get_duration_in_string(d1, d2):
    try:

        if isinstance(d2, str): return 'N/A'

        if isinstance(d1, dt.datetime):
            seconds = (d2-d1).total_seconds()
            if seconds > 0 and seconds < 61:
                return 'Within a min'
            elif seconds > 60 and seconds < 60*15:
                return 'Within 15 min'
            elif seconds > 60*15 and seconds < 60*30:
                return 'Within 30 min'
            elif seconds > 60*30 and seconds < 3600:
                return 'Within an hour'
            elif seconds > 3600 and seconds < 3600*3:
                return 'Within 3 hours'
            elif seconds > 3600*3 and seconds < 3600*6:
                return 'Within 6 hours'
            elif seconds > 3600*6 and seconds < 86400:
                return 'Within a day'
            elif 2592000 > seconds > 86400:
                return 'After 24 hours'
            elif seconds > 2592000:
                return 'After 1 month'
            else:
                pass
        return 'N/A'
    except Exception as e:
        print(e, 'duration')
        print(d1, d2)
        return 'N/A'


### Script to schedule
print("Ready to elastic query")

data = es.search(
    index= tmkt_index,
    scroll='10m',
    size=10000,
    body={
        'size': 10000,
        'query': {
            'bool': {
                'must': [
                    {'range': {
                        'start_timestamp': {
                            'gte': start_datetime}
                    }},
                    {'terms': {
                        'disposition_name.keyword': ['Sent To Voicemail', 'Abandon']
                    }},
                    {'terms': {
                        'type_name.keyword': [
                            'Inbound'
                        ]
                    }}
                ]}
        },
        "_source": ["call_id","start_timestamp","end_timestamp","ANI"]
    }
)

sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])

complete_index = []
while scroll_size > 0:
    complete_index += data['hits']['hits']
    data = es.scroll(scroll_id=sid, scroll='2m')
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

calls = [doc['_source'] for doc in complete_index]
print("Inbound Abandoned Calls found", len(calls))

calls_df = pd.DataFrame(calls)
calls_df['start_timestamp'] = calls_df.start_timestamp.apply(
    lambda x: pd.to_datetime(x).tz_localize('UTC').tz_convert('US/Eastern'))

calls_df['end_timestamp'] = calls_df.end_timestamp.apply(
    lambda x: pd.to_datetime(x).tz_localize('UTC').tz_convert('US/Eastern'))

print("Fetch Call backs for abandoned calls")
next_calls = []
for index, call in calls_df.iterrows():
    try:
        if index%100==0: print(index)

        c = get_next_call_time_and_disposition(call['call_id'], call['ANI'])
        if c is not None:
            next_calls.append({
                'next_call_start_timestamp': c['start_timestamp'],
                'next_call_end_timestamp': c['end_timestamp'],
                'call_id': call['call_id'],
                'next_call_id': c['call_id'],
                'next_call_disposition': c['disposition_name'],
            })
    except:
        print(c, call['call_id'])
#         break

print("Call backs found: ",len(next_calls))

next_calls_df = pd.DataFrame(next_calls)
next_calls_df['next_call_start_timestamp'] = next_calls_df.next_call_start_timestamp.apply(
    lambda x: pd.to_datetime(x).tz_localize('UTC').tz_convert('US/Eastern'))
next_calls_df['next_call_end_timestamp'] = next_calls_df.next_call_end_timestamp.apply(
    lambda x: pd.to_datetime(x).tz_localize('UTC').tz_convert('US/Eastern')
)

calls_df['call_id'] = calls_df['call_id'].map(str)
next_calls_df['call_id'] = next_calls_df['call_id'].map(str)

df = pd.merge(
    calls_df, next_calls_df, on='call_id', how='left').fillna('')

df = df[df['next_call_start_timestamp'] != '']

print("Calculate delay")
df['delay'] = df[['start_timestamp', 'next_call_start_timestamp']].apply(
    lambda x: get_duration_in_string(x[0], x[1]), axis=1)

df['delay_secs'] = df[['start_timestamp', 'next_call_start_timestamp']].apply(
    lambda x: get_duration_in_seconds(x[0], x[1]), axis=1)

print("Calls after 1 month", len(df[df.delay == 'After 1 month']))

df = df[df.delay != 'After 1 month']
print("After dropping",len(df)

df["callback"] = True
df = df[['call_id','next_call_disposition','callback','delay','delay_secs']]
df = df[df["delay_secs"] > 0]

print("Ready to update callbacks in elastic")
list_of_errors = []
count = 0
for index,row in df.iterrows():
    try:
        payload = {"doc" : {"callback" : True,
                            "callback_disposition": row['next_call_disposition'],
                            "callback_delay_secs": row['delay_secs'],
                           "callback_delay": row['delay']}}
        es.update(index= traindex ,doc_type='_doc',id=row['call_id'],
                    body=payload)
        count = count+1
        if count%1000 == 0:
            print("indexed ",count)
    except Exception as e:
        print("callid",row['call_id'])
        print(e)
        list_of_errors.append(row['call_id'])

print("callbacks updated")
print("errors",len(list_of_errors))
