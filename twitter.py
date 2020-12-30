import requests
import json
import pandas as pd
import datetime
import psycopg2
from sqlalchemy import create_engine
import myvariables

BEARER_TOKEN = myvariables.BEARER_TOKEN

today = datetime.datetime.now()
thirty_minutes_ago = today - datetime.timedelta(minutes=30)
thirty_minutes_ago = thirty_minutes_ago.isoformat()[0:20]+'000Z'


def validateData(df: pd.DataFrame) -> bool:

    # check it's not empty
    if df.empty:
        print('df empty')
        return False

    # check no duplicates
    if pd.Series(df['tweet_id']).is_unique:
        pass
    else:
        raise Exception('Primary key check violated.')

    # check no null values
    if df.isnull().values.any():
        raise Exception('At least one empty field')

    # check tweets no older than 30 mins
    for time in df['created_at']:
        time = datetime.datetime.fromisoformat(
            time[0:19])
        tma = datetime.datetime.fromisoformat(thirty_minutes_ago[0:19])
        end = datetime.datetime.fromisoformat(today[0:19])
        if time < tma or time > end:
            raise Exception('Tweet created out of range.')

    return True


headers = {
    "Authorization": "Bearer {bearer}".format(bearer=BEARER_TOKEN)
}

today = datetime.datetime.now()

if today.minute > 30: 
    today = today.replace(minute=30)
else: 
    today = today.replace(minute=0)

thirty_minutes_ago = today - datetime.timedelta(minutes=30)
thirty_minutes_ago = thirty_minutes_ago.isoformat()[0:20]+'000Z'
today = today.isoformat()[0:20]+'000Z'

r = requests.get('https://api.twitter.com/2/tweets/search/recent?query=nvda&tweet.fields=created_at&end_time={end}&start_time={start}&max_results=100'.format(
    start=thirty_minutes_ago, end=today), headers=headers)

if r.ok:
    print('success')
else:
    print('failed')
    print(r.status_code)

tweets = r.json()['data']

tweet_bodies = []
created_ats = []
tweet_ids = []


for tweet in tweets:
    tweet_bodies.append(tweet['text'])
    created_ats.append(tweet['created_at'])
    tweet_ids.append(tweet['id'])

my_dict = {
    "tweet_body": tweet_bodies,
    "created_at": created_ats,
    "tweet_id": tweet_ids
}

df = pd.DataFrame(my_dict, columns=['tweet_body', 'created_at', 'tweet_id'])

# remove retweets
df = df[df['tweet_body'].str[0:2] != 'RT']

df.reset_index(drop=True, inplace=True)


to_drop = []

for index, row in df.iterrows(): 
    if 'nvda' not in row['tweet_body'].lower(): 
        to_drop.append(index)

df.drop(df.index[to_drop], inplace=True)
df.reset_index(drop=True, inplace=True)

if validateData(df): 
    print('valid')
else: 
    print('inval')

# adjust time to USA time 
today = datetime.datetime.now() + datetime.timedelta(hours=-5)
if today.minute > 30: 
    today = today.replace(minute=30)
else: 
    today = today.replace(minute=0)

df['30m_candle_closing_nyc'] = str(today)[0:10] + ' ' + str(today)[11:16]
df = df[['tweet_body', 'tweet_id', '30m_candle_closing_nyc']].copy()

# df.rename(columns={'tweet_body':'TWEET_BODY'}, inplace=True)
# df.rename(columns={'tweet_id':'TWEET_ID'}, inplace=True)
df.rename(columns={'30m_candle_closing_nyc':'thirty_min_close_nyc'}, inplace=True)

print(df); 

con = psycopg2.connect(database="postgres", user="masteruser", password="password", host=myvariables.DB_CON_STRING, port="5432")

print("Database connected successfully")

cur = con.cursor()
cur.execute('''
    CREATE TABLE IF NOT EXISTS tweets(
    tweet_id VARCHAR(200) PRIMARY KEY NOT NULL,
    tweet_body VARCHAR(300) NOT NULL,
    thirty_min_close_nyc VARCHAR(200) NOT NULL);
    ''')

print("Table fine")

con.commit()
con.close()




engine = create_engine(myvariables.FULL_DB_CON_STRING)

print('connected with sqlalchemy')

df.to_sql('tweets', engine, index=False, if_exists='append')

print('write fine')


