import yfinance as yf
import datetime
from sqlalchemy import create_engine
import psycopg2
import myvariables 

nvda = yf.Ticker('NVDA')

hist = nvda.history(period="2d", interval="30m")

df = hist[['Open', 'High', 'Low', 'Close']].copy()
df['CloseTime'] = df.index
df.reset_index(drop=True, inplace=True)

for index, row in df.iterrows():
    df['CloseTime'][index] = str(df.iloc[index]['CloseTime'])[0: 16]

# check that we're getting the most recent one we want
# set time to 5 hours ago (UK to USA)
dt = datetime.datetime.now() - datetime.timedelta(hours=5)
if dt.minute > 30: 
    dt = dt.replace(minute=30)
else: 
    dt = dt.replace(minute=0)

dt = str(dt)[0:16]

df = df[df['CloseTime'] == dt]

df['returns_percent'] = (df['Close'] - df['Open']) * 100 / df['Open']
df['volatility_percent'] = (df['High'] - df['Low']) * 100 / df['Open']

df.rename(columns={'Open':'open'}, inplace=True)
df.rename(columns={'High':'high'}, inplace=True)
df.rename(columns={'Low':'low'}, inplace=True)
df.rename(columns={'Close':'close'}, inplace=True)
df.rename(columns={'CloseTime':'closetime'}, inplace=True)

print(df)


# connect to db 
con = psycopg2.connect(database="postgres", user="masteruser", password="password", host=myvariables.DB_CON_STRING, port="5432")
print('Connected to db')
cur = con.cursor()
cur.execute('''
    CREATE TABLE IF NOT EXISTS stock_data(
        open NUMERIC NOT NULL, 
        high NUMERIC NOT NULL, 
        low NUMERIC NOT NULL, 
        close NUMERIC NOT NULL, 
        closetime VARCHAR(200) NOT NULL, 
        returns_percent NUMERIC NOT NULL, 
        volatility_percent NUMERIC NOT NULL
    );
'''); 
print("Table fine")
con.commit()
con.close()

#write to db
engine = create_engine(myvariables.FULL_DB_CON_STRING)
print('connected with sqlalchemy')
df.to_sql('stock_data', engine, index=False, if_exists='append')
print('write fine')




