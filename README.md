# Fledgling ETL Project

This is my first attempt at building something in the area of data. 

## Main premise

The project is 2 python scripts: 

### twitter.py
This script uses the twitter API to pull batches of up to 100 tweets that mention the stock ticker 'NVDA'. When executed, the script collects tweets between the last 30 minute block, cleans the data, and then pushes it into a SQL db. 

### finance.py
This script uses the yfinance API to pull stock data for 'NVDA', the most recent close of a 30 minute chart is pushed into a SQL db. 

This project was deployed on an AWS EC2 instance, and it was scheduled to repeat at 30 minute intervals during real trading hours using crontab: 

```
5,35 14-21 * * 1-5 python3 /home/ec2-user/etl-copy/twitter.py >/dev/null 2>&1
5,35 14-21 * * 1-5 python3 /home/ec2-user/etl-copy/finance.py >/dev/null 2>&1
```



