# Kinesis Data Source

AWS Kinesis Stream Data Source

Development Installation:

Create new environment:

```commandline
virtualenv my_env
```

When starting development activate the environment
```commandline
source my_env/bin/activate
python setup.py install
```

Exiting current environment just run
```commandline
deactivate
```

Example to run:
```python
AWS_ACCESS_KEY_ID = 'ZIRaPZw2pUjIHr'
AWS_SECRET_ACCESS_KEY = 'ExIkSRQ8Nd'
AWS_REGION = 'us-east-1'
KINESIS_STREAM_NAME = 'KinesisStream-1J0FOY3HR4F5Q'


SOURCE = {
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
    'region_name': AWS_REGION,
    'stream_name': KINESIS_STREAM_NAME,
    # 'shards': {},
    # 'destination': ''
}
OPTIONS = {

}

stream = KinesisStream(source=SOURCE, options=OPTIONS)

total_records = []
while True:
    data = stream.read()
    if data is None:
        break
    print('Data {}'.format(len(data)))
    total_records += data

print('Total records {}'.format(total_records))
```

