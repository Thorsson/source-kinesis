from __future__ import print_function

import boto3
import datetime
import time
import json
import panoply
from botocore.exceptions import ClientError
import botocore
import threading
from functools import wraps

DESTINATION = "kinesis_stream"
# kinesis iterator types
ITERATOR_TYPE = {
    'at_sequence_number': 'AT_SEQUENCE_NUMBER',
    'after_sequence_number': 'AFTER_SEQUENCE_NUMBER',
    'trim': 'TRIM_HORIZON',
    'latest': 'LATEST'
}
# total number of elements to import
BATCH_MAX_SIZE = 5000
# each shard iterator result list
ITERATOR_MAX_RESULTS = 25
DEBUG = False

# on throttling how many times to repeat
MAX_RETRIES = 5
# on throttling how much time should go into sleep
SLEEP_INTERVAL = 5

# exceptions
RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')
RESOURCE_EXCEPTIONS = ('ResourceNotFoundException',
                       'NoSuchEntityException')
CREDENTIALS_EXCEPTIONS = ('UnrecognizedClientException',
                          'InvalidSignatureException')


def exception_decorator(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            res = f(*args, **kwargs)
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] in RESOURCE_EXCEPTIONS:
                # exceptions related to kinesis resources
                Logger.log('Resource exception')
                Logger.error(err, False)
            if err.response['Error']['Code'] in CREDENTIALS_EXCEPTIONS:
                # exceptions related to credentials
                Logger.log('Credentials exception')
                Logger.error(err, False)

        except AttributeError as err:
            # missing attributes
            Logger.error(err)

        if 'res' in locals():
            return res

    return wrapped


class ClosedShardError(Exception):
    def __init__(self, shard_id, message):
        self.shard_id = shard_id
        self.message = message


class Logger():
    @staticmethod
    def log(content, instance=None):
        if DEBUG:
            print(content)
        elif instance is not None:
            instance.log(content)

    @staticmethod
    def error(content, retryable=False):
        Logger.log('Error: {}'.format(content))
        raise panoply.PanoplyException(content, retryable=False)

    def local_log(self, content):
        if self.instance:
            Logger.log(content, self.instance)
        else:
            Logger.log(content)


class KinesisWorker(threading.Thread, Logger):
    def __init__(self, stream_name, shard_id,
                 shard_data={},
                 options={},
                 sleep_interval=SLEEP_INTERVAL,
                 name=None,
                 group=None,
                 args=(),
                 kwargs={}):
        if name is None:
            name = shard_id

        super(KinesisWorker, self).__init__(name=name,
                                            group=group,
                                            args=args,
                                            kwargs=kwargs)
        self.stream_name = stream_name
        self.shard_id = str(shard_id)
        self.sleep_interval = sleep_interval
        self.total_records = 0
        self.max_record_count = options.get('max_record_count', 500)
        self.client = options.get('client', None)
        self.options = options
        self.shard_data = shard_data
        self.original_shard_data = shard_data.copy()
        self.records = []
        self.deprecated_shard = False
        self.shard_iterator = None
        self.instance = options.get('instance', None)

    def run(self):
        try:
            self.records = self._get_shard_records()
        except ClosedShardError as err:
            self.local_log('Kinesis shard "{}" has been closed'.format(self.shard_id))
            self.deprecated_shard = True

        self.local_log('Shard {} Worker import is finished'.format(self.shard_id))

    def _get_shard_records(self):
        retry_count = MAX_RETRIES
        all_records = []
        options = {
            'StreamName': self.stream_name,
            'ShardId': self.shard_id,
        }

        if not self.shard_data['last_processed']:
            options['ShardIteratorType'] = ITERATOR_TYPE['latest']
        else:
            options['StartingSequenceNumber'] = self.shard_data['last_sequence_number']
            options['ShardIteratorType'] = ITERATOR_TYPE['after_sequence_number']

        iterator_response = self.client.get_shard_iterator(**options)
        self.shard_iterator = iterator_response['ShardIterator']

        while True:
            try:
                iteration_records, is_latest_iteration = self._get_iteration_records()
                all_records += iteration_records

                # important to break this after adding new records
                # otherwise it could be skipped
                if is_latest_iteration:
                    break

            except ClientError as err:
                self.local_log(err.message)

                code = err.response['Error']['Code']

                if code in RETRY_EXCEPTIONS:
                    # GetRecords has max size of 10mb of requests
                    retry_count -= 1
                    if retry_count > 0:
                        self.local_log('Exceeding number of requests per second, needs to go to sleep for {}'.format(
                            self.sleep_interval))
                        time.sleep(self.sleep_interval)
                    else:
                        break
                else:
                    break

        return all_records

    def _get_iteration_records(self):
        record_data = []
        is_latest_iteration = False

        if self.max_record_count <= 0:
            return record_data

        max_size_per_shard = self.max_record_count
        record_limit = max_size_per_shard
        if record_limit > ITERATOR_MAX_RESULTS:
            record_limit = ITERATOR_MAX_RESULTS

        response = self.client.get_records(ShardIterator=self.shard_iterator, Limit=record_limit)

        if 'NextShardIterator' not in response:
            # shard has been closed due to merging/splitting of the shard
            raise ClosedShardError(self.shard_id, 'Shard has been closed for {}'.format(self.shard_id))

        self.shard_iterator = response['NextShardIterator']
        records = response['Records']
        is_latest_iteration = response['MillisBehindLatest'] == 0

        if len(records) > 0:
            for record in records:
                # add processed records to the list of data
                data = json.loads(record['Data'].decode("utf-8"))
                record_data.append(data)

            # update sequence number for next iterator and last process import
            self.shard_data['last_processed'] = datetime.datetime.now()
            self.shard_data['last_sequence_number'] = records[-1]['SequenceNumber']

            self.max_record_count -= len(record_data)
        else:
            self.local_log('No available records in shard "{}"'.format(self.shard_id))

            week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            last_processed = self.shard_data['last_processed']
            if last_processed and last_processed < week_ago:
                # this shard hasn't received no content for a week
                # it is old shard pending for removal
                self.deprecated_shard = True

        return record_data, is_latest_iteration

    def get_shard_data(self):
        # return changed options on otherwise revert to original one
        return self.shard_data


class KinesisStream(panoply.DataSource, Logger):
    @staticmethod
    @exception_decorator
    def get_streams(aws_access_key_id, aws_secret_access_key, region_name):
        client = KinesisStream.kinesis_client(aws_access_key_id,
                                              aws_secret_access_key,
                                              region_name)
        streams = client.list_streams(Limit=200)

        if 'StreamNames' in streams:
            return streams['StreamNames']
        else:
            return []

    @staticmethod
    def kinesis_client(aws_access_key_id, aws_secret_access_key, region_name):
        return boto3.client(
            'kinesis',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def __init__(self, source, options):
        super(KinesisStream, self).__init__(source, options)

        if source.get('destination') is None:
            source['Destination'] = DESTINATION

        if source.get('shards') is None:
            source['shards'] = {}

        self.source = source
        self.aws_access_key_id = self.source.get('aws_access_key_id')
        self.aws_secret_access_key = self.source.get('aws_secret_access_key')
        self.region_name = self.source.get('region_name')
        self.stream_name = self.source.get('stream_name')

        self.options = options
        self.shards = source.get('shards')
        self.shard_count = len(self.shards)
        self.instance = self
        self.client = KinesisStream.kinesis_client(self.aws_access_key_id,
                                                   self.aws_secret_access_key,
                                                   self.region_name)

        self.processed_records = BATCH_MAX_SIZE

    @exception_decorator
    def read(self):
        if self.processed_records <= 0:
            return None

        self.process_stream_shards()

        max_record_count = BATCH_MAX_SIZE / self.shard_count
        total_records = []
        threads = []
        options = {
            'max_record_count': max_record_count,
            'client': self.client,
            'instance': self
        }

        for shard_id, shard_data in self.shards.items():
            worker = KinesisWorker(self.stream_name, shard_id,
                                   options=options,
                                   shard_data=shard_data)
            worker.daemon = True
            threads.append(worker)

            self.local_log('Shard "{}" Worker has started with import'.format(shard_id))
            worker.start()

        for thread in threads:
            thread.join()

        for thread in threads:
            thread_records = thread.records
            total_records += thread_records

            # update shard information from response
            # if the shard cannot receive any content anymore mark
            # for removal
            if thread.deprecated_shard:
                self.shards.pop(thread.shard_id, None)

            # shard iterator options should be updated
            self.shards[thread.shard_id] = thread.get_shard_data()

        # update the shards iterator information for the next session
        self.source['shards'] = self.shards

        # make sure to finish one lifecycle after reaching max level
        self.processed_records -= len(total_records)

        if len(total_records) > 0:
            return total_records
        else:
            return None

    @exception_decorator
    def process_stream_shards(self):
        stream = self.client.describe_stream(StreamName=self.stream_name)

        if 'StreamDescription' in stream:
            description = stream['StreamDescription']

            for shard in description['Shards']:
                shard_id = shard['ShardId']

                if shard_id not in self.shards:
                    sequence_number = shard['SequenceNumberRange']['StartingSequenceNumber']
                    self.shards[shard_id] = {
                        'last_sequence_number': sequence_number,
                        'last_processed': None
                    }

        self.shard_count = len(self.shards)

        return self.shards
