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

# default destination name
DESTINATION = "kinesis_stream"

# Kinesis iterator types
ITERATOR_TYPE_AT = 'AT_SEQUENCE_NUMBER'
ITERATOR_TYPE_AFTER = 'AFTER_SEQUENCE_NUMBER'
ITERATOR_TYPE_TRIM = 'TRIM_HORIZON'
ITERATOR_TYPE_LATEST = 'LATEST'

# total number of elements to import
BATCH_MAX_SIZE = 5000

# each shard iterator result list
ITERATOR_MAX_RESULTS = 25

# Switch for debugging output
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
    """
    Exception decorator to catch all the exceptions coming
    from AWS client library. It is intercepting error response code
    and detecting resource or credentials errors.
    """

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

        return res

    return wrapped

"""
Error invoked when shard that was already present in cached memory
has expired, this can happen when there are changes to number of shards
in the stream.
"""
class ClosedShardError(Exception):
    def __init__(self, shard_id, message):
        super(ClosedShardError, self).__init__(message)
        self.shard_id = shard_id
        self.message = message


"""
Mixin class for logging events and invoking panoply exception,
every class that needs to output exceptions outside internal classes
will include this mixin
"""
class Logger():
    """
    method to log any message output if DEBUG it turned on it will
    print to the console otherwise it will pass log message to actual
    instance log
    """
    @staticmethod
    def log(content, instance=None):
        if DEBUG:
            print(content)
        elif instance is not None:
            instance.log(content)

    """
    method to invoke panoply exception that will be propagated 
    to the panoply core engine
    """
    @staticmethod
    def error(content, retryable=False):
        Logger.log('Error: {}'.format(content))
        raise panoply.PanoplyException(content, retryable=False)

    """
    instance method to call log internally if it is a sublass of
    panoply.DataSource it will call internal log
    """
    def local_log(self, content):
        if self.instance:
            Logger.log(content, self.instance)
        else:
            Logger.log(content)

"""
KinesisWorker is a thread class that will be used to process specific shard.
threads will perform in parallel and it will import up to maximum number of records
that is permitted by shard, in case of throttling of the api it will put it into sleep
for a predefined amount of time 
"""
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
        """
        it will import all the records available for this specific shard, on the setup phase
        the first import per shard will start by importing the latest records and the following
        ones will start importing from the last sequence number
        """
        retry_count = MAX_RETRIES
        all_records = []
        options = {
            'StreamName': self.stream_name,
            'ShardId': self.shard_id,
        }

        if self.shard_data['last_processed']:
            # after the initial import it will start importing from the last sequence number
            # that is available in descriptor for last record
            options['StartingSequenceNumber'] = self.shard_data['last_sequence_number']
            options['ShardIteratorType'] = ITERATOR_TYPE_AFTER
        else:
            # this one is used on the setup process and it will be used only for the first time
            options['ShardIteratorType'] = ITERATOR_TYPE_LATEST

        # get the initial iterator pointer, all the
        # subsequent will be received in the get all records
        iterator_response = self.client.get_shard_iterator(**options)
        self.shard_iterator = iterator_response['ShardIterator']

        while True:
            # loop until it reaches up to date iterator
            try:
                iteration_records, is_latest_iteration = self._get_iteration_records()
                all_records += iteration_records

                # important to break this after adding new records
                # otherwise it could be skipped
                if is_latest_iteration:
                    break

            except ClientError as err:
                # this error occurs when there is a api throttling
                self.local_log(err.message)

                if err.response['Error']['Code'] in RETRY_EXCEPTIONS:
                    # GetRecords has max size of 10mb of requests
                    retry_count -= 1
                    if retry_count > 0:
                        self.local_log('Exceeding number of requests per second, needs to go to sleep for {}'.format(
                            self.sleep_interval))
                        time.sleep(self.sleep_interval)
                    else:
                        # if it has passed allowed number of retries stop the worker
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
        # check if this is latest iteration
        is_latest_iteration = response['MillisBehindLatest'] == 0

        records = response['Records']
        if len(records) > 0:
            # process all the records to extract actual data
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

            # data can stay in shard for up to 72 hours and some shards can be removed a couple of day
            # afterwards so this is safe estimate of 7 days to make sure it will clean it up
            week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            last_processed = self.shard_data['last_processed']
            if last_processed and last_processed < week_ago:
                # this shard hasn't received no content for a week
                # it is old shard pending for removal
                self.deprecated_shard = True

        return record_data, is_latest_iteration


"""
KinesisStream will be importing data from the stream and also 
will be responsible to static methods that are needed during the setup 
phase
"""
class KinesisStream(panoply.DataSource, Logger):
    @staticmethod
    @exception_decorator
    def get_streams(aws_access_key_id, aws_secret_access_key, region_name):
        """
        It gets a list of streams for the presented account
        :param aws_access_key_id:
        :param aws_secret_access_key:
        :param region_name:
        :return: list of kinesis streams
        """
        client = KinesisStream.kinesis_client(aws_access_key_id,
                                              aws_secret_access_key,
                                              region_name)
        all_streams = []

        response = client.list_streams(Limit=200)
        all_streams = response.get('StreamNames', [])

        while True:
            # if there are more strems pull until you get all of them
            if response['HasMoreStreams']:
                response = client.list_streams(Limit=200, ExclusiveStartStreamName=all_streams[-1])
                all_streams = response.get('StreamNames', [])
            else:
                break

        return all_streams

    @staticmethod
    def kinesis_client(aws_access_key_id, aws_secret_access_key, region_name):
        """
        create kinesis client
        :param aws_access_key_id:
        :param aws_secret_access_key:
        :param region_name:
        :return: kinesis client
        """
        return boto3.client(
            'kinesis',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def __init__(self, source, options):
        super(KinesisStream, self).__init__(source, options)

        # define destination in the source
        source.setdefault('destination', DESTINATION)

        # define cached list of shards for every import session
        # to have latest sequence number pointer
        self.shards = source.setdefault('shards', {})
        self.shard_count = len(self.shards)

        self.source = source
        self.stream_name = self.source.get('stream_name')
        self.client = KinesisStream.kinesis_client(source.get('aws_access_key_id'),
                                                   source.get('aws_secret_access_key'),
                                                   source.get('region_name'))

        self.options = options
        self.instance = self


    @exception_decorator
    def read(self):
        # import/update available shards for this stream
        self.shards = self.process_stream_shards(self.shards, self.stream_name)
        self.shard_count = len(self.shards)

        # divide evenly number of records for every shard import
        max_record_count = BATCH_MAX_SIZE / self.shard_count
        total_records = []
        threads = []
        options = {
            'max_record_count': max_record_count,
            'client': self.client,
            'instance': self
        }

        # setup thread worker for every shard
        for shard_id, shard_data in self.shards.items():
            worker = KinesisWorker(self.stream_name, shard_id,
                                   options=options,
                                   shard_data=shard_data)
            worker.daemon = True
            threads.append(worker)

            self.local_log('Shard "{}" Worker has started with import'.format(shard_id))
            worker.start()

        # wait to complete all the workers before continuing
        [thread.join() for thread in threads]

        # import records from every worker
        for thread in threads:
            total_records += thread.records

            # update shard information from response
            # if the shard cannot receive any content anymore mark
            # for removal
            if thread.deprecated_shard:
                self.shards.pop(thread.shard_id, None)

            # shard iterator options should be updated
            self.shards[thread.shard_id] = thread.get_shard_data()

        # update the shards iterator information for the next session
        self.source['shards'] = self.shards

        # define when to stop specific batch import
        if len(total_records) > 0:
            return total_records
        else:
            return None

    @exception_decorator
    def get_stream_shards(self, stream_name):
        """
        gets the information about the stream
        :param stream_name:
        :return: list of shards for the selected stream
        """
        stream = self.client.describe_stream(StreamName=stream_name)

        # it needs to check whether the response is actually having this dictionary
        # from experience sometimes AWS api return missing content in some edge cases
        # that are actually not invoking errors
        # for example removing this stream will for some time return results and then
        # it will return error that this stream doesn't exist
        description = stream.get('StreamDescription', {})

        return description('Shards', [])

    @exception_decorator
    def process_stream_shards(self, shards, stream_name):
        """
        It imports/updates information about the shards for the selected stream
        :return: updated object that contains a list of shard information
        """
        shard_list = self.get_stream_shards(stream_name)

        for shard in shard_list:
            shard_id = shard['ShardId']

            if shard_id not in shards:
                sequence_number = shard['SequenceNumberRange']['StartingSequenceNumber']
                shards[shard_id] = {
                    'last_sequence_number': sequence_number,
                    'last_processed': None
                }

        return shards
