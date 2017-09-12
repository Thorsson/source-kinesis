import copy
import unittest
import botocore
import panoply
from botocore.exceptions import ClientError
from mock import patch

import kinesis
from data import test_fixtures

KinesisStream = kinesis.Stream

orig = botocore.client.BaseClient._make_api_call


def create_response(acting_operation_name, response_data=None, raise_exception=False):
    operation = acting_operation_name

    def mock_make_api_call(self, operation_name, kwarg):
        if type(operation) is list:
            iteration_data = operation.pop(0)
            acting_operation_name = iteration_data['name']
            response = iteration_data['response']
            invoke_exception = iteration_data.get('raise_exception', False)
        else:
            acting_operation_name = operation
            response = response_data
            invoke_exception = raise_exception

        if operation_name == acting_operation_name:
            if invoke_exception:
                raise ClientError(response, operation_name)
            else:
                return response

        return orig(self, operation_name, kwarg)

    return mock_make_api_call


def prepare_processing_data():
    single_shard_stream = copy.deepcopy(test_fixtures.stream_details)
    single_shard_stream['StreamDescription']['Shards'] = [
        {
            'ShardId': 'shardId-000000000002',
            'ParentShardId': 'shardId-000000000000',
            'HashKeyRange': {
                'StartingHashKey': '0',
                'EndingHashKey': '113427455640312821154458202477256070484'
            },
            'SequenceNumberRange': {
                'StartingSequenceNumber': '49576779325192435059829537036290334312001617382801932322'
            }
        }
    ]

    data = [
        # 1 get shards
        {
            'name': 'DescribeStream',
            'response': single_shard_stream
        },
        # 2 get iterator
        {
            'name': 'GetShardIterator',
            'response': test_fixtures.iterator_response
        },
        # 3 get records
        {
            'name': 'GetRecords',
            'response': test_fixtures.shard_with_records
        },
        # 4 get next data batch to terminate import
        {
            'name': 'GetRecords',
            'response': test_fixtures.shard_no_records
        }
    ]
    return data


class TestKinesis(unittest.TestCase):
    def test_wrong_access_key(self):
        credentials = {
            'aws_access_key_id': 'accesskey34535345',
            'aws_secret_access_key': 'secretaccess34645365465',
            'region_name': 'us-east-1',
        }

        response_method = create_response('ListStreams', test_fixtures.authentication_error, raise_exception=True)
        with patch('botocore.client.BaseClient._make_api_call', new=response_method), \
             self.assertRaises(panoply.PanoplyException) as context:
            streams = KinesisStream.get_streams(**credentials)

        error_message = 'An error occurred (UnrecognizedClientException) when calling the ListStreams operation: The security token included in the request is invalid.'
        self.assertEqual(str(context.exception.message), error_message)

    def test_wrong_access_secret(self):
        credentials = {
            'aws_access_key_id': 'accesskey34535345',
            'aws_secret_access_key': 'secretaccess34645365465',
            'region_name': 'us-east-1',
        }

        response_method = create_response('ListStreams', test_fixtures.authentication_secret_error,
                                          raise_exception=True)
        with patch('botocore.client.BaseClient._make_api_call', new=response_method), \
             self.assertRaises(panoply.PanoplyException) as context:
            streams = KinesisStream.get_streams(**credentials)

        error_message = 'An error occurred (InvalidSignatureException) when calling the ListStreams operation: The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.'
        self.assertEqual(str(context.exception.message), error_message)

    def test_missing_stream(self):
        SOURCE = {
            'aws_access_key_id': 'accesskey34535345',
            'aws_secret_access_key': 'secretaccess34645365465',
            'region_name': 'us-east-1',
            'stream_name': 'KinesisStream-1J0FOY3HR4F5Q'
        }
        OPTIONS = {}

        stream = KinesisStream(source=SOURCE, options=OPTIONS)

        response_method = create_response('DescribeStream', test_fixtures.stream_missing_error, raise_exception=True)
        with patch('botocore.client.BaseClient._make_api_call', new=response_method), \
             self.assertRaises(panoply.PanoplyException) as context:
            stream.process_stream_shards()

        error_message = 'An error occurred (ResourceNotFoundException) when calling the DescribeStream operation: Stream KinesisStream-1J0FOY3HR4F5Q under account 664727738565 not found.'
        self.assertEqual(str(context.exception.message), error_message)

    def test_get_list_streams(self):
        response_method = create_response('ListStreams', test_fixtures.stream_list)
        with patch('botocore.client.BaseClient._make_api_call', new=response_method):
            credentials = {
                'aws_access_key_id': 'accesskey34535345',
                'aws_secret_access_key': 'secretaccess34645365465',
                'region_name': 'us-east-1',
            }

            streams = KinesisStream.get_streams(**credentials)

            self.assertEqual(len(streams), 1)
            self.assertEqual(streams[0], 'KinesisStream-1J0FOY3HR4F5Q')

    def test_get_stream_shards(self):
        SOURCE = {
            'aws_access_key_id': 'accesskey34535345',
            'aws_secret_access_key': 'secretaccess34645365465',
            'region_name': 'us-east-1',
            'stream_name': 'KinesisStream-1J0FOY3HR4F5Q'
        }
        OPTIONS = {}

        stream = KinesisStream(source=SOURCE, options=OPTIONS)

        response_method = create_response('DescribeStream', test_fixtures.stream_details)
        with patch('botocore.client.BaseClient._make_api_call', new=response_method):
            stream.process_stream_shards()
            shards = stream.shards

            self.assertEqual(len(shards), 3)
            self.assertEqual(shards['shardId-000000000005'], {
                'last_sequence_number': '49576779335963694990719828013652086237690778051774775378',
                'last_processed': None
            })

    def test_initial_stream_records(self):
        SOURCE = {
            'aws_access_key_id': 'accesskey34535345',
            'aws_secret_access_key': 'secretaccess34645365465',
            'region_name': 'us-east-1',
            'stream_name': 'KinesisStream-1J0FOY3HR4F5Q'
        }
        OPTIONS = {}

        stream = KinesisStream(source=SOURCE, options=OPTIONS)

        operation_content = prepare_processing_data()
        response_method = create_response(operation_content)
        with patch('botocore.client.BaseClient._make_api_call', new=response_method):
            data = stream.read()

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['referrer'], 'http://www.facebook.com')
        self.assertEqual(data[0]['resource'], '/index.html')


# run the tests
if __name__ == "__main__":
    unittest.main()
