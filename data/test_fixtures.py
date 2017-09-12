# data for the responses

# error access key is invalid
import datetime

from dateutil.tz import tzlocal

authentication_error = {
    'Error': {
        'Message': 'The security token included in the request is invalid.',
        'Code': 'UnrecognizedClientException'
    },
    'ResponseMetadata': {
        'RequestId': 'e53f8ac5-1a2b-42fc-b70e-1ff5315d1df8',
        'HTTPStatusCode': 400,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'e53f8ac5-1a2b-42fc-b70e-1ff5315d1df8',
            'x-amz-id-2': 'uRzpNIvfIZJlACONY66C8kCvjpgEUGjUrKH3dxGnTamCvsYpr9JO7BB04xEiVs646QjsPt0mfrmJ1BK84ZU/0X0h9n9+3MVQ',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '107',
            'date': 'Tue, 12 Sep 2017 07:55:28 GMT',
            'connection': 'close'
        },
        'RetryAttempts': 0
    }
}

# error secret key is invalid
authentication_secret_error = {
    'Error': {
        'Message': 'The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.',
        'Code': 'InvalidSignatureException'
    },
    'ResponseMetadata': {
        'RequestId': 'e3af0dc7-4edc-da38-b19e-98b93787ca3d',
        'HTTPStatusCode': 400,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'e3af0dc7-4edc-da38-b19e-98b93787ca3d',
            'x-amz-id-2': 'mLvxY1BA6FuKWl/xJOELlPFWb5S0MVPQNCbJChKLtsvqdy9HYXOo7zuFvd6QtjRFwnm7riMoqTRWDpIZCZlR4i1kixUzG1MYn5Gf+SWkBdY=',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '229',
            'date': 'Tue, 12 Sep 2017 07:56:46 GMT',
            'connection': 'close'
        },
        'RetryAttempts': 0
    }
}

# error stream does not exist
stream_missing_error = {
    'Error': {
        'Message': 'Stream KinesisStream-1J0FOY3HR4F5Q under account 664727738565 not found.',
        'Code': 'ResourceNotFoundException'
    },
    'ResponseMetadata': {
        'RequestId': 'cc9246a8-1268-5395-9ea3-d36bf02db2a8',
        'HTTPStatusCode': 400,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'cc9246a8-1268-5395-9ea3-d36bf02db2a8',
            'x-amz-id-2': 'DuYbWmfxiElTdxdmMo7DZtt3U8sSpOMuyOVqkwy3DgX2A0supxT6xnNhV2LSlgAU8ujZpa6RfWX2HDsCtKGF6Op2lHFDzZTE',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '148',
            'date': 'Tue, 12 Sep 2017 07:57:55 GMT',
            'connection': 'close'
        },
        'RetryAttempts': 0
    }
}

# streams list
stream_list = {
    'StreamNames': [
        'KinesisStream-1J0FOY3HR4F5Q'
    ],
    'HasMoreStreams': False,
    'ResponseMetadata': {
        'RequestId': 'c982b0ee-f102-f133-9bb3-222bae8cfa80',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'c982b0ee-f102-f133-9bb3-222bae8cfa80',
            'x-amz-id-2': 'VOkBLZcnmAdu364U2EyoHEU7N9xxzVu2uzgAhm/HAK5f2HOQ6y6VZRclVL1beYYojllpITAsCwTK+RHsXSxMIIxqyLC+4A4LP8xPrm9XRfw=',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '94',
            'date': 'Tue, 12 Sep 2017 07:45:08 GMT'
        },
        'RetryAttempts': 0
    }
}

# stream details with shards
stream_details = {
    'StreamDescription': {
        'StreamName': 'KinesisStream-1J0FOY3HR4F5Q',
        'StreamARN': 'arn:aws:kinesis:us-east-1:664727738565:stream/KinesisStream-1J0FOY3HR4F5Q',
        'StreamStatus': 'ACTIVE',
        'Shards': [
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
            },
            {
                'ShardId': 'shardId-000000000005',
                'ParentShardId': 'shardId-000000000001',
                'HashKeyRange': {
                    'StartingHashKey': '226854911280625642308916404954512140970',
                    'EndingHashKey': '340282366920938463463374607431768211455'
                },
                'SequenceNumberRange': {
                    'StartingSequenceNumber': '49576779335963694990719828013652086237690778051774775378'
                }
            },
            {
                'ShardId': 'shardId-000000000006',
                'ParentShardId': 'shardId-000000000003',
                'AdjacentParentShardId': 'shardId-000000000004',
                'HashKeyRange': {
                    'StartingHashKey': '113427455640312821154458202477256070485',
                    'EndingHashKey': '226854911280625642308916404954512140969'
                },
                'SequenceNumberRange': {
                    'StartingSequenceNumber': '49576779337770055351800808488116479417775295677356572770'
                }
            }
        ],
        'HasMoreShards': False,
        'RetentionPeriodHours': 24,
        'StreamCreationTimestamp': datetime.datetime(2017, 9, 1, 11, 2, 18, tzinfo=tzlocal()),
        'EnhancedMonitoring': [
            {
                'ShardLevelMetrics': [

                ]
            }
        ],
        'EncryptionType': 'NONE'
    },
    'ResponseMetadata': {
        'RequestId': 'eac22a44-902c-5a8b-b8f3-be7546d6952b',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'eac22a44-902c-5a8b-b8f3-be7546d6952b',
            'x-amz-id-2': 'TEl7cPv99Tjx6bZ+DVMBLrU2u0cpu2JapOPQMHfjv5SrT6h1pI40vbenni4Z0wD8bVxaWhcTZ93V/C2G1YLoJeF10a1xNdA+',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '1357',
            'date': 'Tue, 12 Sep 2017 07:51:13 GMT'
        },
        'RetryAttempts': 0
    }
}

# get iterator
iterator_response = {
    'ShardIterator': 'AAAAAAAAAAHnCFuCCzm6dvlOQluArOcchM0F9L7+uSc/8ai8fpklx8yO3kui64Y/EZ/cJ+EHi3lUl5Qxc72ndFs5Pp+KjiNBo3tAY7pRDebgADO4+2XUQPdCC+klpvBRIiF/nFsptbEQ5Q6SVttlTKrm/qfpLIY/x7fvHq+hOz0tktvY9U9NHVVg76qDsLF1VxrO+ax1Ge3sSYSBpDsro+d1D3VY79B9tCAQ82i8iLqBVnGKdLM+PhxGMH41xVxie/Aax93/b6+LgBGez+z77ix31R0dmR5G',
    'ResponseMetadata': {
        'RequestId': 'd4ac486e-e6e5-ab1b-869d-dcd768359cf6',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'd4ac486e-e6e5-ab1b-869d-dcd768359cf6',
            'x-amz-id-2': 'eGujwFf/XeL3EomrjV9s2nseqsWtX0lPzWcsJYNxyorreQMwqukiW7JwZLPHfY0qAgN7EI6/Cjuz6/kjLZuAZvDjINvEjjI3',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '308',
            'date': 'Tue, 12 Sep 2017 07:53:29 GMT'
        },
        'RetryAttempts': 0
    }
}

# getrecords empty response
shard_no_records = {
    'Records': [],
    'NextShardIterator': 'AAAAAAAAAAGnlOdmj9HPfKv2Bvy78jnxcvciT3thg+9TlWZMgz6bsNPpLrhbasZ7qzmftgaTRM+TRS1NXbK+44l0eMPnqW+nJa7Vn03uS2rvtAjyNdICJi6yZPYMCIh4eUdb6vWObdDEWWJgjdfs9+esdyVX4wdVzyGjkLOSieeljai7TQh0MeOm+9keAjMIvJcOuKY2tn08Os5/P9CouUNRuKwUYE5jmY8YjbkGR4fmutUz+gmadgmEaeAr2NUwXdRSFN6DniMWZ+WTAyi/plrnfn7xYv3B',
    'MillisBehindLatest': 0,
    'ResponseMetadata': {
        'RequestId': 'e19c5c32-63bd-0b8b-b3ad-cad8dd418cdf',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'e19c5c32-63bd-0b8b-b3ad-cad8dd418cdf',
            'x-amz-id-2': 'BJVx87gTRSqaZ/j/c4aoen46Py3u1UFTyfX0FS8C0BTQ33unlLF2yB1tfXrBy+mBGk51pZTOoq0ZMBg6dWdqqhemA54G0QMnpnSDmW2ZM3Y=',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '2343',
            'date': 'Tue, 12 Sep 2017 08:02:50 GMT'
        },
        'RetryAttempts': 0
    }
}

# getrecords with records
shard_with_records = {
    'Records': [
        {
            'SequenceNumber': '49576779335963694990727001090816802339424332026684112978',
            'ApproximateArrivalTimestamp': datetime.datetime(2017, 9, 12, 10, 2, 50, 759000, tzinfo=tzlocal()),
            'Data': b'{"resource":"/index.html", "referrer":"http://www.facebook.com"}',
            'PartitionKey': '/index.html'
        },
        {
            'SequenceNumber': '49576779335963694990727001090818011265243946655858819154',
            'ApproximateArrivalTimestamp': datetime.datetime(2017, 9, 12, 10, 2, 50, 761000, tzinfo=tzlocal()),
            'Data': b'{"resource":"/index.html", "referrer":"http://www.google.com"}',
            'PartitionKey': '/index.html'
        }
    ],
    'NextShardIterator': 'AAAAAAAAAAGnlOdmj9HPfKv2Bvy78jnxcvciT3thg+9TlWZMgz6bsNPpLrhbasZ7qzmftgaTRM+TRS1NXbK+44l0eMPnqW+nJa7Vn03uS2rvtAjyNdICJi6yZPYMCIh4eUdb6vWObdDEWWJgjdfs9+esdyVX4wdVzyGjkLOSieeljai7TQh0MeOm+9keAjMIvJcOuKY2tn08Os5/P9CouUNRuKwUYE5jmY8YjbkGR4fmutUz+gmadgmEaeAr2NUwXdRSFN6DniMWZ+WTAyi/plrnfn7xYv3B',
    'MillisBehindLatest': 2000,
    'ResponseMetadata': {
        'RequestId': 'e19c5c32-63bd-0b8b-b3ad-cad8dd418cdf',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'e19c5c32-63bd-0b8b-b3ad-cad8dd418cdf',
            'x-amz-id-2': 'BJVx87gTRSqaZ/j/c4aoen46Py3u1UFTyfX0FS8C0BTQ33unlLF2yB1tfXrBy+mBGk51pZTOoq0ZMBg6dWdqqhemA54G0QMnpnSDmW2ZM3Y=',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '2343',
            'date': 'Tue, 12 Sep 2017 08:02:50 GMT'
        },
        'RetryAttempts': 0
    }
}

# getrecords with null iterator
shard_that_is_closed = {
    'Records': [],
    'NextShardIterator': None,
    'MillisBehindLatest': 0,
    'ResponseMetadata': {
        'RequestId': 'e19c5c32-63bd-0b8b-b3ad-cad8dd418cdf',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'server': 'Apache-Coyote/1.1',
            'x-amzn-requestid': 'e19c5c32-63bd-0b8b-b3ad-cad8dd418cdf',
            'x-amz-id-2': 'BJVx87gTRSqaZ/j/c4aoen46Py3u1UFTyfX0FS8C0BTQ33unlLF2yB1tfXrBy+mBGk51pZTOoq0ZMBg6dWdqqhemA54G0QMnpnSDmW2ZM3Y=',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '2343',
            'date': 'Tue, 12 Sep 2017 08:02:50 GMT'
        },
        'RetryAttempts': 0
    }
}
