from boto3 import client, Session
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--delete_after_retention_days', required=False, default=15)
    parser.add_argument('--number_deployments', required=False)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--prefix', required=False, default="")

    args = parser.parse_args()

    delete_after_retention_days = int(args.delete_after_retention_days)
    number_deployments = args.number_deployments
    bucket = args.bucket
    prefix = args.prefix

    # get current date
    today = datetime.now(timezone.utc)

    try:
        # create a connection to Wasabi
        s3_client = client('s3')
    except Exception as e:
        raise e

    try:
        # list all the buckets under the account
        list_buckets = s3_client.list_buckets()
    except ClientError:
        # invalid access keys
        raise Exception("Invalid Access or Secret key")

    # create a paginator for all objects.
    object_response_paginator = s3_client.get_paginator('list_objects')
    if len(prefix) > 0:
        operation_parameters = {'Bucket': bucket,
                                'Prefix': prefix,}
    else:
        operation_parameters = {'Bucket': bucket}

    # instantiate temp variables.
    delete_list = []
    prefix_list = []

    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%Y%m%d%H%M%S'))

    print("$ Paginating bucket " + bucket)
    if number_deployments:
        operation_parameters['Delimiter'] = '/'
        for object_response_itr in object_response_paginator.paginate(**operation_parameters):
            try:
                for content in object_response_itr['CommonPrefixes']:
                    prefix_list.append({'Prefix': content['Prefix'][:-1]})
            except KeyError:
                print("No directories found in bucket.")

        operation_parameters = {'Bucket': bucket,
                                'Prefix': prefix,}
        for prefix in prefix_list[0: int(number_deployments)]:
            for object_response_itr in object_response_paginator.paginate(**operation_parameters):
                if 'Contents' in object_response_itr:
                    objs = object_response_itr['Contents']
                    files = sorted(objs, key=get_last_modified)
                    for file in files:
                        delete_list.append({'Key': file['Key']}) #Figure out how to solve for Versioning later

    elif delete_after_retention_days:
        for object_response_itr in object_response_paginator.paginate(**operation_parameters):
            for content in object_response_itr['Contents']:
                if (today - content['LastModified']).days > delete_after_retention_days:
                    delete_list.append({'Key': content['Key'], 'VersionId': ''})

    # print objects count
    print("-" * 20)
    print("$ Before deleting objects")
    print("$ Items marked for deletion: " + str(delete_list))
    print("-" * 20)

    # delete objects 1000 at a time
    print("$ Deleting objects from bucket " + bucket)
    for i in range(0, len(delete_list), 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': delete_list[i:i + 1000],
                'Quiet': False
            }
        )

    # reset counts
    delete_list=[]

    # paginate and recount
    print("$ Paginating bucket " + bucket)
    for object_response_itr in object_response_paginator.paginate(Bucket=bucket):
        try:
            for content in object_response_itr['Contents']:
                if (today - content['LastModified']).days < delete_after_retention_days:
                    delete_list.append({'Key': content['Key'], 'VersionId': content['VersionId']})
        except KeyError:
            print("No content found in bucket.")

    # print objects count
    print("-" * 20)
    print("$ After deleting objects")
    print("$ Items Remaining: " + str(delete_list))
    print("-" * 20)
    print("$ task complete")