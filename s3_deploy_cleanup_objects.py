import argparse
import boto3
from botocore.exceptions import ClientError


def clean_objects(s3_resource, number_deployments, bucket_name):

    bucket = s3_resource.Bucket(bucket_name)

    dir_list = []
    dir_map = {}
    del_list = []
    file_list = []

    print("\nYour bucket contains the following objects:")
    try:
        files = bucket.objects.filter(Prefix='')
        files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified, reverse=True)]
        for file in files:
            file_split = file.split('/', 1)
            dir_list.append(file_split) 

        for d in dir_list:
            try:
                f = list(set(dir_map.get(d[0], []) + d[1:]))
                dir_map = {**dir_map, **{d[0]:f}}
            except KeyError:
                dir_map = {**dir_map, **{d[0]:d[1:]}}
        for item in iter(dir_map.items()):
            del_list.append(item)
        del_list = del_list[number_deployments: ]
        if len(del_list) > 0:
            for key, value in del_list:
                for v in value:
                    ls = (key, v)
                    deployment = "/".join(ls)
                    file_list.append(deployment)
                    print(deployment)
        else:
            print(f"No items to meet the criteria.\nExiting.")
            quit()

    except ClientError as err:
        print(f"Couldn't list the objects in bucket {bucket.name}.")
        print(f"\t{err.response['Error']['Code']}:{err.response['Error']['Message']}")


    answer = input(
        "\nDo you want to delete the list of objects in the bucket (y/n)? ")
    if answer.lower() == 'y':
        try:
            for item in file_list:
                obj = s3_resource.Object(bucket_name, item)
                print(f"Deleting {obj.key}.")
                obj.delete()
        except ClientError as err:
            print(f"Couldn't empty the bucket {bucket.name}.")
            print(f"\t{err.response['Error']['Code']}:{err.response['Error']['Message']}")


if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    
    parser.add_argument('--delete_after_retention_days', required=False, default=15)
    parser.add_argument('--number_deployments', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--prefix', required=False, default="")

    args = parser.parse_args()

    s3_resource = boto3.resource('s3')
    delete_after_retention_days = int(args.delete_after_retention_days)
    number_deployments = int(args.number_deployments)
    bucket_name = str(args.bucket)

    # if number_deployments > 1:
    #     number_deployments -= 1
    clean_objects(s3_resource, number_deployments, bucket_name)
