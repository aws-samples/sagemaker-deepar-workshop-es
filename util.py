import pandas as pd
from botocore.exceptions import ClientError
from datetime import date, timedelta


def week_days(start_date, end_date):
    """
    Creates a list of the Mondays through Fridays contained in the range of dates.

    :param start_date: The starting date to evaluate, if it is a working day then is included in the list.
    :param end_date: The end date, it is excluded even if is a working day.
    :return: List of working days.
    """
    days = list()
    eval_date = start_date
    while eval_date < end_date:
        if eval_date.weekday() < 5:
            days.append(eval_date)
        eval_date = eval_date + timedelta(days=1)
    return days


def list_keys(client, bucket, prefix, token=None):
    """
    Recursive function used to retrieve all the object keys that match with a given prefix in the given S3 bucket.
    :param client: Client for the Amazon S3 service.
    :param bucket: The S3 bucket name.
    :param prefix: The prefix used for filtering.
    :param token: The continuation token returned by a previous call.
    :return: The found keys matching the prefix.
    """
    keys = list()
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        ContinuationToken=token
    ) if token else client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    if 'Contents' in response:
        for item in response['Contents']:
            keys.append(item['Key'])
        if 'NextContinuationToken' in response:
            keys += list_keys(client, bucket, prefix, response['NextContinuationToken'])
    return keys


def copy_objects(client, src_bucket, dest_bucket, dest_prefix, dates):
    """
    Copy the XETRA dataset objects from the source bucket to the destination bucket.
    :param client: Client for the Amazon S3 service.
    :param src_bucket: Source bucket containing the XETRA data set.
    :param dest_bucket: Destination object where the data set will be copied.
    :param dest_prefix: The destination prefix used to create the destination object keys.
    :param dates: The list of dates used to copy the objects.
    :return: List of available objects.
    """
    object_keys = list()
    already_copied = list_keys(client, dest_bucket, dest_prefix)
    for weekday in dates:
        try:
            # Catch error if a day does not exist in the source data set
            for hour in range(0, 24):
                src_key = f'{weekday.isoformat()}/{weekday.isoformat()}_BINS_XETR{hour:02d}.csv'
                key = f'{dest_prefix}/{weekday.isoformat()}_BINS_XETR{hour:02d}.csv'
                # Copy only new objects.
                if key not in already_copied:
                    client.copy_object(
                        Bucket=dest_bucket,
                        Key=key,
                        CopySource={
                            'Bucket': src_bucket,
                            'Key': src_key
                        }
                    )
                object_keys.append(key)
        except ClientError as error:
            print(error)
    return object_keys


def create_dataframe(client, bucket, object_keys):
    """
    Loads the list of objects from the S3 bucket into a pandas dataframe. Finally, concatenates all frames.
    :param client: The Amazon S3 client.
    :param bucket: Source bucket where the data will be read.
    :param object_keys: The objects keys to read.
    :return: The concatenated data frames.
    """
    dfs = list()
    for key in object_keys:
        response = client.get_object(
            Bucket=bucket,
            Key=key
        )
        dfs.append(pd.read_csv(response['Body']))
    return pd.concat(dfs, ignore_index=True)
