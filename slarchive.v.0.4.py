#!/usr/bin/env python3

# dhslab archive script

# Components:
# - AWS S3 Glacier bucket
# - RIS Archive tier location
# - Local file system with Globus endpoint
# - JSON file containing a unique id, archive filename, local path, archive path (on Glacier or RIS), list of files in it, and a checksum of the tarball
# - Database, either sqlite or Amazon Lambda, where the JSON file is stored
# - Python script to archive/unarchive files

# Database/JSON file structure:
# {Id: 'string', Filename: 'filename.tar.gz', LocalPath: '/path/to/file', ArchivePath: 's3://bucket/path/to/tarball', Files: ['file1','file2',], TarballMD5sum: 'abcd'}

# Only files that do not start with . will be included in the tarball.

# Steps to archive:
# 1. Check if passed path is valid and is a file or directory
# 2. Get convert file to list of files or get list of files in the directory that do not start with .
# 3. Store list of files in a DataFrame
# 4. Make a unique id for the archive
# 4. Create a tarball of the files
# 5. Get the md5sum of the tarball
# 7. Make a local JSON file with the unique id, tarball filename, local path, archive path, list of files, and md5sum
# 8. Upload the tarball to the S3 Glacier bucket
# 9. Store the JSON file in the database

import os
import sys
import glob
import re
import tarfile, gzip
import hashlib
import time
import pandas as pd
import subprocess
import shutil
import json
import argparse
import random
import string
import threading
import boto3

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

#
# Classes
#

class ProgressPercentage:
    """A callable object that keeps track of upload progress and prints a progress bar."""
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # The callback gets the number of bytes transferred for this chunk.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            # Print progress on a single line with carriage return
            sys.stdout.write(
                f"\rUploading {self._filename}  "
                f"{self._seen_so_far:.0f} / {self._size:.0f} bytes  "
                f"({percentage:.2f}%)"
            )
            sys.stdout.flush()


#
# Helper functions
#

def is_valid_path(path):
    path = os.path.expanduser(path)
    if not os.path.isdir(path) and not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid directory path")
    return path

def is_valid_directory(path):
    path = os.path.expanduser(path)
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid directory path")
    return path

def is_valid_file(path):
    path = os.path.expanduser(path)
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid file path")
    return path

def readable_bytes(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.2f} {unit}"

#
# Main functions
#

# Initialize the configuration file
def init_config_file(config,args):
    """
    Prompts user for five configuration items, suggests defaults from 'conf',
    and writes the result to a JSON file.
    """
    config_path = os.path.expanduser(args.config)

    # Check if file exists and ask user if they want to overwrite it
    if os.path.isfile(config_path):
        overwrite = input(f"Configuration file '{config_path}' already exists. Overwrite? [y/N]: ")
        if overwrite.lower() != 'y':
            print("Exiting without overwriting configuration file.")
            sys.exit(0)

    print("Please enter the following configuration items.\n"
          "Press Enter to accept the shown default in brackets.")

    # Prompt user for each item
    def prompt(key, prompt_text):
        default_value = config[key]
        user_input = input(f"{prompt_text} [{default_value}]: ").strip()
        return user_input if user_input else default_value

    config["ris_archive_path"] = prompt("ris_archive_path", "Location of RIS archive path")
    config["globus_endpoint"]  = prompt("globus_endpoint", "Globus endpoint")
    config["s3_bucket"]        = prompt("s3_bucket", "S3 bucket name")
    config["s3_region"]        = prompt("s3_region", "S3 region")
    config["storage_class"]    = prompt("storage_class", "S3 storage class")
    config["db"]               = prompt("db", "Database file/location")

    # check if the database file exists
    if not os.path.isfile(config["db"]):
        print(f"Database file '{config['db']}' does not exist. Exiting.")
        sys.exit(1)

    # Check if the S3 bucket exists
    if not check_s3_bucket(config["s3_bucket"]):
        print(f"Bucket {config['s3_bucket']} does not exist. Exiting.")
        sys.exit(1)

    # check storage class
    if config["storage_class"] not in ['DEEP_ARCHIVE', 'GLACIER', 'GLACIER_IR', 'STANDARD', 'STANDARD_IA']:
        print(f"Storage class {config['storage_class']} is not valid. Exiting.")
        sys.exit(1)

    # Write the configuration to a JSON file
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"\nConfiguration has been saved to '{os.path.abspath(config_path)}'.")
    print("Final configuration:")
    print(json.dumps(config, indent=2))

    # Set permissions to 600
    os.chmod(config_path, 0o600)

    # Tell user to add AWS credentials to ~/.aws/credentials if using Glacier
    print("\nIf you are using AWS Glacier, make sure you have added your AWS credentials to ~/.aws/credentials.")

def get_files(filepath):
    files = []
    filesizes = []

    if os.path.isfile(filepath):
        # Single file
        files = [filepath]
        filesizes = [os.path.getsize(filepath)]
    elif os.path.isdir(filepath):
        # Directory
        for root, dirs, f in os.walk(filepath):
            for file in f:
                # skip files that match dhslabarchive.\S+.{tar.gz,json}
                if re.match(r'dhslabarchive.\S+.tar.gz', file) or re.match(r'dhslabarchive.\S+.json', file):
                    continue

                fullpath = os.path.join(root, file)
                files.append(fullpath)
                filesizes.append(os.path.getsize(fullpath))
    else:
        print(f"'{filepath}' is not a valid file or directory path")
        sys.exit(1)

    df = pd.DataFrame({'File': files, 'Size': filesizes})
    return df

def create_tarball(files, tarball_path):
    directory = os.path.dirname(tarball_path)
    sys.stderr.write(f'Creating tarball...')
    sys.stderr.flush()
    with tarfile.open(tarball_path, 'w:gz') as tar:
        for file in files:
            tar.add(file, arcname=os.path.relpath(file, directory))

    sys.stderr.write(f' Done.\n')
    sys.stderr.flush()

def calculate_file_md5sum(file_path):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b''):
            md5.update(chunk)
    return md5.hexdigest()

# Test integrity of the tarball by checking that its a valid tarball and extracting each file
def test_tarball_integrity(tarball_path):
    try:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            for member in tar.getmembers():
                tar.extract(member)
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

# function to check if archive path exists
def check_archive_path(path):
    if os.path.isdir(path):
        return True
    return False

def check_s3_bucket(bucket_name):
    # dummy implementation; replace with your actual logic
    return True

def check_s3_object(bucket_name, object_name):
    # dummy implementation; replace with your actual logic
    return False

def transfer_to_s3(bucket, region, tarball_path, overwrite=False, storage_class='STANDARD_IA'):
    # first check if the bucket exists
    if not check_s3_bucket(bucket):
        print(f"Bucket {bucket} does not exist. Exiting.")
        sys.exit(1)
    
    # check if the object exists
    if check_s3_object(bucket, os.path.basename(tarball_path)) and not overwrite:
        print(f"Object {os.path.basename(tarball_path)} already exists in bucket {bucket}. Exiting.")
        return
    
    # Create S3 client
    s3 = boto3.client('s3', region_name=region)
    
    # Configure multi-part uploads
    config = TransferConfig(
        multipart_threshold=25 * 1024 * 1024, 
        max_concurrency=10,
        multipart_chunksize=25 * 1024 * 1024, 
        use_threads=True
    )

    # Create our progress callback
    progress = ProgressPercentage(tarball_path)

    # Start uploading
    try:
        s3.upload_file(
            tarball_path,
            bucket,
            os.path.basename(tarball_path),
            Config=config,
            ExtraArgs={'StorageClass': storage_class},
            Callback=progress  # <--- progress callback
        )
        print("\nUpload complete!")
    except ClientError as e:
        print(f"Error: {e}")
        return False

    return True

# move the tarball to the archive location
def transfer_to_local_archive(tarball_path, archive_path):
    try:
        shutil.copy(tarball_path, archive_path)
    except Exception as e:  
        print(f"Error: {e}")
        return False

# Upload the JSON file to the database
def add_to_database(db, archive_dict):
    # use sqlalchemy to add the json file to the database
    pass



# Add json file to DynamoDB using boto3
def add_to_dynamodb(table_name, record_dict, region_name=None):
    pass

def restore_from_local_archive(archive_path, tarball, restore_path):
    try:
        shutil.copy(os.path.join(archive_path,tarball), restore_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def initiate_deep_archive_restore(bucket_name, object_key, days=7, tier='Bulk', region='us-east-1'):
    """
    Initiates the restore request for an object stored in Glacier Deep Archive.

    :param bucket_name: Name of the S3 bucket
    :param object_key: Key (path) of the object in the S3 bucket
    :param days: Number of days for which the restored object will be accessible
    :param tier: Retrieval tier to use ('Standard' ~12 hours, 'Bulk' ~48 hours)
    :param region: AWS region where the bucket is located
    """
    s3_client = boto3.client('s3', region_name=region)

    try:
        response = s3_client.restore_object(
            Bucket=bucket_name,
            Key=object_key,
            RestoreRequest={
                'Days': days,
                'GlacierJobParameters': {
                    'Tier': tier  # 'Standard' or 'Bulk'
                }
            }
        )
        print(f"Restore request initiated for {bucket_name}/{object_key}.")
        return response
    except ClientError as e:
        # If the restore is already in progress or the object is not in Deep Archive,
        # AWS might return an error. We catch and display it.
        print(f"Error initiating restore: {e}")
        sys.exit(1)

def check_restore_status(bucket_name, object_key, region='us-east-1'):
    """
    Checks whether a Deep Archive object restore is ongoing or complete.

    :param bucket_name: Name of the S3 bucket
    :param object_key: Key (path) of the object in the S3 bucket
    :param region: AWS region where the bucket is located
    :return: A string with the restore status (ongoing-request="true" or "false") or None
    """
    s3_client = boto3.client('s3', region_name=region)

    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    except ClientError as e:
        print(f"Error checking status: {e}")
        sys.exit(1)

    restore_status = response.get('Restore')
    return restore_status

def wait_for_restore(bucket_name, object_key, region='us-east-1', poll_interval=300):
    """
    Polls until the object has been restored or an error occurs.

    :param bucket_name: Name of the S3 bucket
    :param object_key: Key (path) of the object in the S3 bucket
    :param region: AWS region where the bucket is located
    :param poll_interval: Time (seconds) between checks
    """
    print("Waiting for restore to complete...")
    while True:
        status = check_restore_status(bucket_name, object_key, region)
        if status:
            # Example format: 'ongoing-request="false", expiry-date="Fri, 20 Feb 2025 00:00:00 GMT"'
            if 'ongoing-request="false"' in status:
                print(f"Restore complete. Status: {status}")
                break
            else:
                print(f"Restore still in progress. Status: {status}")
        else:
            # If there's no 'Restore' field, it could mean the object isn't in a restorable state
            # or there's some issue with the object metadata.
            print("No restore status found; possibly not in Deep Archive or another issue.")
            sys.exit(1)

        time.sleep(poll_interval)

def download_restored_object(bucket_name, object_key, download_path, region='us-east-1'):
    """
    Downloads the restored Deep Archive object to a local path.

    :param bucket_name: Name of the S3 bucket
    :param object_key: Key (path) of the object in the S3 bucket
    :param download_path: Local file path to which the object should be saved
    :param region: AWS region where the bucket is located
    """
    s3_client = boto3.client('s3', region_name=region)
    
    try:
        s3_client.download_file(bucket_name, object_key, download_path)
        print(f"Object downloaded to {download_path}")
    except ClientError as e:
        print(f"Error downloading object: {e}")
        sys.exit(1)

def download_s3_object(s3_client, bucket, key, local_path):
    """
    Download an object from S3 with a simple progress bar.
    """
    try:
        # 1) Determine the object size by doing a head_object
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']

        # 2) Create the callback
        progress = ProgressPercentage(local_path, file_size)

        # 3) Download the file with the callback
        s3_client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=local_path,
            Callback=progress
        )
        print("\nDownload complete!")

    except ClientError as e:
        print(f"Error: {e}")


def run_archive(config,filepath,archivepath,tarball=False,force=False,overwrite=False,keep=False): 
    # archivepath is either the S3 bucket or the local archive path, tarball is the tarball filename

    ts = time.time()
    
    archive_dict = {
        'Id': '',
        'Timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
        'Location': config['mode'],
        'Filename': '',
        'LocalPath': '',
        'ArchivePath': archivepath,
        'Files': [],
        'TarballMD5sum': ''
    }

    unique_id = ''
    fileDf = pd.DataFrame()
    
    # check if there is a file in the directory that matches dhslabarchive.*.json
    existing_json = glob.glob(os.path.join(filepath,'dhslabarchive.*.json'))
    if existing_json:
        # if force is not set, then exit
        if not force and not overwrite:
            print(f"Archive files already exist in this directory. Exiting.")
            sys.exit(1)
    
        elif overwrite:
            # get the files that match dhslabarchive.\S+.{tar.gz,json} and from the fileDf
            unique_id = os.path.basename(existing_json).split('.')[1]
            tarball = os.path.join(filepath,f'dhslabarchive.{unique_id}.tar.gz')
    
    # generate a unique id that is numbers and letters that is 10 characters long
    if tarball:
        tarball = os.path.abspath(tarball)
        filepath = os.path.dirname(filepath)
        # get the unique id from the tarball filename, which is dhslaarchive.<unique_id>.tar.gz
        unique_id = os.path.basename(tarball).split('.')[1]

        # get the files in the tarball
        with tarfile.open(tarball, 'r:gz') as tar:
            files = [member.name for member in tar.getmembers()]
            # get the list of files and their sizes
            fileDf = pd.DataFrame({'File': files, 'Size': [tar.getmember(file).size for file in files]})

        print(f"Archiving {tarball} to {archivepath}")

    else:
        unique_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        tarball = os.path.join(filepath,f'dhslabarchive.{unique_id}.tar.gz')
        filepath = os.path.abspath(filepath)

        # get the list of files and their sizes
        fileDf = get_files(filepath)

        if fileDf.shape[0] == 0:
            print(f"No files found in {filepath}. Exiting.")
            sys.exit(1)            

        # if sum of filesizes is greater than 2Tb, then exit
        if fileDf['Size'].sum() > 2000000000000:
            print(f"Total size of files is greater than 2Tb. Exiting.")
            sys.exit(1)

        # based on the config and the args, print the filepath, number of files, the total size of the files and where it will be archived
        print(f"Archiving {fileDf.shape[0]} files in {filepath} with a total size of {readable_bytes(fileDf['Size'].sum())} bytes to {archivepath}")

        # create the tarball
        create_tarball(fileDf['File'].tolist(), tarball)

    # get the md5sum of the tarball
    tarball_md5sum = calculate_file_md5sum(tarball)

    # test integrity of the tarball
    if not test_tarball_integrity(tarball):
        print(f"Tarball is not valid. Deleting and exiting.")
        os.remove(tarball)
        sys.exit(1)

    # create a JSON file with the unique id, tarball filename, local path, archive path, list of files, and md5sum
    archive_dict['Id'] = unique_id
    archive_dict['Filename'] = os.path.basename(tarball)
    archive_dict['LocalPath'] = filepath
    archive_dict['ArchivePath'] = archivepath
    archive_dict['Files'] = [ os.path.relpath(file, filepath) for file in fileDf['File'].tolist() ]
    archive_dict['TarballMD5sum'] = tarball_md5sum

    if config['mode'] == 'glacier':
        # upload the tarball to the S3 Glacier bucket
        try:
            transfer_to_s3(archivepath, config['s3_region'], tarball, storage_class=config['storage_class'])
            # delete the tarball
            os.remove(tarball)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif config['mode'] == 'local_archive':
        # move the tarball to the archive location and delete the tarball if successful
        try:
            transfer_to_local_archive(tarball, config['ris_archive_path'])
            # delete the tarball
            os.remove(tarball)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    # if not keep, delete all the files
    if not keep:
        # remove all the files and directories in filepath/*
        for file in fileDf['File'].tolist():
            if os.path.isfile(file):
                os.remove(file)
            
        # walk through the directories in filepath and remove them if they are empty
        for root, dirs, files in os.walk(filepath, topdown=False):
            # remove directories starting with .
            dirs = [d for d in dirs if not d.startswith('.')]
            # now descend into directoies and remove them if they are empty
            for dir in dirs:
                if not os.listdir(os.path.join(root,dir)):
                    # rm dir
                    os.rmdir(os.path.join(root,dir))

    # write the JSON file
    json_file = os.path.join(filepath,f'dhslabarchive.{unique_id}.json')
    with open(json_file, 'w') as f:
        json.dump(archive_dict, f, indent=2)

    if config['database'] == 'sqlite':
        # upload the JSON file to the database
        add_to_database(config['db'], archive_dict)

    if config['database'] == 'dynamodb':
        # add the json file to DynamoDB
        add_to_dynamodb(config['db'], archive_dict)

# Function to unarchive files from S3 Glacier
def run_unarchive(config,filepath):
    # exit if filepath isnt a directory
    if not os.path.isdir(filepath):
        print(f"'{filepath}' is not a valid directory path")
        sys.exit(1)

    # get the list of files that match dhslabarchive.\S+.json
    archive_json = glob.glob(os.path.join(filepath,'dhslabarchive.*.json'))
    # get the most recent json file
    archive_json = max(archive_json, key=os.path.getctime)
    
    #read in the json file
    with open(archive_json, 'r') as f:
        archive_dict = json.load(f)

        if archive_dict['Location'] == 'glacier':
            # get storage class of the object
            s3 = boto3.client('s3', region_name=config['s3_region'])
            response = s3.head_object(Bucket=archive_dict['ArchivePath'], Key=archive_dict['Filename'])
            storage_class = response['StorageClass']

            if storage_class == 'DEEP_ARCHIVE' or storage_class == 'GLACIER':

                # initiate the restore request
                initiate_deep_archive_restore(archive_dict['ArchivePath'], archive_dict['Filename'], region=config['s3_region'])

                # wait for the restore to complete
                wait_for_restore(archive_dict['ArchivePath'], archive_dict['Filename'], region=config['s3_region'])

                # download the restored object
                download_restored_object(archive_dict['ArchivePath'], archive_dict['Filename'], os.path.join(filepath,archive_dict['Filename']), region=config['s3_region'])

            # if is STANDARD_IA, then just download the object
            elif storage_class == 'STANDARD_IA' or storage_class == 'STANDARD' or storage_class == 'GLACIER_IR':

                download_s3_object(archive_dict['ArchivePath'], archive_dict['Filename'], os.path.join(filepath,archive_dict['Filename']))

            else:
                print(f"Storage class {storage_class} is not supported. Exiting.")
                sys.exit(1)

        elif archive_dict['Location'] == 'local_archive':
            # copy the tarball to the local directory
            restore_from_local_archive(archive_dict['ArchivePath'], archive_dict['Filename'], filepath)

        # verify the tarball
        tarball = os.path.join(filepath,archive_dict['Filename'])
        if not test_tarball_integrity(tarball):
            print(f"Restored tarball is not valid. Exiting.")
            sys.exit(1)

        # check tarball md5sum
        tarball_md5sum = calculate_file_md5sum(tarball)
        
        if tarball_md5sum != archive_dict['TarballMD5sum']:
            print(f"Tarball MD5sum does not match. Exiting.")
            sys.exit(1)
        
        # extract the tarball
        with tarfile.open(tarball, 'r:gz') as tar:
            tar.extractall(filepath)

        # delete the tarball
        os.remove(tarball)


def main():

    config = {
        'archive_name': 'dhslabarchive',
        'ris_archive_path': '/Users/dspencer/tmp/archive',
        'globus_endpoint': 'XXXX',
        's3_bucket': 'dhs-lab-archive-1',
        's3_region': 'us-east-1',
        'storage_class': 'STANDARD_IA',
        'db': '/Users/dspencer/tmp/dhslabarchive.db'
    }

    # Main parser
    parser = argparse.ArgumentParser(description='Archive unarchive files either on the RIS system with globus or to S3 Glacier')

    # make a subpaser to set up the confi file
    subparsers = parser.add_subparsers(help='sub-command help',dest='subcommand')
    # config command
    parser_config = subparsers.add_parser('init', help='Initialize the configuration file')
    # config file location, defalt is ~/.slarchive
    parser_config.add_argument('-c', '--config', type=str, default='~/.slarchive', help='Configuration file location')

    # Archive command
    parser_archive = subparsers.add_parser('archive', help='Archive files')

    # Archive command
    parser_restore = subparsers.add_parser('restore', help='Restore files')

    # Archive options

    parser_archive.add_argument('-c', '--config', type=is_valid_file, default='~/.slarchive', help='Configuration file location')
    # supplied path is a tarball
    parser_archive.add_argument('-t', '--tarball', action='store_true', default=False, help='Provided file path is a tarball to archive')
    # update the current archive with new files (overwrite)
    parser_archive.add_argument('-u', '--update', action='store_true', help='Overwrite Archive tier directory with new files')
    # do not delete the files after archiving or the archive after restoring
    parser_archive.add_argument('-k', '--keep', action='store_true', default=False, help='Keep active files after archiving (default: delete)')
    # force archiving if there is already a json or archive
    parser_archive.add_argument('-f', '--force', action='store_true', help='Force archiving even if there is already an archive')
    # overwrite the archive if it already exists
    parser_archive.add_argument('-o', '--overwrite', action='store_true', help='Overwrite archive if it already exists')

    # Specify glacier or local archive
    # Create a mutually exclusive group for glacier vs local
    destination_group = parser_archive.add_mutually_exclusive_group(required=True)
    destination_group.add_argument('-g', '--glacier', action='store_true', help='Archive to S3 Glacier')
    destination_group.add_argument('-l', '--local-archive', action='store_true', help='Archive to local RIS archive')

    parser_archive.add_argument('-d', '--database-file', type=is_valid_file, default=config['db'], help='Database file to use')

    # RIS archive location
    parser_archive.add_argument('-a', '--archive-path', type=str, default=config['ris_archive_path'], help='Path to archive location')
    # Globus endpoint
    parser_archive.add_argument('-e', '--endpoint', type=str, default=config['globus_endpoint'], help='Globus endpoint to use')

    # S3 Glacier options
    parser_archive.add_argument('-r','--region', type=str, default=config['s3_region'], help='AWS region to use')
    parser_archive.add_argument('-b','--bucket', type=str, default=config['s3_bucket'], help='AWS bucket')
    parser_archive.add_argument('-s','--storage-class', type=str, default='STANDARD_IA', choices=['DEEP_ARCHIVE', 'GLACIER', 'GLACIER_IR', 'STANDARD', 'STANDARD_IA'], help='S3 storage class')

    # the file/path to archive/unarchive
    parser_archive.add_argument('filepath', type=is_valid_path, help='Path or file to archive')

    parser_restore.add_argument('-c', '--config', type=is_valid_file, default='~/.slarchive', help='Configuration file location')
    parser_restore.add_argument('-d', '--delete', action='store_true', help='Delete the archive tarball after restoring')
    parser_restore.add_argument('filepath', type=is_valid_directory, help='Path to restore files to')

    args = parser.parse_args()

    # if the config command is called, then run the config function
    if args.subcommand == 'init':
        init_config_file(config,args)

    elif args.subcommand == 'archive' and args.filepath and (args.glacier or args.local_archive):

        # check config file and load it, otherwise if it doesnt exist, then exit and say run init first
        if not os.path.isfile(args.config):
            print('Configuration file does not exist. Run "slarchive init" first')
            sys.exit(1)
        else:
            with open(args.config, 'r') as f:
                config = json.load(f)

        # replace config values with command line values, if passed
        if args.archive_path:
            config['ris_archive_path'] = args.archive_path
        if args.endpoint:
            config['globus_endpoint'] = args.endpoint
        if args.region:
            config['s3_region'] = args.region
        if args.bucket:
            config['s3_bucket'] = args.bucket
        if args.database_file:
            config['db'] = args.database_file
        if args.storage_class:
            config['storage_class'] = args.storage_class

        archive_path = ''
        if args.glacier:
            archive_path = config['s3_bucket']
            config['mode'] = 'glacier'

        if args.local_archive:
            archive_path = config['ris_archive_path']
            config['mode'] = 'local_archive'

        # expand/normalize args.filepath and get its absolute path
        filepath = os.path.expanduser(args.filepath)  
        filepath = os.path.abspath(filepath)            

        # set database mode
        if os.path.isfile(config['db']):
            config['database'] = 'sqlite'
        elif config['db'].startswith('arn:aws'):
            config['database'] = 'dynamodb'

        run_archive(config,filepath,archive_path,tarball=args.tarball,keep=args.keep,force=args.force,overwrite=args.overwrite)

    elif args.subcommand == 'restore' and args.filepath:
            
            # check config file and load it, otherwise if it doesnt exist, then exit and say run init first
            if not os.path.isfile(args.config):
                print('Configuration file does not exist. Run "slarchive init" first')
                sys.exit(1)
            else:
                with open(args.config, 'r') as f:
                    config = json.load(f)
    
            # expand/normalize args.filepath and get its absolute path
            filepath = os.path.expanduser(args.filepath)  
            filepath = os.path.abspath(filepath)            
    
            run_unarchive(config,filepath)

    else:
        parser_archive.print_help()


if __name__ == "__main__":
    main()
