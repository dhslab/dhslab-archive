#!/usr/bin/env python3

# dhslab archive script

# Components:
# - Python script to archive/unarchive files and search for files in the archive
# - configuration file to set up the archive location, database, and other settings
# - AWS S3 bucket with Glacier storage class (or other storage class)
# - RIS Archive tier location
# - Globus with RIS endpoint for transfer to RIS Archive (we cant unarchive ourselves at this point)
# - JSON file located in the archived path containing a unique id, archive filename, local path, archive path (on Glacier or RIS), list of files in it, a checksum of the tarball, and username of the person who archived it
# - Database, either sqlite or Amazon Lambda, where the JSON file info is stored for searching

# Database/JSON file structure:
# {Id: 'string', Filename: 'filename.tar.gz', LocalPath: '/path/to/file', ArchivePath: '{s3_bucket} or /path/to/archive', Files: ['file1','file2',], TarballMD5sum: 'abcd', Username: 'username'}

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
import fnmatch
import getpass
import re
import shlex
import tarfile, gzip
import hashlib
import mmap
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
from tqdm import tqdm

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, inspect
from sqlalchemy.exc import SQLAlchemyError

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

#
# Classes
#

class TqdmUploadProgress:
    def __init__(self, filename, desc=None):
        self._filename = filename
        self._size = os.path.getsize(filename)
        if desc is None:
            desc = f"Uploading {os.path.basename(filename)}"
        self._tqdm = tqdm(total=self._size, unit='B', unit_scale=True, desc=desc)

    def __call__(self, bytes_amount):
        self._tqdm.update(bytes_amount)


#
# Helper functions
#

def is_valid_path(path):
    path = os.path.expanduser(path)
    if not os.path.isdir(path) and not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid file or directory path")
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

    config["globus_endpoint"]  = prompt("globus_endpoint", "Globus endpoint")
    config["ris_archive_path"] = prompt("ris_archive_path", "Location of RIS archive path")
    config["s3_bucket"]        = prompt("s3_bucket", "S3 bucket name")
    config["s3_region"]        = prompt("s3_region", "S3 region")
    config["storage_class"]    = prompt("storage_class", "S3 storage class")
    config["db"]               = prompt("db", "Database file/location")

    # add username (not root) to the config
    config["username"] = getpass.getuser()

    # check if the database file exists
    if not os.path.isfile(config["db"]):
        # ask user if they want to create the database
        create_db = input(f"Database file '{config['db']}' does not exist. Create it? [y/N]: ")
        if create_db.lower() == 'y':
            create_archive_db(config["db"], config["archive_name"])
        else:
            print("Exiting.")
            sys.exit(0)

    # Check if the S3 bucket exists
    if not check_s3_bucket(config["s3_bucket"]):
        # ask user if they want to create the bucket
        create_bucket = input(f"Bucket {config['s3_bucket']} does not exist. Create it? [y/N]: ")
        if create_bucket.lower() == 'y':
            create_s3_bucket(config["s3_bucket"], config["s3_region"])
        else:
            print("Exiting.")
            sys.exit(0)

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

#
# Archive functions
#

def calculate_file_md5sum(file_path):
    # if its an empty file, then return False
    if os.path.getsize(file_path) == 0:
        return False

    with open(file_path, 'rb') as f:
        # Memory-map the file, size 0 means whole file
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            return hashlib.md5(mm).hexdigest()

# normalize a pattern argument that may be None, a single glob string, or a list of globs
def _as_pattern_list(pattern):
    if pattern is None:
        return None
    if isinstance(pattern, str):
        return [pattern]
    patterns = list(pattern)
    return patterns if patterns else None

# get the list of files in the directory
def get_files(filepath, pattern=None):
    """
    Collects files under `filepath` (a single file or a directory, walked recursively).
    Always excludes hidden archive bookkeeping files (dhslabarchive.*.tar.gz / .json).
    If `pattern` is given, it's a shell-style glob (e.g. '*.fastq.gz'), or a list of
    globs, matched against each file's basename via fnmatch - a file is included if it
    matches ANY of the given patterns (OR logic).
    """
    patterns = _as_pattern_list(pattern)

    files = []
    filesizes = []
    fileMd5sums = []
    skipped_empty = 0
    skipped_pattern = 0

    if os.path.isfile(filepath):
        # Single file
        if patterns and not any(fnmatch.fnmatch(os.path.basename(filepath), p) for p in patterns):
            print(f"'{filepath}' does not match any of the given pattern(s): {', '.join(patterns)}")
        else:
            files = [filepath]
            filesizes = [os.path.getsize(filepath)]
            fileMd5sums = [calculate_file_md5sum(filepath)]

    elif os.path.isdir(filepath):
        # Directory

        # First, gather all files (excluding archive bookkeeping files, and anything
        # that doesn't match at least one of the glob patterns, if any were given)
        all_files = []
        for root, dirs, f in os.walk(filepath):
            for file in f:
                if re.match(r'dhslabarchive.\S+.tar.gz', file) or re.match(r'dhslabarchive.\S+.json', file):
                    continue
                if patterns and not any(fnmatch.fnmatch(file, p) for p in patterns):
                    skipped_pattern += 1
                    continue
                fullpath = os.path.join(root, file)
                all_files.append(fullpath)

        # Now, process each file with a progress bar
        for fullpath in tqdm(all_files, desc="Preparing files", unit="file"):

            # Skip empty files because mmap will fail otherwise, but still note them
            # so the user knows they were left out of the archive.
            filesize = os.path.getsize(fullpath)
            if filesize == 0:
                skipped_empty += 1
                continue

            files.append(os.path.relpath(fullpath, filepath))
            filesizes.append(filesize)
            fileMd5sums.append(calculate_file_md5sum(fullpath))

    else:
        print(f"'{filepath}' is not a valid file or directory path")
        sys.exit(1)

    if skipped_pattern:
        print(f"Note: {skipped_pattern} file(s) were excluded because they did not match any of the "
              f"given pattern(s): {', '.join(patterns)}")

    if skipped_empty:
        print(f"Warning: {skipped_empty} empty (0-byte) file(s) were found and will NOT be included in the archive.")

    df = pd.DataFrame({'file': files, 'size': filesizes, 'md5sum': fileMd5sums})
    return df

def chunk_files_by_size(file_df, chunk_size_bytes):
    """
    Greedily splits a DataFrame of files (columns: file, size, md5sum) into a list of
    DataFrames such that the total size of files in each chunk does not exceed
    chunk_size_bytes. A single file that is itself larger than chunk_size_bytes is
    placed alone in its own chunk (it can't be split further). Order of files within
    the original DataFrame is preserved across chunks.
    """
    chunks = []
    current_rows = []
    current_size = 0

    for _, row in file_df.iterrows():
        size = row['size']

        # start a new chunk if this file would push the current one over the limit
        # (but never start a new chunk before we've placed at least one file in it)
        if current_rows and current_size + size > chunk_size_bytes:
            chunks.append(pd.DataFrame(current_rows).reset_index(drop=True))
            current_rows = []
            current_size = 0

        current_rows.append(row)
        current_size += size

    if current_rows:
        chunks.append(pd.DataFrame(current_rows).reset_index(drop=True))

    return chunks


def calculate_obj_md5sum(file_obj, chunk_size=256 * 1024):  # 256KB chunks
    md5 = hashlib.md5()
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        md5.update(chunk)
    return md5.hexdigest()

def get_tarball_md5sums(tarball_name):
    tarball_md5sums = []
    with tarfile.open(tarball_name, 'r:gz') as tar:
        for member in tar.getmembers():
            if member.isfile():
                file_obj = tar.extractfile(member)
                md5sum = calculate_obj_md5sum(file_obj)
                # get size of file in bytes
                size = member.size
                tarball_md5sums.append((member.name, size, md5sum))

    return pd.DataFrame(tarball_md5sums, columns=['file', 'size', 'md5sum'])

# create a tarball of the files
def create_tarball(files, tarball_path):
    directory = os.path.dirname(tarball_path)
    total_files = len(files)

    with tarfile.open(tarball_path, 'w:gz') as tar:
         for file_path in tqdm(files, desc="Creating tarball", unit="file"):
            tar.add(os.path.join(directory, file_path), arcname=file_path)

# Test integrity of the tarball by checking that its a valid tarball and extracting each file
def test_tarball_integrity(tarball_path, md5sums):
    # md5sums must be a list of strings
    if not isinstance(md5sums, list):
        return False

    # get tarball md5sums
    tarball_md5sums = get_tarball_md5sums(tarball_path)
    # convert md5sums to a set and do the same for the files and if they all match then
    # the tarball is valid and return true. If not, return false
    if set(tarball_md5sums['md5sum'].tolist()) == set(md5sums):
        return True

    return False

# function to list members of a tarball and return as a list
def list_tarball_files(tarball_path):
    try:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            return tar.getnames()
    except Exception as e:
        print(f"Error: {e}")
        return []

# function to check if archive path exists
def check_archive_path(path):
    if os.path.isdir(path):
        return True
    return False

# Verify that a pre-existing tarball (used with -t/--tarball) is a complete, accurate
# archive of the files currently sitting in its parent directory. This is separate from
# test_tarball_integrity(), which only checks the tarball against itself (i.e. that it
# isn't corrupted) - it never compares to what's actually on disk.
def verify_tarball_matches_directory(directory, tarball_df, pattern=None):
    """
    Checks that every non-hidden, non-archive file in `directory` is present in the
    tarball's file list (`tarball_df`, as produced by get_tarball_md5sums) and that
    its checksum matches the checksum recorded in the tarball.

    If `pattern` is given (a single glob or a list of globs), only files matching at
    least one of them are required to be present in the tarball - other files in
    `directory` are ignored for this check, since they were never intended to be archived.

    Returns True if every expected file on disk is present in the tarball with a
    matching checksum. Returns False (and prints details) if anything is missing or
    mismatched. Files present in the tarball but no longer on disk are reported as a
    note, not treated as an error, since that can legitimately happen (e.g. files
    were cleaned up after the tarball was made).
    """
    dir_df = get_files(directory, pattern=pattern)

    dir_files = dict(zip(dir_df['file'], dir_df['md5sum']))
    tar_files = dict(zip(tarball_df['file'], tarball_df['md5sum']))

    missing_from_tarball = sorted(set(dir_files) - set(tar_files))
    extra_in_tarball = sorted(set(tar_files) - set(dir_files))
    mismatched = sorted(
        f for f in (set(dir_files) & set(tar_files))
        if dir_files[f] != tar_files[f]
    )

    ok = True

    if missing_from_tarball:
        ok = False
        print(f"Error: {len(missing_from_tarball)} file(s) in '{directory}' are NOT in the tarball:")
        for f in missing_from_tarball:
            print(f"  {f}")

    if mismatched:
        ok = False
        print(f"Error: {len(mismatched)} file(s) differ between the tarball and the copy on disk (checksum mismatch):")
        for f in mismatched:
            print(f"  {f}")

    if extra_in_tarball:
        print(f"Note: {len(extra_in_tarball)} file(s) are in the tarball but not currently present in '{directory}':")
        for f in extra_in_tarball:
            print(f"  {f}")

    return ok

#
# S3 functions
#

# create an S3 bucket
def create_s3_bucket(bucket_name, region='us-east-1'):
    s3_client = boto3.client('s3', region_name=region)
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f"Error: {e}")
        sys.exit(1)

    return True

# check if the bucket exists
def check_s3_bucket(bucket_name):
    # check that the bucket exists
    s3_client = boto3.client('s3')
    # get bucket
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            print(f"Error: {e}")
            sys.exit(1)

    return True

# check if the object exists in the bucket
def check_s3_object(bucket_name, object_name):
    # check that the object exists
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            print(f"Error: {e}")
            sys.exit(1)

    return True

def check_storage_class(bucket_name, object_name, region='us-east-1'):
    # get storage class of the object
    s3 = boto3.client('s3', region_name=region)
    try:
        response = s3.head_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        print(f"Error: {e}")
        sys.exit(1)

    storage_class = response.get('StorageClass', 'STANDARD')

    return storage_class

# upload file to the S3 bucket
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

    # print message with storage class
    print(f"Uploading {os.path.basename(tarball_path)} to {bucket} with storage class {storage_class}...")

    # Create our progress callback
    progress = TqdmUploadProgress(tarball_path)

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

#
# Globus functions
#

# Globus login check
def globus_login_active(endpoint_id, directory_path):
    # check that the user is logged in
    command = "globus whoami -F json"
    try:
        subprocess.check_output(command, shell=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # print(f"Error: Not logged into to Globus.")
        return False

    # check that there is an active session
    session_command = "globus session show -F json"
    try:
        session_output = subprocess.check_output(session_command, shell=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # print(f"Error: No active Globus session.")
        return False

    try:
        session_data = json.loads(session_output)
    except json.JSONDecodeError:
        # print(f"Error: Could not parse Globus session output.")
        return False

    # 'identities' is empty if there is no active session
    if not session_data.get('session_id'):
        # print(f"Error: No active Globus session identities found.")
        return False

    return True

# Create a directory on the Globus endpoint
def globus_create_directory(endpoint_id, directory_path):
    command = f"globus mkdir {shlex.quote(endpoint_id)}:{shlex.quote(directory_path)}"
    try:
        subprocess.check_output(command, shell=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return False
    return True

# Check if a directory exists on the Globus endpoint
def globus_path_exists(endpoint_id, directory_path):
    command = f"globus ls {shlex.quote(endpoint_id)}:{shlex.quote(directory_path)}"
    try:
        subprocess.check_output(command, shell=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return False
    return True

# Check if a file exists on the Globus endpoint
def globus_file_exists(endpoint_id,destination_path,file_name):
    command = f"globus ls {shlex.quote(endpoint_id)}:{shlex.quote(destination_path)}/{shlex.quote(file_name)}"
    try:
        output = subprocess.check_output(command, shell=True).decode('utf-8')
    except subprocess.CalledProcessError:
        return False

    return file_name in output

# Initiate a transfer on the Globus endpoint
def initiate_globus_transfer(source_path, destination_path, overwrite=False):
    doDelete = ''
    if overwrite is True:
        doDelete = '--delete'

    command = (f"globus transfer -F json --notify failed -s checksum --preserve-timestamp "
               f"--verify-checksum -v {doDelete} {shlex.quote(source_path)} {shlex.quote(destination_path)}")
    try:
        transfer_result = json.loads(subprocess.check_output(command, shell=True, text=True, universal_newlines=True))
    except subprocess.CalledProcessError:
        sys.exit(f"\nError: could not initate transfer: {source_path} {destination_path}")

    return transfer_result['task_id']

# Remove a file on the Globus endpoint
def globus_remove_file(path):

    command = f"globus rm -F json --notify failed {shlex.quote(path)}"
    try:
        rm_result = json.loads(subprocess.check_output(command, shell=True, text=True, universal_newlines=True))
    except subprocess.CalledProcessError:
        sys.exit(f"\nError: could not rm: {path}")

    return rm_result['task_id']

# Globus transfer function
def globus_transfer_to_archive(file_path,endpoint_id,destination_path):

    sys.stderr.write(f'Checking destination directory...\n')

    # Step 5: Check if the destination directory is present and create if not
    if not globus_path_exists(endpoint_id, destination_path):
        globus_create_directory(endpoint_id, destination_path)

    # Step 6: Initiate transfer and get the task id
    task_id = initiate_globus_transfer(f"{endpoint_id}:{file_path}", f"{endpoint_id}:{destination_path}/{os.path.basename(file_path)}")

    sys.stderr.write(f'Transferring via globus (task id: {task_id})...\n')
    sys.stderr.flush()

    # Step 7: Wait for the transfer to complete
    waiting = subprocess.check_output(f"globus task wait {shlex.quote(task_id)}", shell=True, text=True, universal_newlines=True)

    result = json.loads(subprocess.check_output(f'globus task show -F json {shlex.quote(task_id)}',shell=True, text=True, universal_newlines=True))

    # Step 8: Check if the tarball is present at the destination endpoint
    if result['status'] != 'SUCCEEDED':
        print(f"\nError: Transfer unsuccessful. Check task: {task_id}\nsource: {os.path.dirname(file_path)}\ndestination: {endpoint_id}:{destination_path}.")
        sys.exit(1)

    sys.stderr.write(f'Done.\nTransfer successful.\n')

#
# Database functions
#

def create_archive_db(db_path, db_table):
    """
    Creates a SQLite database file with a table called 'archive_table' having fields that match the archive_dict:

        archive_dict = {
            'Id': '',
            'Timestamp': <timestamp string>,
            'Location': <string>,
            'Filename': <string>,
            'LocalPath': <string>,
            'ArchivePath': <string>,
            'Files': <string>,         # a single file element (exploded from a list, one row per file)
            'Sizes': <integer>,        # a single file element (exploded from a list, one row per file)
            'MD5Sums': <string>,       # a single file element (exploded from a list, one row per file)
            'TarballMD5sum': <string>,
            'Username': <string>
        }

    The 'record' field is an auto-incrementing primary key.

    Parameters:
        db_path (str): Path to the SQLite database file (e.g., 'mydatabase.db').
    """
    # Create the engine using the SQLite database URL
    engine = create_engine(f"sqlite:///{db_path}")

    # Create a MetaData instance to hold table definitions
    metadata = MetaData()

    # Define the archive_table with a single file element per row (matches archive_dict's
    # plural field names, since rows are exploded one-file-per-row in add_to_database)
    archive_table = Table(f"{db_table}", metadata,
                          Column('record', Integer, primary_key=True, autoincrement=True),
                          Column('Id', String),
                          Column('Timestamp', String),   # store timestamp as a string
                          Column('Location', String),
                          Column('Filename', String),
                          Column('LocalPath', String),
                          Column('ArchivePath', String),
                          Column('Files', String),
                          Column('Sizes', Integer),
                          Column('MD5Sums', String),
                          Column('TarballMD5sum', String),
                          Column('Username', String)
                         )

    # Create the table(s) in the database (if they don't already exist)
    metadata.create_all(engine)
    print(f"Database created with table {db_table} in {db_path}")

# function to dump the database to a JSON file
def dump_database_to_json(db_file, table_name):
    """
    Dumps the contents of a database table to stdout as tab-separated values.

    Parameters:
        db_file (str): Path to the SQLite database file (e.g., 'mydatabase.db').
        table_name (str): Name of the table to dump.
    """
    # Create an engine and reflect the table
    engine = create_engine(f"sqlite:///{db_file}")
    metadata = MetaData()
    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except Exception as e:
        print(f"Error reflecting table '{table_name}': {e}")
        return False

    # Use a transaction to read the data
    try:
        with engine.begin() as connection:
            # Select all rows from the table
            query = table.select()
            rows = connection.execute(query).fetchall()
    except SQLAlchemyError as e:
        print(f"Error reading data from '{table_name}': {e}")
        return False

    # Convert the rows to a list of dictionaries
    data = [dict(row._mapping) for row in rows]
    df = pd.DataFrame(data)
    df.to_csv(sys.stdout, sep='\t', index=False)

# Upload the JSON file to the database
def add_to_database(db_file, table_name, data):
    """
    Inserts a dictionary as new row(s) into the specified table using SQLAlchemy.
    Each file in data['Files'] becomes its own row (exploded).

    Parameters:
        db_file (str): Path to the SQLite database file.
        table_name (str): Name of the table to insert data into.
        data (dict): Dictionary with keys corresponding to column names.

    Returns:
        True if insertion is successful, False otherwise.
    """

    # convert dict to pandas dataframe
    df = pd.DataFrame([data], index=[0])
    # explode the list of files into separate rows
    df = df.explode(['Files','Sizes','MD5Sums'])

    # Create an engine and reflect the table
    engine = create_engine(f"sqlite:///{db_file}")
    metadata = MetaData()

    # Use SQLAlchemy's inspector to check if the table exists.
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        print(f"Table '{table_name}' does not exist in the database.")
        return False

    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except Exception as e:
        print(f"Error reflecting table '{table_name}': {e}")
        return False

    # Use a transaction to insert the data
    try:
        # Using engine.begin() automatically handles commit/rollback.
        with engine.begin() as connection:
            # For a single row insert, pass a list with one dictionary.
            # For multiple rows, pass a list of dictionaries.
            connection.execute(table.insert(), df.to_dict(orient='records'))
        return True
    except SQLAlchemyError as e:
        print(f"Error inserting data into '{table_name}': {e}")
        return False

def search_database(db_file, table_name, search_string):
    # NOTE: not yet implemented.
    print("Database search is not yet implemented.")

# Add json file to DynamoDB using boto3
def add_to_dynamodb(table_name, record_dict, region_name=None):
    # NOTE: not yet implemented.
    print("DynamoDB support is not yet implemented.")


#
# Restore functions
#

# restore the tarball from the local archive
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

def wait_for_restore(bucket_name, object_key, region='us-east-1', poll_interval=120):
    """
    Polls until the object has been restored or an error occurs.

    :param bucket_name: Name of the S3 bucket
    :param object_key: Key (path) of the object in the S3 bucket
    :param region: AWS region where the bucket is located
    :param poll_interval: Time (seconds) between checks
    """
    print("Waiting for restore to complete...")
    # flush stdout buffer to ensure the message is displayed
    sys.stdout.flush()

    while True:
        status = check_restore_status(bucket_name, object_key, region)
        if status:
            # Example format: 'ongoing-request="false", expiry-date="Fri, 20 Feb 2025 00:00:00 GMT"'
            if 'ongoing-request="false"' in status:
                print(f"Restore complete. Status: {status}")
                break
            else:
                print(f"Restore still in progress. Status: {status}")
                sys.stdout.flush()

        else:
            # If there's no 'Restore' field, it could mean the object isn't in a restorable state
            # or there's some issue with the object metadata.
            print("No restore status found; possibly not in Deep Archive or another issue.")
            sys.exit(1)

        time.sleep(poll_interval)

def download_s3_object(bucket_name, object_key, download_path, region='us-east-1'):
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

def _build_and_ship_chunk(config, filepath, archivepath, chunk_df, unique_id,
                           chunk_index, chunk_count, overwrite, keep):
    """
    Creates a tarball for a single chunk of files, verifies it, uploads/transfers it
    per config['mode'], writes its JSON record, and returns the archive_dict.
    Does NOT delete the original source files - that's handled once, after all chunks
    for a given filepath have succeeded (see run_archive).
    """
    tarball_path = os.path.join(filepath, f'dhslabarchive.{unique_id}.tar.gz')

    chunk_label = f" (chunk {chunk_index}/{chunk_count})" if chunk_count > 1 else ""
    print(f"Archiving {chunk_df.shape[0]} files in {filepath}{chunk_label} "
          f"with a total size of {readable_bytes(chunk_df['size'].sum())} to {archivepath}")

    create_tarball(chunk_df['file'].tolist(), tarball_path)

    tarball_md5sum = calculate_file_md5sum(tarball_path)

    if not test_tarball_integrity(tarball_path, chunk_df['md5sum'].tolist()):
        print(f"Tarball is not valid. Deleting and exiting.")
        if not keep:
            os.remove(tarball_path)
        sys.exit(1)

    archive_dict = {
        'Id': unique_id,
        'Timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
        'Location': config['mode'],
        'Filename': os.path.basename(tarball_path),
        'LocalPath': filepath,
        'ArchivePath': archivepath,
        'Files': chunk_df['file'].tolist(),
        'Sizes': chunk_df['size'].tolist(),
        'MD5Sums': chunk_df['md5sum'].tolist(),
        'TarballMD5sum': tarball_md5sum,
        'Username': config['username'],
        'ChunkIndex': chunk_index,
        'ChunkCount': chunk_count,
    }

    if config['mode'] == 'dry-run':
        print(f"Archive dry-run complete for {os.path.basename(tarball_path)}.")
        return archive_dict

    if config['mode'] == 'glacier':
        # upload the tarball to the S3 Glacier bucket
        try:
            transfer_to_s3(archivepath, config['s3_region'], tarball_path, overwrite=overwrite, storage_class=config['storage_class'])
            os.remove(tarball_path)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif config['mode'] == 'ris_archive':
        # move the tarball to the archive location and delete the tarball if successful
        try:
            globus_transfer_to_archive(tarball_path, config['globus_endpoint'], config['ris_archive_path'])
            os.remove(tarball_path)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    # write the JSON record for this chunk
    json_file = os.path.join(filepath, f'dhslabarchive.{unique_id}.json')
    with open(json_file, 'w') as f:
        json.dump(archive_dict, f, indent=2)

    return archive_dict


def run_archive(config,filepath,archivepath,tarball=False,force=False,overwrite=False,keep=False,
                 pattern=None,chunk_size_bytes=None):
    # filepath: path passed on the command line (a directory of files, OR a tarball if is_tarball_input)
    # archivepath: either the S3 bucket name or the local RIS archive path
    # tarball: bool -- True if the supplied filepath is ALREADY a tarball to be archived as-is (from --tarball)
    # pattern: optional glob (e.g. '*.fastq.gz') or list of globs to filter which files
    #   get archived - a file is included if it matches ANY of the given patterns
    # chunk_size_bytes: when building a tarball from source files, split them into
    #   separate tarballs/JSON records of at most this many bytes each. Not applied to
    #   a pre-existing tarball supplied via -t/--tarball (see note below).
    #
    # Returns a list of archive_dict records - one per tarball/JSON pair created
    # (a single-element list unless chunking produced more than one).

    is_tarball_input = tarball
    archive_dicts = []

    unique_id = ''
    reuse_id = None

    # check if there is a file in the directory that matches dhslabarchive.*.json
    existing_json = glob.glob(os.path.join(filepath,'dhslabarchive.*.json'))
    if existing_json:
        # if force/overwrite is not set, then exit rather than clobber an existing archive
        if not force and not overwrite:
            print(f"Archive files already exist in this directory. Exiting.")
            sys.exit(1)

        elif overwrite:
            # reuse the unique id from the most recent existing archive record, so the
            # regenerated tarball/json overwrite the same archive id rather than creating
            # a new one. Only meaningful when the new archive is also a single chunk -
            # if chunking produces multiple tarballs, fresh ids are used for all of them
            # (there's no clean way to map N new chunks onto 1 previous archive id).
            existing_json_file = max(existing_json, key=os.path.getctime)
            reuse_id = os.path.basename(existing_json_file).split('.')[1]

    if is_tarball_input:
        tarball_path = os.path.abspath(filepath)
        filepath = os.path.dirname(filepath)
        # get the unique id from the tarball filename, which is dhslabarchive.<unique_id>.tar.gz
        unique_id = os.path.basename(tarball_path).split('.')[1]

        # get the files, sizes, and md5sums in the tarball
        fileDf = get_tarball_md5sums(tarball_path)

        # confirm the tarball actually contains (and matches checksums for) every
        # (matching) file currently in its parent directory, before we archive/delete anything
        print(f"Verifying tarball contents against files in {filepath}...")
        if not verify_tarball_matches_directory(filepath, fileDf, pattern=pattern):
            print("Tarball does not fully match the files in its parent directory. Exiting without archiving.")
            sys.exit(1)

        if chunk_size_bytes and fileDf['size'].sum() > chunk_size_bytes:
            print(f"Note: this tarball is larger than the configured chunk size "
                  f"({readable_bytes(chunk_size_bytes)}), but a pre-existing tarball supplied "
                  f"with -t/--tarball is archived as a single unit and is not split.")

        print(f"Archiving {tarball_path} to {archivepath}")

        archive_dict = _build_and_ship_chunk(config, filepath, archivepath, fileDf,
                                              unique_id, 1, 1, overwrite, keep)
        archive_dicts.append(archive_dict)

    else:
        filepath = os.path.abspath(filepath)

        # get the list of files and their sizes, filtered by the glob pattern(s) if given
        fileDf = get_files(filepath, pattern=pattern)

        if fileDf.shape[0] == 0:
            msg = f"No files found in {filepath}"
            patterns = _as_pattern_list(pattern)
            if patterns:
                msg += f" matching any of the given pattern(s): {', '.join(patterns)}"
            print(msg + ". Exiting.")
            sys.exit(1)

        total_size = fileDf['size'].sum()

        # split into chunks if a chunk size was given; otherwise treat everything as one chunk
        if chunk_size_bytes:
            chunks = chunk_files_by_size(fileDf, chunk_size_bytes)
        else:
            chunks = [fileDf]

        n_chunks = len(chunks)

        if n_chunks > 1:
            print(f"Splitting {fileDf.shape[0]} files ({readable_bytes(total_size)}) into "
                  f"{n_chunks} chunk(s) of up to {readable_bytes(chunk_size_bytes)} each.")
        else:
            print(f"Archiving {fileDf.shape[0]} files in {filepath} with a total size of "
                  f"{readable_bytes(total_size)} to {archivepath}")

        for i, chunk_df in enumerate(chunks, start=1):
            # reuse the previous archive's id only when this run produces exactly one
            # chunk (see note above); otherwise every chunk gets a fresh unique id
            if reuse_id and n_chunks == 1:
                chunk_unique_id = reuse_id
            else:
                chunk_unique_id = ''.join(random.choices(string.ascii_letters + string.digits, k=20))

            archive_dict = _build_and_ship_chunk(config, filepath, archivepath, chunk_df,
                                                  chunk_unique_id, i, n_chunks, overwrite, keep)
            archive_dicts.append(archive_dict)

        # if not keep, delete the source files only after every chunk has been
        # successfully archived (avoids deleting files if a later chunk fails)
        if not keep and config['mode'] != 'dry-run':
            for file in fileDf['file'].tolist():
                fullpath = os.path.join(filepath,file)
                if os.path.isfile(fullpath):
                    os.remove(fullpath)

            # walk through the directories in filepath and remove them if they are empty
            for root, dirs, files in os.walk(filepath, topdown=False):
                # remove directories starting with .
                dirs = [d for d in dirs if not d.startswith('.')]
                # now descend into directoies and remove them if they are empty
                for dir in dirs:
                    if not os.listdir(os.path.join(root,dir)):
                        # rm dir
                        os.rmdir(os.path.join(root,dir))

    return archive_dicts

#
# Restore functions
#

# Function to unarchive files from S3 Glacier
def run_restore(config,filepath,keep=False):
    # exit if filepath isnt a directory
    if not os.path.isdir(filepath):
        print(f"'{filepath}' is not a valid directory path")
        sys.exit(1)

    # get the list of files that match dhslabarchive.\S+.json
    archive_json_files = glob.glob(os.path.join(filepath,'dhslabarchive.*.json'))
    if not archive_json_files:
        print(f"No archive record (dhslabarchive.*.json) found in '{filepath}'. Exiting.")
        sys.exit(1)

    # get the most recent json file
    archive_json = max(archive_json_files, key=os.path.getctime)

    #read in the json file
    with open(archive_json, 'r') as f:
        archive_dict = json.load(f)

    tarball = os.path.join(filepath,archive_dict['Filename'])

    if archive_dict['Location'] == 'glacier':
        # get storage class of the object
        storage_class = check_storage_class(archive_dict['ArchivePath'], archive_dict['Filename'], region=config['s3_region'])

        if storage_class == 'DEEP_ARCHIVE' or storage_class == 'GLACIER':

            # check if the object is currently being restored
            restore_status = check_restore_status(archive_dict['ArchivePath'], archive_dict['Filename'], region=config['s3_region'])

            # if ongoing request is false, initiate the restore
            if not restore_status or 'ongoing-request="false"' in restore_status:
                # initiate the restore request
                initiate_deep_archive_restore(archive_dict['ArchivePath'], archive_dict['Filename'], region=config['s3_region'])

            # wait for the restore to complete
            wait_for_restore(archive_dict['ArchivePath'], archive_dict['Filename'], region=config['s3_region'])

            # download the restored object
            download_s3_object(archive_dict['ArchivePath'], archive_dict['Filename'], tarball, region=config['s3_region'])

        # if is STANDARD_IA, then just download the object
        elif storage_class == 'STANDARD_IA' or storage_class == 'STANDARD' or storage_class == 'GLACIER_IR':

            download_s3_object(archive_dict['ArchivePath'], archive_dict['Filename'], tarball, region=config['s3_region'])

        else:
            print(f"Storage class {storage_class} is not supported. Exiting.")
            sys.exit(1)

    elif archive_dict['Location'] == 'ris_archive':
        # Cant restore from RIS, print message and exit
        print(f"Self-service restore from RIS archive not possible.")
        print(f"Please submit a ticket to request restore for this file:")
        print(f"{os.path.join(archive_dict['ArchivePath'], archive_dict['Filename'])} --> {tarball}")
        print(f"Exiting.")
        sys.exit(1)

    else:
        print(f"Unknown archive location '{archive_dict['Location']}'. Exiting.")
        sys.exit(1)

    # check tarball md5sum (tarball must exist locally by this point - it was just downloaded above)
    tarball_md5sum = calculate_file_md5sum(tarball)

    if tarball_md5sum != archive_dict['TarballMD5sum']:
        print(f"Tarball MD5sum does not match. Exiting.")
        sys.exit(1)

    # verify the tarball
    if not test_tarball_integrity(tarball, archive_dict['MD5Sums']):
        print(f"Restored tarball is not valid. Exiting.")
        sys.exit(1)

    # extract the tarball
    # with tarfile.open(tarball, 'r:gz') as tar:
    #     tar.extractall(filepath)

    print(f"{archive_dict['Filename']} successfully restored.")



def main():

    default_config = {
        'archive_name': 'dhslabarchive',
        'ris_archive_path': '/storage1/fs1/dspencer/Archive/spencerlab/dhs-lab-ris-archive-1',
        'globus_endpoint': 'b9545fe1-f647-40bf-9eaf-e66d2d1aaeb4',
        's3_bucket': 'dhs-lab-archive-1',
        's3_region': 'us-east-1',
        'storage_class': 'STANDARD_IA',
        'db': '',
    }

    # Main parser
    parser = argparse.ArgumentParser(description='Archive unarchive files either on the RIS system with globus or to S3 Glacier')

    # make a subpaser to set up the confi file
    subparsers = parser.add_subparsers(help='sub-command help',dest='subcommand')
    # config command
    parser_config = subparsers.add_parser('init', help='Initialize the configuration file')
    # config file location, defalt is ~/.slarchive
    parser_config.add_argument('-c', '--config', type=str, default='~/.dhslab-archive-config', help='Configuration file location')

    # Archive command
    parser_archive = subparsers.add_parser('archive', help='Archive files')

    # Archive command
    parser_restore = subparsers.add_parser('restore', help='Restore files')

    # Archive options
    # NOTE: type=str (not is_valid_file) here - the config file's existence is checked
    # explicitly below, after we know the subcommand isn't 'init'. Using is_valid_file as
    # the argparse type would run the check during parsing itself (even for defaults),
    # producing a generic argparse error before a user's first "init" run instead of the
    # friendly message below.
    parser_archive.add_argument('-c', '--config', type=str, default='~/.dhslab-archive-config', help='Configuration file location')
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

    # dry run
    parser_archive.add_argument('--dry-run', action='store_true', help='Dry run, do not archive')

    # Specify glacier or local archive
    # Create a mutually exclusive group for glacier vs local
    destination_group = parser_archive.add_mutually_exclusive_group(required=True)
    destination_group.add_argument('-G', '--glacier', action='store_true', help='Archive to S3 Glacier')
    destination_group.add_argument('-R', '--ris-archive', action='store_true', help='Archive to local RIS archive')

    parser_archive.add_argument('-d', '--database-file', type=is_valid_file, help='Database file to use')

    # RIS archive location
    parser_archive.add_argument('-a', '--archive-path', type=str, default=default_config['ris_archive_path'], help='Path to archive location')
    # Globus endpoint
    parser_archive.add_argument('-e', '--endpoint', type=str, default=default_config['globus_endpoint'], help='Globus endpoint to use')

    # S3 Glacier options
    parser_archive.add_argument('-r','--region', type=str, default=default_config['s3_region'], help='AWS region to use')
    parser_archive.add_argument('-b','--bucket', type=str, default=default_config['s3_bucket'], help='AWS bucket')
    parser_archive.add_argument('-s','--storage-class', type=str, help='S3 storage class')

    # only archive files matching a glob pattern, e.g. '*.fastq.gz'; can be given multiple
    # times to match any of several patterns (OR logic), e.g. -g '*.fastq.gz' -g '*.bam'
    parser_archive.add_argument('-g', '--pattern', action='append', default=None,
                                 help="Only archive files matching this glob pattern (e.g. '*.fastq.gz'). "
                                      "Matched against each file's basename. May be given multiple times "
                                      "(-g '*.fastq.gz' -g '*.bam') to include files matching ANY of the patterns.")

    # split large file sets into multiple size-limited tarballs
    parser_archive.add_argument('--chunk-size-gb', type=float, default=1000.0,
                                 help='Split files into separate tarballs/JSON records of at most this many '
                                      'GB each (default: 1000, i.e. ~1TB). Only applies when building a tarball '
                                      'from source files, not when using -t/--tarball. Set to 0 to disable chunking.')

    # the file/path to archive/unarchive
    parser_archive.add_argument('filepath', type=is_valid_path, nargs='+', help='Path or file to archive')

    parser_restore.add_argument('-c', '--config', type=str, default='~/.dhslab-archive-config', help='Configuration file location')
    parser_restore.add_argument('-d', '--delete', action='store_true', help='Delete the archive tarball after restoring')
    parser_restore.add_argument('filepath', type=is_valid_directory, help='Path to restore files to')

    # subparser for database commands
    parser_createdatabase = subparsers.add_parser('create-db', help='Create database commands')
    parser_createdatabase.add_argument('-c', '--config', type=str, default='~/.dhslab-archive-config', help='Configuration file location')
    parser_createdatabase.add_argument('-d', '--database-file', type=str, help='Database file to create or use')
    parser_createdatabase.add_argument('-t', '--table', type=str, default='dhslabarchive', help='Set database table name [dhslabarchive]')
    parser_createdatabase.add_argument('-o', '--overwrite', action='store_true', help='Overwrite database if it already exists')

    # dump the database to a JSON file
    parser_database = subparsers.add_parser('db', help='Create database commands')
    parser_database.add_argument('-c', '--config', type=str, default='~/.dhslab-archive-config', help='Configuration file location')
    parser_database.add_argument('-d', '--database-file', type=str, help='Database file to create or use')
    parser_database.add_argument('-t', '--table', type=str, default='dhslabarchive', help='Set database table name [dhslabarchive]')
    parser_database.add_argument('--dump', action='store_true', help='Dump the database to a JSON file')
    parser_database.add_argument('-s','--searchstring', type=str, help='Search the database for a string')

    args = parser.parse_args()

    # print help if no subcommand given
    if args.subcommand is None:
        parser.print_help()
        sys.exit(1)

    config = default_config

    # if the config command is called, then run the config function
    if args.subcommand == 'init':
        init_config_file(default_config,args)
        sys.exit(0)

    # check config file and load it, otherwise if it doesnt exist, then exit and say run init first
    if not os.path.isfile(os.path.expanduser(args.config)):
        print('Configuration file does not exist. Run "slarchive init" first')
        sys.exit(1)
    else:
        with open(os.path.expanduser(args.config), 'r') as f:
            config = json.load(f)

    # Execute the appropriate subcommand
    if args.subcommand == 'create-db':
        if args.database_file:
            config['db'] = args.database_file

        if args.table:
            config['archive_name'] = args.table

        if os.path.isfile(config['db']):

            if not args.overwrite:
                print(f"Database file {config['db']} already exists. Exiting.")
                sys.exit(1)
            else:
                # remove the database file
                os.remove(config['db'])
                create_archive_db(config['db'],config['archive_name'])

        else:
            create_archive_db(config['db'],config['archive_name'])

    elif args.subcommand == 'db':
        if args.database_file:
            config['db'] = args.database_file

        if args.dump:
            dump_database_to_json(config['db'],config['archive_name'])

        elif args.searchstring:
            # search the database for the searchstring
            search_database(config['db'],config['archive_name'],args.searchstring)

        else:
            # print help
            parser_database.print_help()

    elif args.subcommand == 'archive' and args.filepath and (args.glacier or args.ris_archive):

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
            if args.storage_class in ['DEEP_ARCHIVE', 'GLACIER', 'GLACIER_IR', 'STANDARD', 'STANDARD_IA']:
                config['storage_class'] = args.storage_class
            else:
                print(f"Storage class {args.storage_class} is not supported. Exiting.")
                sys.exit(1)

        archive_path = ''

        if args.dry_run:
            config['mode'] = 'dry-run'
            archive_path = '(dry run)'

        elif args.glacier:
            archive_path = config['s3_bucket']
            config['mode'] = 'glacier'

        elif args.ris_archive:
            archive_path = config['ris_archive_path']
            config['mode'] = 'ris_archive'

            # check that globus is installed
            if not shutil.which('globus'):
                print('Globus CLI not installed. Exiting.')
                sys.exit(1)

            # check that the user is logged into globus
            if not globus_login_active(config['globus_endpoint'], config['ris_archive_path']):
                print('Globus CLI not logged in, or no active Globus session. Run "globus login" and "globus session update" then try again. Exiting.')
                sys.exit(1)

        # convert --chunk-size-gb (decimal GB) into bytes; 0 or negative disables chunking
        if args.chunk_size_gb and args.chunk_size_gb > 0:
            chunk_size_bytes = int(args.chunk_size_gb * 1_000_000_000)
        else:
            chunk_size_bytes = None

        # expand/normalize args.filepath and get its absolute path
        for fp in args.filepath:

            filepath = os.path.expanduser(fp)
            filepath = os.path.abspath(filepath)

            # print directory that is being archived
            print(f"Archiving {filepath} to {archive_path}")

            archive_dats = run_archive(config,filepath,archive_path,tarball=args.tarball,keep=args.keep,
                                        force=args.force,overwrite=args.overwrite,pattern=args.pattern,
                                        chunk_size_bytes=chunk_size_bytes)

            # add each chunk's archive record to the database (usually just one, unless chunked)
            for archive_dat in archive_dats:
                if config['db'] and os.path.isfile(config['db']):
                    add_to_database(config['db'],config['archive_name'],archive_dat)

                elif config['db'] and config['db'].startswith('arn:aws'):
                    add_to_dynamodb('archive_table',archive_dat)

    elif args.subcommand == 'restore' and args.filepath:

            # expand/normalize args.filepath and get its absolute path
            filepath = os.path.expanduser(args.filepath)
            filepath = os.path.abspath(filepath)

            run_restore(config,filepath,keep=not args.delete)

    else:
        parser_archive.print_help()

if __name__ == "__main__":
    main()