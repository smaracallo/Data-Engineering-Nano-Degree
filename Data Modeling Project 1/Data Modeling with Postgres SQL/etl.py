# Import Python packages
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv
from sql_queries import *


def process_csv_data():
    """ Description: Process all files in the event_data folder to create a csv file

    """
    # checking your current working directory
    print(os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*'))
        # print(file_path_list)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

                # uncomment the code below if you would like to get total number of rows
    # print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    # print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    print("CSV created")


def process_data(session, tables, file):
    """ Description: Create and perform ETL process to query tables

        Arguments:
        session: establishes connection and begin executing queries.
        tables: list object that holds queries for each table required.
        file: csv file to perform ETL.
    """

    for process in tables:
        session.execute(process['create'])
        print('created')

        with open(file, encoding='utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader)  # skip header

            for line in csvreader:
                csv_assigned_lines = {'music_session': [int(line[8]), int(line[3]), line[0], line[9], float(line[5])],
                                      'user_session': [int(line[10]), int(line[8]), int(line[3]), line[0], line[9],
                                                       line[1], line[4]],
                                      'song_user': [line[9], int(line[10]), line[1], line[4]]}
                # Assign the INSERT statements into the `query` variable
                insert_query = process['insert']
                session.execute(insert_query, csv_assigned_lines[process['table']])
            print('inserted')

            # Verify that the data have been inserted into each table
            rows = session.execute(process['select'])
            print(process['table'] + ' Query Results:')

            # print dataframe from select query results
            row_data = []
            for row in rows:
                row_data.append(row)
                df = pd.DataFrame(row_data)
            df


def main():
    # create cluster
    cluster = Cluster()

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()

    # Create a Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS project
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                    )

    # TO-DO: Set KEYSPACE to the keyspace specified above
    session.set_keyspace('project')

    session.execute("DROP TABLE IF EXISTS music_session")
    session.execute("DROP TABLE IF EXISTS user_session")
    session.execute("DROP TABLE IF EXISTS song_user")

    process_csv_data()
    process_data(session, process_tables, file='event_datafile_new.csv')

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()