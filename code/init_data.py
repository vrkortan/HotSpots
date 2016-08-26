from __future__ import print_function
import pymysql
import time
import os
import sys
import csv
import pandas as pd



def sql_to_csv(filename_out,loc):
    '''
    change the mls sql data file into a csv file
    '''
    dbServer = 'localhost'
    dbUser = 'root'
    if loc == 'DC':
        dbName = 'dc_mls'
    elif loc == 'DEN':
        dbName = 'den_mls'
    else:
        sys.exit()

    print ("loading %s" % (dbName), end=' ')
    dbQuery = "SELECT * FROM property_listings;"
    conn = pymysql.connect(host=dbServer, user=dbUser, passwd='Obabtat<3', db=dbName)
    df = pd.read_sql(dbQuery, con=conn)
    print ("done")

    if os.path.isfile(filename_out):
        print ('file %s already exists' % (filename_out))
    else:
        print ("writing %s to %s ..." % (dbName, filename_out), end=' ')
        df.to_csv(filename_out, header=True, index=False)
        print ("done")

    conn.close()







if __name__ == '__main__':

    #to get the sql databases sorted out
    #to start connection to database:
    #terminal
        #sudo mysqld --user=root
    #to import sql files into databases
    #new terminal
        #sudo mysql -u root -p
        #>temp_password
        #mysql> SET PASSWORD = PASSWORD('new');
        #mysql> CREATE DATABASE dc_mls;
        #mysql> CREATE DATABASE den_mls;
        #mysql> use dc_mls;
        #mysql> source /Users/victoriakortan/Desktop/HotSpots/privy_data/mls_sql/dc_data.sql
        #mysql> use den_mls;
        #mysql> source /Users/victoriakortan/Desktop/HotSpots/privy_data/mls_sql/denver_data.sql
    #figure out table names
        #mysql> use dc_mls;
        #mysql> SHOW TABLES;
            # +-------------------+
            # | Tables_in_dc_mls  |
            # +-------------------+
            # | properties        |
            # | property_listings |
            # +-------------------+
        #mysql> use den_mls;
        #mysql> SHOW TABLES;
            # +-------------------+
            # | Tables_in_den_mls |
            # +-------------------+
            # | properties        |
            # | property_listings |
            # +-------------------+
    #to see column names
        #mysql> DESCRIBE property_listings;

    sql_to_csv('/Users/victoriakortan/Desktop/HotSpots/privy_data/dc_full_data.csv','DC')
    sql_to_csv('/Users/victoriakortan/Desktop/HotSpots/privy_data/den_full_data.csv','DEN')
