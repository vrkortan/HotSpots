from __future__ import print_function
import pymysql
import time
import os
import sys
import csv
import pandas as pd



def sql_to_csv(filename_out,loc):
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

def mls_csv_to_flip_csv(location, csv_filename, filename_out):
    '''
    '''
    #check to see if file exists
    if os.path.isfile(filename_out):
        print ('file %s already exists' % (filename_out))
        sys.exit()

    #check to see if location is valid
    valid_locations = ['DC', 'DEN']
    if location not in valid_locations:
        print('location must be : ', end='')
        for l in valid_locations:
            print('%s, ' % (l), end='')
        print ('')
        sys.exit()

    print ('loading data...')
    #load csv
    df = pd.read_csv(csv_filename)

    print ('cleaning data...')
    #parse dates (only the ones we're interested in)
    all_date_columns = ['listed_on', 'contracted_on', 'sold_on']
    for col in all_date_columns:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')

    #drop rows with no date in 'sold_on', no lon, no lat
    df = df.dropna(subset=['sold_on', 'lng', 'lat'])

    #drop null columns
    if location == 'DC':
        null_columns = ['stories', 'version', 'parcel_number', 'hoa_name', 'hoa_fee', 'school_district', 'private_remarks',
                        'showings_phone', 'approval_condition', 'lot_size_acres', 'basement_finished_status', 'architecture',
                        'previous_price', 'off_market_on', 'listing_agent_mls_id', 'listing_agent_email', 'listing_agent_phone']
    elif location == 'DEN':
        null_columns = ['stories', 'version', 'parcel_number', 'hoa_name', 'hoa_fee', 'school_district', 'private_remarks',
                        'showings_phone', 'approval_condition', 'lot_size_acres', 'basement_finished_status', 'architecture',
                        'previous_price', 'off_market_on']

    df = df.drop(null_columns, axis=1)

    #drop less than useful columns
    if location == 'DC':
        lessuseful_columns = ['externally_last_updated_at', 'photos', 'listing_agent', 'listing_brokerage', 'selling_brokerage_name',
                              'selling_agent_mls_id', 'selling_agent_phone', 'seller_concessions', 'property_type', 'created_at',
                              'updated_at']
    elif location == 'DEN':
        lessuseful_columns = ['externally_last_updated_at', 'photos', 'listing_agent', 'listing_brokerage',
                              'seller_concessions', 'property_type', 'created_at', 'updated_at']
    df = df.drop(lessuseful_columns, axis=1)

    #drop rows with dates > today
    for col in all_date_columns:
        df = df.drop(df[df[col] > pd.to_datetime('today')].index)

    #take care of wrong lon/lat point in DC data
    if (location == 'DC'):
        if (df.shape[0] >= 12022): # bad lat
            df = df.drop(df[df['lat'] == df['lat'].min()].index)
        if (df.shape[0] >= 402539): # bad lat
            df = df.drop(df[df['lat'] == df['lat'].max()].index)
    #take care of wrong lon/lat point in DEN data
    if (location == 'DEN'):
        df = df.drop(df[df['lat'] == 0.00].index) #bad lat
        if (df.shape[0] >= 102535): #bad lng
                df = df.drop(df[df['lng'] == df['lng'].min()].index)

    #keep only top 98% of location data
    df = df[(df['lat'] > df['lat'].quantile(0.02)) & (df['lat'] < df['lat'].quantile(0.98))]
    df = df[(df['lng'] > df['lng'].quantile(0.02)) & (df['lng'] < df['lng'].quantile(0.98))]

    #properties listed more than once by property id
    df_flip = df[['property_id','sold_on']]
    df_flip = df_flip.dropna(subset=['property_id'])
    df_flip = df_flip.groupby('property_id').count()
    df_flip = df_flip[df_flip['sold_on'] > 1]
    df_flip = df_flip.reset_index() #property_id back to column status
    df_flip.columns = ['property_id','times_listed']

    #metrics we want to keep
    keep_metrics = ['property_id', 'lat', 'lng', 'listed_on', 'contracted_on', 'sold_on', 'list_price', 'sold_price',
                    'above_grade_square_feet', 'finished_square_feet', 'derived_basement_square_feet', 'garages',
                    'beds', 'baths', 'zip', 'year_built', 'lot_size_square_feet', 'zoned', 'is_attached']
    keep_metrics_dict = {}
    cnt = 0
    for m in keep_metrics:
        keep_metrics_dict[m] = cnt
        cnt += 1

    df_keep_metrics = df[keep_metrics]

    #order so last row is most recent sale
    df_keep_metrics = df_keep_metrics.sort_values('sold_on')

    #metrics we want to calculated for flipped properties
    flipped_metrics = [
                        #should be same for all enetries with the same property id
                        'property_id', 'lat', 'lng', 'zip',
                        #want to keep a copy for each side of sale, A=row, B=row+1
                        'listed_on_A', 'contracted_on_A', 'sold_on_A', 'list_price_A', 'sold_price_A', 'year_built_A', 'beds_A', 'baths_A',
                        'finished_square_feet_A', 'above_grade_square_feet_A', 'derived_basement_square_feet_A', 'garages_A',
                        'listed_on_B', 'contracted_on_B', 'sold_on_B', 'list_price_B', 'sold_price_B', 'year_built_B', 'beds_B', 'baths_B',
                        'finished_square_feet_B', 'above_grade_square_feet_B', 'derived_basement_square_feet_B', 'garages_B',
                        #want change in value from B to A
                        'change_above_grade_square_feet', 'change_finished_square_feet',
                        'change_derived_basement_square_feet', 'change_garages', 'change_beds',
                        'change_baths', 'change_year_built', 'change_lot_size_square_feet',
                        #just want to know if change in value from B to A
                        'change_zoned_tf', 'change_is_attached_tf'
                       ]

    #calculate flipped metrics
    print ("calculating flipped metrics ...")
    flipped = []
    flipped.append(flipped_metrics)

    total_num = df_flip.shape[0]
    cnt=1
    for flip_row in xrange(0, df_flip.shape[0]):
        start = time.time()
        print ('%i/%i' % (cnt,total_num), end = '\t')
        cnt += 1

        df_temp = df_keep_metrics.loc[ df['property_id'] == df_flip.iloc[flip_row,0] ]

        for temp_row in xrange(0, df_temp.shape[0]-1):
            A = temp_row
            B = temp_row + 1

            row = []

            #should be same for all enetries with the same property id
            row.append( df_temp.iloc[ A, keep_metrics_dict['property_id'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['lat'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['lng'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['zip'] ] )

            #want to keep a copy for each side of sale, A=row, B=row+1
            #A
            row.append( df_temp.iloc[ A, keep_metrics_dict['listed_on'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['contracted_on'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['sold_on'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['list_price'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['sold_price'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['year_built'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['beds'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['baths'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['finished_square_feet'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['above_grade_square_feet'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['derived_basement_square_feet'] ] )
            row.append( df_temp.iloc[ A, keep_metrics_dict['garages'] ] )
            #B
            row.append( df_temp.iloc[ B, keep_metrics_dict['listed_on'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['contracted_on'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['sold_on'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['list_price'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['sold_price'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['year_built'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['beds'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['baths'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['finished_square_feet'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['above_grade_square_feet'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['derived_basement_square_feet'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['garages'] ] )

            #want change in value from B to A
            row.append( df_temp.iloc[ B, keep_metrics_dict['above_grade_square_feet'] ] - df_temp.iloc[ A, keep_metrics_dict['above_grade_square_feet'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['finished_square_feet'] ] - df_temp.iloc[ A, keep_metrics_dict['finished_square_feet'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['derived_basement_square_feet'] ] - df_temp.iloc[ A, keep_metrics_dict['derived_basement_square_feet'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['garages'] ] - df_temp.iloc[ A, keep_metrics_dict['garages'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['beds'] ] - df_temp.iloc[ A, keep_metrics_dict['beds'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['baths'] ] - df_temp.iloc[ A, keep_metrics_dict['baths'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['year_built'] ] - df_temp.iloc[ A, keep_metrics_dict['year_built'] ] )
            row.append( df_temp.iloc[ B, keep_metrics_dict['lot_size_square_feet'] ] - df_temp.iloc[ A, keep_metrics_dict['lot_size_square_feet'] ] )

            #just want to know if change in value from B to A
            if df_temp.iloc[ A, keep_metrics_dict['zoned'] ] == df_temp.iloc[ B, keep_metrics_dict['zoned'] ]:
                row.append(True)
            else:
                row.append(False)
            if df_temp.iloc[ A, keep_metrics_dict['is_attached'] ] == df_temp.iloc[ B, keep_metrics_dict['is_attached'] ]:
                row.append(True)
            else:
                row.append(False)

            flipped.append(row)

        #keep time metrics
        end = time.time()
        print (end-start)

    print ('')

    #write to csv
    print ("writting flipped metrics to csv...")
    with open(filename_out, 'w') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n', delimiter=',')
        writer.writerows(flipped)


























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

    mls_csv_to_flip_csv('DC',
                        '/Users/victoriakortan/Desktop/HotSpots/privy_data/dc_full_data.csv',
                        '/Users/victoriakortan/Desktop/HotSpots/privy_data/dc_flip_data.csv')
    mls_csv_to_flip_csv('DEN',
                        '/Users/victoriakortan/Desktop/HotSpots/privy_data/den_full_data.csv',
                        '/Users/victoriakortan/Desktop/HotSpots/privy_data/den_flip_data.csv')
