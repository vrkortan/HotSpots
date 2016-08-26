from __future__ import print_function
import os
import sys
import time
import csv
import json
import multiprocessing
import pandas as pd
import numpy as np
from PIL import Image

#helper function for calling c++ program 'gausify' in a parallelized scheme
def run_cpp_conv_png(call):
    '''
    call gausify.cpp to create the PNG image
        - the call variable (string) should be in the form:
            "gausify -l=min_lon,max_lon,min_lat,max_lat -g=grid_x,grid_y -s=sigma -b=blocks -c=distance_cutoff filename -i=1"
    '''
    start = time.time()
    os.system(call)
    end = time.time()
    print ('%.3f (sec)' % (end-start), end='')
    print ('')


class MLS:
    '''
    class for reading in and producing spatial maps of mls data
    '''

    '''
    CALCULATING FLIPPED CSV
    '''
    def load_full_csv(self):
        '''
        load the full csv of MLS data for the given location
        '''
        print ('loading full data...')
        all_date_columns = ['created_at', 'updated_at', 'listed_on', 'contracted_on', 'sold_on']
        #load csv
        df = pd.read_csv(self.csv_full_filename, parse_dates=all_date_columns, infer_datetime_format=True)
        #drop rows with no date in 'sold_on', no lon, no lat
        df = df.dropna(subset=['sold_on', 'lng', 'lat'])
        #drop rows with dates > today
        for col in all_date_columns:
            df = df.drop(df[df[col] > pd.to_datetime('today')].index)
        #drop null columns
        all_null_columns = ['stories', 'version', 'parcel_number', 'hoa_name', 'hoa_fee', 'school_district', 'private_remarks',
                        'showings_phone', 'approval_condition', 'lot_size_acres', 'basement_finished_status', 'architecture',
                        'previous_price', 'off_market_on', 'listing_agent_mls_id', 'listing_agent_email', 'listing_agent_phone']
        #make sure null_columns exist in df.columns
        null_columns = [col for col in all_null_columns if col in df.columns]
        df = df.drop(null_columns, axis=1)
        #drop less than usefull columns
        all_lessusefull_columns = ['externally_last_updated_at', 'photos', 'listing_agent', 'listing_brokerage', 'selling_brokerage_name',
                              'selling_agent_mls_id', 'selling_agent_phone', 'seller_concessions', 'property_type']
        #make sure null_columns exist in df.columns
        lessusefull_columns = [col for col in all_lessusefull_columns if col in df.columns]
        df = df.drop(lessusefull_columns, axis=1)
        #take care of wrong lon/lat point in DC data
        if (self.location == 'DC'):
            if (df.shape[0] >= 12022): # bad lat
                df = df.drop(df[df['lat'] == df['lat'].min()].index)
            if (df.shape[0] >= 402539): # bad lat
                df = df.drop(df[df['lat'] == df['lat'].max()].index)
        #take care of wrong lon/lat point in DEN data
        if (self.location == 'DEN'):
            df = df.drop(df[df['lat'] == 0.00].index) #bad lat
            if (df.shape[0] >= 102535): #bad lng
                    df = df.drop(df[df['lng'] == df['lng'].min()].index)

        #keep only top 98% of location data
        df = df[(df['lat'] > df['lat'].quantile(0.02)) & (df['lat'] < df['lat'].quantile(0.98))]
        df = df[(df['lng'] > df['lng'].quantile(0.02)) & (df['lng'] < df['lng'].quantile(0.98))]

        return df

    def make_flip_csv(self, df):
        '''
        create the flip stats csv by looking through the full MLS data for property id's that are listed more than once
            - the only requirement here is that the property id is listed more than once
            - one entry in the flip csv will be made for each pair of the same property ids,
                say there are 3 listing for one property id, the first & second will be one flip,
                and the second & third will be considered a flip
        '''
        #identify properties listed more than once by property id
        df_flip = df[['property_id','sold_on']]
        df_flip = df_flip.dropna(subset=['property_id'])
        df_flip = df_flip.groupby('property_id').count()
        df_flip = df_flip[df_flip['sold_on'] > 1]
        df_flip = df_flip.reset_index() #property_id back to column status
        df_flip.columns = ['property_id','times_listed']

        #metrics we want to keep
        flip_metrics = ['property_id', 'lat', 'lng', 'listed_on', 'contracted_on', 'sold_on', 'list_price', 'sold_price',
                        'above_grade_square_feet', 'finished_square_feet', 'derived_basement_square_feet', 'garages',
                        'beds', 'baths', 'zip', 'year_built', 'lot_size_square_feet', 'zoned', 'is_attached']
        # make dictionary the relates flip metric to column number
        flip_metrics_dict = {}
        cnt = 0
        for m in flip_metrics:
            flip_metrics_dict[m] = cnt
            cnt += 1

        # new dataframe with just the cols we want
        df_flip_metrics = df[flip_metrics]

        #order so last row is most recent sale
        df_flip_metrics = df_flip_metrics.sort_values('sold_on')

        flipped_metrics = [
                            #should be same for all enetries with the same property id
                            'property_id', 'lat', 'lng', 'zip',
                            #want to keep a copy for each side of sale, A=row, B=row+1
                            'listed_on_A', 'contracted_on_A', 'sold_on_A', 'list_price_A', 'sold_price_A', 'beds_A', 'baths_A',
                            'listed_on_B', 'contracted_on_B', 'sold_on_B', 'list_price_B', 'sold_price_B', 'beds_B', 'baths_B',
                            #just want change in value from B to A
                            'change_above_grade_square_feet', 'change_finished_square_feet',
                            'change_derived_basement_square_feet', 'change_garages', 'change_beds',
                            'change_baths', 'change_year_built', 'change_lot_size_square_feet',
                            #just want to know if change in value from B to A
                            'change_zoned_tf', 'change_is_attached_tf'
                           ]

        #calculate flipped metrics
        #chose to do this in lists and not with a data frame
        print ("calculating flipped metrics ...")
        flipped = []
        flipped.append(flipped_metrics)

        total_num = df_flip.shape[0]
        cnt=1
        for flip_row in xrange(0, df_flip.shape[0]):
            start = time.time()
            print ('%i/%i' % (cnt,total_num), end = '\t')
            cnt += 1

            #make a temporary table of a given property id that has been identified as a flip
            df_temp = df_flip_metrics.loc[ df['property_id'] == df_flip.iloc[flip_row,0] ]

            #bulild up the row that will eventually be added to the flipped dataframe
            for temp_row in xrange(0, df_temp.shape[0]-1):
                A = temp_row
                B = temp_row + 1

                row = []

                #should be same for all enetries with the same property id
                row.append( df_temp.iloc[ A, flip_metrics_dict['property_id'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['lat'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['lng'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['zip'] ] )

                #want to keep a copy for each side of sale, A=row, B=row+1
                row.append( df_temp.iloc[ A, flip_metrics_dict['listed_on'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['contracted_on'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['sold_on'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['list_price'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['sold_price'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['beds'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['baths'] ] )
                row.append( df_temp.iloc[ A, flip_metrics_dict['sold_price'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['listed_on'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['contracted_on'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['sold_on'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['list_price'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['sold_price'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['beds'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['baths'] ] )

                #just want change in value from B to A
                row.append( df_temp.iloc[ B, flip_metrics_dict['above_grade_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['above_grade_square_feet'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['finished_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['finished_square_feet'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['derived_basement_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['derived_basement_square_feet'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['garages'] ] - df_temp.iloc[ A, flip_metrics_dict['garages'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['beds'] ] - df_temp.iloc[ A, flip_metrics_dict['beds'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['baths'] ] - df_temp.iloc[ A, flip_metrics_dict['baths'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['year_built'] ] - df_temp.iloc[ A, flip_metrics_dict['year_built'] ] )
                row.append( df_temp.iloc[ B, flip_metrics_dict['lot_size_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['lot_size_square_feet'] ] )

                #just want to know if change in value from B to A
                if df_temp.iloc[ A, flip_metrics_dict['zoned'] ] == df_temp.iloc[ B, flip_metrics_dict['zoned'] ]:
                    row.append(True)
                else:
                    row.append(False)
                if df_temp.iloc[ A, flip_metrics_dict['is_attached'] ] == df_temp.iloc[ B, flip_metrics_dict['is_attached'] ]:
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
            writer = csv.writer(self.csv_flip_filename, lineterminator='\n', delimiter=',')
            writer.writerows(flipped)

    '''
    DROPPING NONSENSICLE FLIPPED DATA
    '''
    def drop_spread_times(self, df):
        '''
        drop flip stats with spread times that are less than 'drop_spread_greater' (set by hand in init)
        drop flip stats with spread times that are greater than 'dop_spread_less' (set by hand in init)
        '''
        #keep only spread times that are < 2 years
        df = df[(df['sold_on_B'] - df['sold_on_A']).astype('timedelta64[D]') <= self.drop_spread_greater]
        #keep onlytimes that are > 2 weeks
        df = df[(df['sold_on_B'] - df['sold_on_A']).astype('timedelta64[D]') >= self.drop_spread_less]
        return df

    def drop_goofy_pairs(self, df):
        '''
        drop flip stats with a difference in the time listed_on_B and listed_on_A less than 'drop_spread_less'
        drop flip stats with negative time difference from listed_on_B and sold_on_A
        drop flip stats with negative time difference from contracted_on_A and listed_on_A (and B)
        drop flip stats with time difference from contracted_on_A and listed_on_A greater than 'drop_DOM_greater' (and B)
        drop flip stats with time difference from sold_on_A and contracted_on_A is negative (and B)
        drop flip stats with time difference from sold_on_A and contracted_on_A is greater than 'drop_DOM_greater' (and B)
        '''
        #keep only differences in listed_on_B and listed_on_A is > 2 weeks
        df = df[(df['listed_on_B'] - df['listed_on_A']).astype('timedelta64[D]') >= self.drop_spread_less]

        #keep only differences in listed_on_B and sold_on_A is > 0
        df = df[(df['listed_on_B'] - df['sold_on_A']).astype('timedelta64[D]') >= 0]

        #keep only differences in contracted_on_A and listed_on_A is > 0
        df = df[(df['contracted_on_A'] - df['listed_on_A']).astype('timedelta64[D]') >= 0]
        #keep only differences in contracted_on_A and listed_on_A is < 8 months
        df = df[(df['contracted_on_A'] - df['listed_on_A']).astype('timedelta64[D]') <= self.drop_DOM_greater]

        #keep only differences in contracted_on_B and listed_on_B is > 0
        df = df[(df['contracted_on_B'] - df['listed_on_B']).astype('timedelta64[D]') >= 0]
        #keep only differences in contracted_on_B and listed_on_B is < 8 months
        df = df[(df['contracted_on_B'] - df['listed_on_B']).astype('timedelta64[D]') <= self.drop_DOM_greater]

        #keep only differences in sold_on_A and contracted_on_A is > 0
        df = df[(df['sold_on_A'] - df['contracted_on_A']).astype('timedelta64[D]') >= 0]
        #keep only differences in sold_on_A and contracted_on_A is < 8 months
        df = df[(df['sold_on_A'] - df['contracted_on_A']).astype('timedelta64[D]') <= self.drop_sale_to_contract_greater]

        #keep only differences in sold_on_B and contracted_on_B is > 0
        df = df[(df['sold_on_B'] - df['contracted_on_B']).astype('timedelta64[D]') >= 0]
        #keep only differences in sold_on_B and contracted_on_B is < 8 months
        df = df[(df['sold_on_B'] - df['contracted_on_B']).astype('timedelta64[D]') <= self.drop_sale_to_contract_greater]

        return df

    '''
    SETTING UP DATA FRAME FOR SPECIFIC VARIABLES TO BE MAPPED
    '''
    def get_price_range(self, df):
        '''
        keep only houses with sold_price_A in the selected range (set by target_price)
        '''
        #keep only houses with sold_price_A in the selected range
        df = df[ (df['sold_price_A'] >= self.price_dict[self.target_price][0]) & (df['sold_price_A'] <= self.price_dict[self.target_price][1])]
        return df

    def calc_spread(self, df):
        '''
        calculate the spread for flips, drop negative values
        set date to sold_on_B
        '''
        df = df.dropna(subset = ['sold_price_A', 'sold_price_B'])
        df2 = df.copy()
        df2['sold_price_A'] = pd.to_numeric(df['sold_price_A'], errors='coerce')
        df2['sold_price_B'] = pd.to_numeric(df['sold_price_B'], errors='coerce')
        df2[self.target_name] = df2['sold_price_B'] - df2['sold_price_A']
        #remove negative spread
        df2 = df2[df2[self.target_name] >= 0]
        self.date_column = 'sold_on_B'
        return df2

    def calc_hold_time(self, df):
        '''
        calculate the hold_time for flips
        set date to sold_on_B
        '''
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['sold_on_B'] - df['sold_on_A']).astype('timedelta64[D]')
        self.date_column = 'sold_on_B'
        return df

    def calc_project_days(self, df):
        '''
        calculate the number of days the flip took
        set date to listed_on_B
        '''
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['listed_on_B'] - df['sold_on_A']).astype('timedelta64[D]')
        self.date_column = 'listed_on_B'
        return df

    def calc_initial_days_to_contract(self, df):
        '''
        calculate the initial DOM
        set date to contracted_on_A
        '''
        df = df.dropna(subset = ['contracted_on_A'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['contracted_on_A'] - df['listed_on_A']).astype('timedelta64[D]')
        self.date_column = 'contracted_on_A'
        return df

    def calc_initial_days_contract_to_sale(self, df):
        '''
        calculate the initial days from contract to sale (measure of using cash or lone)
        set date to sold_on_A
        '''
        df = df.dropna(subset = ['contracted_on_A'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['sold_on_A'] - df['contracted_on_A']).astype('timedelta64[D]')
        self.date_column = 'sold_on_A'
        return df

    def calc_final_days_to_contract(self, df):
        '''
        calculate the final DOM
        set date to contracted_on_B
        '''
        df = df.dropna(subset = ['listed_on_B', 'contracted_on_B'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['contracted_on_B'] - df['listed_on_B']).astype('timedelta64[D]')
        self.date_column = 'contracted_on_B'
        return df

    def calc_final_days_contract_to_sale(self, df):
        '''
        calculate the final days from contract to sale (measure of using cash or lone)
        set date to sold_on_B
        '''
        df2 = df.dropna(subset = ['contracted_on_B'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['sold_on_B'] - df['contracted_on_B']).astype('timedelta64[D]')
        self.date_column = 'sold_on_B'
        return df

    '''
    INITIALIZATION
    '''
    def __init__(self, csv_full_filename='/Users/victoriakortan/Desktop/HotSpots/privy_data/den_full_data.csv',
                 csv_flip_filename='/Users/victoriakortan/Desktop/HotSpots/privy_data/den_flip_data.csv',
                 base_dir='/Users/victoriakortan/Desktop/HotSpots/data', location='DC', target_name='', distance_cutoff=10,
                 grid_x=1200, grid_y=1200, grid_square=False, sigma_blocks=10, month_dir='month', parallelize=True,
                 target_price='all'):
        '''
        initialize MLS object
            - set variables that controle what is getting calculated
            - make flip stats if that file doesn't already exist
            - set up some dictionaries for use later
            - arbitrarily set:
                    self.min_data_points = 0 #if timeframe doesn't have more than min_data_points points, drop it
                    self.drop_spread_greater = 730 #if spread is larger than 2 years drop (in days)
                    self.drop_spread_less = 14 #if spread is less than 2 weeks drop (in days)
                    self.drop_DOM_greater = 245 #if took longer to go under contract than 8 months don't care
                    self.drop_sale_to_contract_greater = 245 #if took longer than 8 months to go from under contract to sold don't care
        '''

        #define with initialization
        self.csv_full_filename = csv_full_filename
        self.csv_flip_filename = csv_flip_filename
        self.base_dir = base_dir
        self.month_dir = month_dir
        self.month_ave = int(self.month_dir.split('_')[-1]) #every month's data includes a window of the past x months
        self.location = location
        self.target_name = target_name
        self.grid_x = grid_x
        self.grid_y = grid_y
        self.grid_square = grid_square
        self.sigma_blocks = sigma_blocks
        self.parallelize = parallelize
        self.target_price=target_price
        self.distance_cutoff=distance_cutoff

        #write flip stats csv if not already created
        #check to see if file exists
        if not os.path.isfile(self.csv_flip_filename):
            df = self.load_full_csv()
            self.make_flip_csv(df)

        #setup dictionary of possible target prices
        self.price_dict = {
                            '' : (0.0, 100000000.0),
                            'all' : (0.0, 100000000.0),
                            '0_200000' : (0.0, 200000.0),
                            '200000_400000' : (200000.0, 400000.0),
                            '400000_600000' : (400000.0, 600000.0),
                            '600000_800000' : (600000.0, 800000.0),
                            '800000_1000000' : (800000.0, 1000000.0),
                            '1000000_plus' : (1000000.0, 100000000.0),
                            }
        #check to make sure target_name is a valid choice
        if self.target_price not in self.price_dict:
            print ('invalid target price, try: ')
            for key in self.price_dict:
                print (" %s," % (key), end='')
            print ('')
            sys.exit()
        #make sure empty string defaults to 'all'
        if self.target_price == '':
            self.target_price = 'all'

        #define dictionary for target_names
            #the value called is a function that takes a dataframe and returns a dataframe with a column named the key (will be the variables the color map is based on)
        self.target_dict = {
                            '' : self.calc_spread,
                            'spread' : self.calc_spread,
                            'hold_time' : self.calc_hold_time,
                            'project_days' : self.calc_project_days,
                            'initial_days_to_contract' : self.calc_initial_days_to_contract,
                            'initial_days_contract_to_sale' : self.calc_initial_days_contract_to_sale,
                            'final_days_to_contract' : self.calc_final_days_to_contract,
                            'final_days_contract_to_sale' : self.calc_final_days_contract_to_sale,
                            }
        #check to make sure target_name is a valid choice
        if self.target_name not in self.target_dict:
            print ('invalid target name, try: ')
            for key in self.target_dict:
                print (" %s," % (key), end='')
            print ('')
            sys.exit()
        #make sure empty string defaults to 'all'
        if self.target_name == '':
            self.target_name = 'spread'
        #set weather or not a high number should be incoded as red
        if self.target_name in ['spread', 'hold_time', 'project_days']:
            self.highred = 1
        else:
            self.highred = 0

        #calc sigma (units of lat/lon)
        #city block is ~ 1/16 of a mile, 1 degree longitude = 69.172 miles, 1 degree latitude = cos (lat) * 69.172
        #roughly call 1 degree = 60 miles, so one block is (1/16)/60 = ~ 0.001
        self.sigma = 0.001 * self.sigma_blocks

        #arbirarily define now
        self.min_data_points = 0 #if timeframe doesn't have more than min_data_points points, drop it
        self.drop_spread_greater = 730 #if spread is larger than 2 years drop (in days)
        self.drop_spread_less = 14 #if spread is less than 2 weeks drop (in days)
        self.drop_DOM_greater = 245 #if took longer to go under contract than 8 months don't care
        self.drop_sale_to_contract_greater = 245 #if took longer than 8 months to go from under contract to sold don't care

        #other variables defined later on:
        # #date to classify by
        # self.date_column = date_column
        # #lon lat
        # self.min_lon
        # self.max_lon
        # self.min_lat
        # self.max_lat
        # self.mean_lon
        # self.mean_lat
        # #target (write_dates)
        # self.min_target
        # self.max_target
        # #dates
        # self.dates #list of touples (year, weekofyear)
        # #calculation specific directory
        # self.calc_specific_dir
        # #buckets (for color key)
        # self.bucket_devisions

    '''
    LOAD, WRITE DATE DATA & MAKE PNGs
    '''
    def load_flip(self):
        '''
        load the approproate flip csv into a dataframe and remove silly values
        create a data frame with the values that will be necessary for the app and writing the png files
        '''
        print ('loading flipped data...')
        all_date_columns = ['listed_on_A', 'contracted_on_A', 'sold_on_A', 'listed_on_B', 'contracted_on_B', 'sold_on_B']

        #load csv
        df = pd.read_csv(self.csv_flip_filename, parse_dates=all_date_columns, infer_datetime_format=True)

        #git rid of goofy values
        df = df.dropna(subset=all_date_columns)
        df = self.drop_spread_times(df)
        df = self.drop_goofy_pairs(df)

        #set location variables
        #do this here because I want it consistant for all price ranges and variables for a given location
        self.set_locs(df)

        #set calculation specific directory
        self.set_grid()
        self.calc_specific_dir = str(self.grid_x) + 'X' + str(self.grid_y) + '_' + 'sigmablocks' + '_' + str(self.sigma_blocks)

        #set possible dates
        #do this here because I want it consistant for all price ranges and variables for a given location
        self.set_dates(df)

        #call function to calculate target variable, and set self.date_column
        df = self.target_dict[self.target_name](df)
        #keep these columns, last ones are for the tags on the markers in the map, if want to change also need to change in write_month_to_csv and in gausify.cpp
        df = df[[self.target_name, 'lng', 'lat', self.date_column, 'sold_price_A', 'beds_A', 'beds_B', 'baths_A', 'baths_B']]

        #min max target valuese:
        #do this here because I want it consistant for all price ranges for a given location
        if self.target_name == 'spread':
            self.min_target = 0.0
            self.max_target = 900000.0
        else:
            self.min_target = df[self.target_name].min()
            self.max_target = df[self.target_name].max()
        #buckets:
        #do this here because I want it consistant for all price ranges for a given location
        self.calc_buckets()

        #use selected target price range
        df = self.get_price_range(df)

        return df

    def write_month_to_csv(self, df):
        '''
        write the data frame from load_csv() by month to csv files in the appropriate directory (variable, price range)
        '''
        print ('writing date csv files ...')

        #make sure directory exists
        date_base_dir = self.base_dir + '/' + self.month_dir + '/' + self.location + '/' + self.target_name + '/' + self.target_price
        if not os.path.exists(date_base_dir):
            os.makedirs(date_base_dir)

        #write metatdata for this calculation
        #metadata base_dir
        metadata_base_dir = self.base_dir + '/' + self.month_dir + '/' + self.location + '/' + self.target_name + '-' + self.target_price
        #metadata
        self.write_metadata(metadata_base_dir)

        total_num = len(self.dates)
        cnt=1
        for date in self.dates:
            print ('%i/%i' % (cnt,total_num))
            cnt += 1
            year, month = date

            month_str = str(month)
            if len(str(month)) < 2:
                month_str = '0%i' % (month)
            else:
                month_str = str(month)

            #output filename
            filename = date_base_dir + '/' + str(year) + '_' + month_str + '.csv'

            #only use data in a month_ave (set by month_dir) month period previous to the current date being considered
            end_date = pd.to_datetime('%i-%i' % (year,month)).to_period('M').to_timestamp('M')
            start_date = end_date - pd.DateOffset(months=self.month_ave)

            df2 = df.loc[(pd.DatetimeIndex(df[self.date_column]) > start_date)
                            & (pd.DatetimeIndex(df[self.date_column]) <= end_date)].copy()

            df2 = df2.drop(self.date_column, axis=1)

            #make sure enough points to worry about
            if df2.shape[0] >= self.min_data_points:
                #write to csv
                #check to see if file exists
                if os.path.isfile(filename):
                    print ('file %s already exists' % (filename))
                else:
                    os.system('touch %s' % (filename)) #to write empty file, not sure if pandas will write an empty df to csv
                    df2[[self.target_name, 'lng', 'lat', 'sold_price_A', 'beds_A', 'beds_B', 'baths_A', 'baths_B']].to_csv(filename, header=False, index=False, mode='w')
            else:
                print ('not enough data to write file')

    def write_conv_data_to_png(self):
        '''
        read the month csv produced by write_month_to_csv() and call the c++ program (gausify) to make the png files
        '''
        #make sure directory exists
        conv_base_dir = self.base_dir + '/' + self.month_dir + '/' + self.location + '/' + self.target_name + '/' + self.target_price + '/' + self.calc_specific_dir
        if not os.path.exists(conv_base_dir):
            os.makedirs(conv_base_dir)

        #list of input filenames
        date_base_dir = self.base_dir + '/' + self.month_dir + '/' + self.location + '/' + self.target_name + '/' + self.target_price
        date_filenames = [f for f in os.listdir(date_base_dir) if os.path.isfile(os.path.join(date_base_dir, f))]
        date_filenames = ['%s/%s'%(date_base_dir,f) for f in date_filenames if 'csv' in f]

        print ("")
        print ("date filenames")
        for fn in date_filenames:
            print (fn)
        print ("")

        #make strings to call gausify
        run_its = []
        for date_filename in date_filenames:
            run_its.append('./gausify -l=%f,%f,%f,%f -g=%i,%i -t=%f,%f -s=%f -b=%i -c=%i -i=%i %s' % (self.min_lon, self.max_lon, self.min_lat, self.max_lat, self.grid_x, self.grid_y, self.min_target, self.max_target, self.sigma, self.sigma_blocks, self.distance_cutoff, self.highred, date_filename))

        print ('writing %i convolution png files ...' % (len(run_its)))

        if self.parallelize == True:
            cores = multiprocessing.cpu_count()
        else:
            cores = 1
        pool = multiprocessing.Pool(cores)
        pool.map(run_cpp_conv_png, run_its)
        pool.close()
        pool.join()

    '''
    HELPER FUNCTIONS
    '''
    def calc_buckets(self):
        '''
        figure out how to divide the values of the target (maping variable) to have 17
            different value ranges that will correspond to the 17 colors in the png
        '''
        self.bucket_devisions = []
        self.bucket_devisions.append(self.min_target)
        delta = abs(self.max_target-self.min_target) / 18.0
        starting = self.min_target + (delta / 2.0)
        for i in xrange(0,16):
            self.bucket_devisions.append(starting + delta * float(i))
        self.bucket_devisions.append(self.max_target)
        if self.highred == 0:
            self.bucket_devisions.reverse()

    def set_locs(self,df):
        '''
        set the min/max/mean longitude/latitude for the given location
        '''
        self.min_lon = df['lng'].min()
        self.max_lon = df['lng'].max()
        self.min_lat = df['lat'].min()
        self.max_lat = df['lat'].max()
        mean_lon_tmp = df['lng'].mode() # if no element occurs more than twice this can be empty
        mean_lat_tmp = df['lat'].mode() # if no element occurs more than twice this can be empty
        if mean_lon_tmp.empty:
            self.mean_lon = df['lng'].median()
        else:
            self.mean_lon = df['lng'].mode()[0]
        if mean_lat_tmp.empty:
            self.mean_lat = df['lat'].median()
        else:
            self.mean_lat = df['lat'].mode()[0]

    def set_dates(self,df):
        '''
        set the possible dates for the given location
        '''
        #make list of all dates by (year, month)
        start = df['listed_on_A'].min()
        end = df['sold_on_B'].max()
        start_year = start.year
        start_month = start.month
        end_year = end.year
        end_month = end.month
        self.dates = [(start_year, start_month)]
        if start_year == end_year:
            for month in xrange(start_month+1,end_month+1,1):
                self.dates.append((start_year,month))
        else:
            #year = start_year
            for month in xrange(start_month+1,12+1,1):
                self.dates.append((start_year,month))
            #years with all weeks
            for year in xrange(start_year+1,end_year,1):
                for month in xrange(1,12+1,1):
                    self.dates.append((year,month))
            #year = end_year
            for month in xrange(1,end_month+1,1):
                self.dates.append((end_year,month))
        #don't include first dates to account for average over months in self.month_ave
        self.dates = self.dates[self.month_ave-1:]

    def set_grid(self):
        '''
        set the size of the grid if grid_square=False
            - this was an attemp to get good proportions in the PNG
        '''
        #set calculation specific directory
        if not self.grid_square:
            if self.location == 'DC':
                self.grid_y = int( self.grid_x * ( abs(self.max_lat-self.min_lat) / abs(self.max_lon-self.min_lon) ) )
            elif self.location == 'DEN':
                self.grid_x = int( self.grid_y * ( abs(self.max_lon-self.min_lon) / abs(self.max_lat-self.min_lat) ) )


    def write_metadata(self, date_base_dir):
        '''
        writes the metadata for a given price range & target (mapping variable) pair
            - includes: bucket devisions, target max/min, min/max longitude/latitude
                    target name, which column holds the date information, location
                    name of the base directory
            - some of this infomation is read in by HS_app.py
        '''
        filename = date_base_dir + '-metadata.json'
        #check to see if file exists
        if not os.path.isfile(filename):
            with open(filename, 'w') as outf:
                outf.write(json.dumps({'bucket_devisions': self.bucket_devisions,
                                        'min_target': self.min_target,
                                        'max_target': self.max_target,
                                        'min_lon': self.min_lon,
                                        'max_lon': self.max_lon,
                                        'min_lat': self.min_lat,
                                        'max_lat': self.max_lat,
                                        'mean_lon': self.mean_lon,
                                        'mean_lat': self.mean_lat,
                                        'target_name': self.target_name,
                                        'date_column': self.date_column,
                                        'location': self.location,
                                        'base_dir': self.base_dir}))










if __name__ == '__main__':

    #full run
    map_prices = ['0_200000', '200000_400000', '400000_600000', '600000_800000', '800000_1000000', '1000000_plus', 'all']
    map_values = ['spread', 'hold_time', 'project_days', 'initial_days_to_contract', 'final_days_to_contract']
    months = [4,6,8]
    sigmabs = [1,2,3,5]

    #test run
    map_prices = ['400000_600000', '600000_800000']
    map_values = ['spread']
    months = [4]
    sigmabs = [2]

    #AWS
    base = '/home/ubuntu/moredata/toAWS/data'
    csv_full_dc = '/home/ubuntu/moredata/toAWS/data/dc_full_data.csv'
    csv_full_den = '/home/ubuntu/moredata/toAWS/data/den_full_data.csv'
    csv_flip_dc = '/home/ubuntu/moredata/toAWS/data/dc_flip_data.csv'
    csv_flip_den = '/home/ubuntu/moredata/toAWS/data/den_flip_data.csv'
    #local
    base = '/Users/victoriakortan/Desktop/HotSpots/data'
    csv_full_dc = '/Users/victoriakortan/Desktop/HotSpots/privy_data/dc_full_data.csv'
    csv_full_den = '/Users/victoriakortan/Desktop/HotSpots/privy_data/den_full_data.csv'
    csv_flip_dc = '/Users/victoriakortan/Desktop/HotSpots/privy_data/dc_flip_data.csv'
    csv_flip_den = '/Users/victoriakortan/Desktop/HotSpots/privy_data/den_flip_data.csv'

    for month in months:
        for sigmab in sigmabs:
            date_dir = 'month_ave_%i' % (month)
            for val in map_values:
                for price in map_prices:

                    #DEN
                    mls = MLS(csv_full_filename=csv_full_den,
                                csv_flip_filename=csv_flip_den, base_dir=base,
                                location='DEN', target_name=val, target_price=price,
                                grid_x=1800, grid_y=1800, grid_square=True, distance_cutoff=10,
                                sigma_blocks=sigmab, month_dir=date_dir, parallelize=True)
                    df = mls.load_flip()
                    mls.write_month_to_csv(df)
                    mls.write_conv_data_to_png()

                    #DC
                    mls = MLS(csv_full_filename=csv_full_dc,
                                csv_flip_filename=csv_flip_dc, base_dir=base,
                                location='DC', target_name=val, target_price=price,
                                grid_x=2000, grid_y=2000, grid_square=True, distance_cutoff=10,
                                sigma_blocks=sigmab, month_dir=date_dir, parallelize=True)
                    df = mls.load_flip()
                    mls.write_month_to_csv(df)
                    mls.write_conv_data_to_png()
