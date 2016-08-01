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
    start = time.time()
    os.system(call)
    end = time.time()
    print ('%.3f (sec)' % (end-start), end='')
    print ('')


class MLS:
    '''
    class for reading in and producing spatial maps of data
    '''

    def drop_spread_times(self, df):
        #keep only spread times that are < 2 years
        df = df[(df['sold_on_B'] - df['sold_on_A']).astype('timedelta64[D]') <= self.drop_spread_greater]
        #keep onlytimes that are > 2 weeks
        df = df[(df['sold_on_B'] - df['sold_on_A']).astype('timedelta64[D]') >= self.drop_spread_less]
        return df

    def drop_goofy_pairs(self, df):
        #keep only differences in listed_on_B and listed_on_A is > 2 weeks
        df = df[(df['listed_on_B'] - df['listed_on_A']).astype('timedelta64[D]') >= self.drop_spread_less]
        #keep only differences in contracted_on_A and listed_on_A is > 0
        df = df[(df['contracted_on_A'] - df['listed_on_A']).astype('timedelta64[D]') >= 0]
        #keep only differences in contracted_on_B and listed_on_B is > 0
        df = df[(df['contracted_on_B'] - df['listed_on_B']).astype('timedelta64[D]') >= 0]
        #keep only differences in Sold_on_A and contracted_on_A is > 0
        df = df[(df['sold_on_A'] - df['contracted_on_A']).astype('timedelta64[D]') >= 0]
        #keep only differences in Sold_on_B and contracted_on_B is > 0
        df = df[(df['sold_on_B'] - df['contracted_on_B']).astype('timedelta64[D]') >= 0]
        return df

    def get_price_range(self, df):
        #keep only houses with sold_price_A in the selected range
        df = df[ (df['sold_price_A'] >= self.price_dict[self.target_price][0]) & (df['sold_price_A'] <= self.price_dict[self.target_price][1])]
        return df

    def calc_spread(self, df):
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
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['sold_on_B'] - df['sold_on_A']).astype('timedelta64[D]')
        self.date_column = 'sold_on_B'
        return df

    def calc_project_days(self, df):
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['listed_on_B'] - df['sold_on_A']).astype('timedelta64[D]')
        self.date_column = 'listed_on_B'
        return df

    def calc_initial_days_to_contract(self, df):
        df = df.dropna(subset = ['contracted_on_A'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['contracted_on_A'] - df['listed_on_A']).astype('timedelta64[D]')
        self.date_column = 'contracted_on_A'
        return df

    def calc_initial_days_contract_to_sale(self, df):
        df = df.dropna(subset = ['contracted_on_A'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['sold_on_A'] - df['contracted_on_A']).astype('timedelta64[D]')
        self.date_column = 'sold_on_A'
        return df

    def calc_final_days_to_contract(self, df):
        df = df.dropna(subset = ['listed_on_B', 'contracted_on_B'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['contracted_on_B'] - df['listed_on_B']).astype('timedelta64[D]')
        self.date_column = 'contracted_on_B'
        return df

    def calc_final_days_contract_to_sale(self, df):
        df2 = df.dropna(subset = ['contracted_on_B'])
        df = self.drop_spread_times(df)
        df[self.target_name] = (df['sold_on_B'] - df['contracted_on_B']).astype('timedelta64[D]')
        self.date_column = 'sold_on_B'
        return df


    def __init__(self, csv_flip_filename='/Users/victoriakortan/Desktop/Galvenize/capstone/privy/data/mrislistings_short.csv',
                 base_dir='/Users/victoriakortan/Desktop/HotSpots/data', location='DC', target_name='', distance_cutoff=10,
                 grid_x=1200, grid_y=1200, grid_square=False, sigma_blocks=10, date_data_dir='date_data', parallelize=True,
                 target_price='all'):

        #define with initialization
        self.csv_flip_filename = csv_flip_filename
        self.base_dir = base_dir
        self.date_data_dir = date_data_dir
        self.location = location
        self.target_name = target_name
        self.grid_x = grid_x
        self.grid_y = grid_y
        self.grid_square = grid_square
        self.sigma_blocks = sigma_blocks
        self.parallelize = parallelize
        self.target_price=target_price
        self.distance_cutoff=distance_cutoff

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

        #calc sigma (units of lat/lon)
        #city block is ~ 1/16 of a mile, 1 degree longitude = 69.172 miles, 1 degree latitude = cos (lat) * 69.172
        #roughly call 1 degree = 60 miles, so one block is (1/16)/60 = ~ 0.001
        self.sigma = 0.001 * self.sigma_blocks

        #arbirarily define now
        self.min_data_points = 0 #if timeframe doesn't have more than min_data_points points, drop it
        self.drop_spread_greater = 730 #if spread is larger than 2 years drop (in days)
        self.drop_spread_less = 14 #if spread is less than 2 weeks drop (in days)
        self.month_ave = 4 #every month's data includes a window of the past 4 months

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
        # self.devisions ?


    def load_mls(self):
        '''
        '''
        print ('loading data...')
        all_date_columns = ['listed_on_A', 'contracted_on_A', 'sold_on_A', 'listed_on_B', 'contracted_on_B', 'sold_on_B']

        #load csv
        df = pd.read_csv(self.csv_flip_filename, parse_dates=all_date_columns, infer_datetime_format=True)

        #git rid of goofy values
        df = df.dropna(subset=all_date_columns)
        df = self.drop_spread_times(df)
        df = self.drop_goofy_pairs(df)

        #use selected target price range
        df = self.get_price_range(df)

        #call function to calculate target variable, and set self.date_column
        df = self.target_dict[self.target_name](df)
        df = df[[self.target_name, 'lng', 'lat', self.date_column, 'sold_price_A']]

        #make list of all dates by (year, month)
        start = df[self.date_column].min()
        end = df[self.date_column].max()
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

        #set location variables
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

        #set calculation specific directory
        if not self.grid_square:
            if self.location == 'DC':
                self.grid_y = int( self.grid_x * ( abs(self.max_lat-self.min_lat) / abs(self.max_lon-self.min_lon) ) )
            elif self.location == 'DEN':
                self.grid_y = int( self.grid_y * ( abs(self.max_lon-self.min_lon) / abs(self.max_lat-self.min_lat) ) )
        self.calc_specific_dir = str(self.grid_x) + 'X' + str(self.grid_y) + '_' + 'sigmablocks' + '_' + str(self.sigma_blocks)

        return df


    def write_date_data_to_csv(self, df):
        '''
        '''
        print ('writing date csv files ...')

        #make sure directory exists
        date_base_dir = self.base_dir + '/' + self.date_data_dir + '/' + self.location + '/' + self.target_name + '/' + self.target_price
        if not os.path.exists(date_base_dir):
            os.makedirs(date_base_dir)

        #write metatdata for this calculation
        #metadata base_dir
        metadata_base_dir = self.base_dir + '/' + self.date_data_dir + '/' + self.location + '/' + self.target_name + '-' + self.target_price
        #min max target valuese:
        self.min_target = df[self.target_name].min()
        self.max_target = df[self.target_name].max()
        #buckets:
        self.calc_buckets()
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

            #only use data in an 4 month period previous to the current date being considered
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
                    df2[[self.target_name, 'lng', 'lat', 'sold_price_A']].to_csv(filename, header=False, index=False, mode='w')
            else:
                print ('not enough data to write file')


    def write_conv_data_to_png(self):
        '''
        '''
        #make sure directory exists
        conv_base_dir = self.base_dir + '/' + self.date_data_dir + '/' + self.location + '/' + self.target_name + '/' + self.target_price + '/' + self.calc_specific_dir
        if not os.path.exists(conv_base_dir):
            os.makedirs(conv_base_dir)

        #list of input filenames
        date_base_dir = self.base_dir + '/' + self.date_data_dir + '/' + self.location + '/' + self.target_name + '/' + self.target_price
        date_filenames = [f for f in os.listdir(date_base_dir) if os.path.isfile(os.path.join(date_base_dir, f))]
        date_filenames = ['%s/%s'%(date_base_dir,f) for f in date_filenames if 'csv' in f]

        #make strings to call gausify
        run_its = []
        for date_filename in date_filenames:
            run_its.append('./gausify -l=%f,%f,%f,%f -g=%i,%i -t=%f,%f -s=%f -b=%i -c=%i %s' % (self.min_lon, self.max_lon, self.min_lat, self.max_lat, self.grid_x, self.grid_y, self.min_target, self.max_target, self.sigma, self.sigma_blocks, self.distance_cutoff, date_filename))

        print ('writing %i convolution png files ...' % (len(run_its)))

        if self.parallelize == True:
            cores = multiprocessing.cpu_count()
        else:
            cores = 1
        pool = multiprocessing.Pool(cores)
        pool.map(run_cpp_conv_png, run_its)


    def calc_buckets(self):
        self.bucket_devisions = []
        self.bucket_devisions.append(self.min_target)
        delta = abs(self.max_target-self.min_target) / 16.0
        starting = self.min_target + (delta / 2.0)
        for i in xrange(0,16):
            self.bucket_devisions.append(starting + delta * float(i))
        self.bucket_devisions.append(self.max_target)


    def write_metadata(self, date_base_dir):
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

    map_prices = ['0_200000', '200000_400000', '400000_600000', '600000_800000', '800000_1000000', '1000000_plus', 'all']
    map_values = ['spread', 'hold_time', 'project_days', 'initial_days_to_contract', 'final_days_to_contract']

    for val in map_values:
        for price in map_prices:

            #DC
            mls = MLS(csv_flip_filename='/Users/victoriakortan/Desktop/HotSpots/privy_data/den_flip_data.csv',
                        base_dir='/Users/victoriakortan/Desktop/HotSpots/data',
                        location='DEN', target_name=val, target_price=price,
                        grid_x=1800, grid_y=1800, grid_square=False, distance_cutoff=10,
                        sigma_blocks=2, date_data_dir='date_data', parallelize=True)

            df = mls.load_mls()
            mls.write_date_data_to_csv(df)
            mls.write_conv_data_to_png()

            #DEN
            mls = MLS(csv_flip_filename='/Users/victoriakortan/Desktop/HotSpots/privy_data/dc_flip_data.csv',
                      base_dir='/Users/victoriakortan/Desktop/HotSpots/data',
                      location='DC', target_name=val, target_price=price,
                      grid_x=1600, grid_y=1600, grid_square=False, distance_cutoff=10,
                      sigma_blocks=2, date_data_dir='date_data', parallelize=True)

            df = mls.load_mls()
            mls.write_date_data_to_csv(df)
            mls.write_conv_data_to_png()
