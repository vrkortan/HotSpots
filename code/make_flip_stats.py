from __future__ import print_function
import pandas as pd
import MLS



mls = MLS.MLS(csv_filename='/Users/victoriakortan/Desktop/Galvenize/capstone/privy/data/dc_data.csv',
              base_dir='/Users/victoriakortan/Desktop/HotSpots/data',
              location='DC', target_name='',
              grid_x=1200, grid_y=1200, grid_square=False,
              sigma_blocks=10, date_data_dir='date_data', parallelize=True)

all_date_columns = ['created_at', 'updated_at', 'listed_on', 'contracted_on', 'sold_on']

#load csv
df = pd.read_csv(mls.csv_filename, parse_dates=all_date_columns, infer_datetime_format=True)

#drop rows with no date in 'sold_on'
df = df.dropna(subset=['sold_on'])

#drop rows with dates > today
for col in all_date_columns:
    df = df.drop(df[df[col] > pd.to_datetime('today')].index)

#drop null columns
null_columns = ['stories', 'version', 'parcel_number', 'hoa_name', 'hoa_fee', 'school_district', 'private_remarks',
                'showings_phone', 'approval_condition', 'lot_size_acres', 'basement_finished_status', 'architecture',
                'previous_price', 'off_market_on', 'listing_agent_mls_id', 'listing_agent_email', 'listing_agent_phone']
df = df.drop(null_columns, axis=1)

#drop less than useful columns
lessuseful_columns = ['externally_last_updated_at', 'photos', 'listing_agent', 'listing_brokerage', 'selling_brokerage_name',
                      'selling_agent_mls_id', 'selling_agent_phone', 'seller_concessions', 'property_type']
df = df.drop(lessuseful_columns, axis=1)

#take care of wrong lon/lat point in DC data
if (df.shape[0] > 193519):
    if (mls.location == 'DC'):
        df = df.drop(df.index[193519])

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
flip_metrics = ['property_id', 'lat', 'lng', 'listed_on', 'contracted_on', 'sold_on', 'list_price', 'sold_price',
                'above_grade_square_feet', 'finished_square_feet', 'derived_basement_square_feet', 'garages',
                'beds', 'baths', 'zip', 'year_built', 'lot_size_square_feet', 'zoned', 'is_attached']
flip_metrics_dict = {}
cnt = 0
for m in flip_metrics:
    flip_metrics_dict[m] = cnt
    cnt += 1

df_flip_metrics = df[flip_metrics]
#order so last row is most recent sale
df_flip_metrics = df_flip_metrics.sort_values('sold_on')

flipped_metrics = [
                    #should be same for all enetries with the same property id
                    'property_id', 'lat', 'lng', 'zip',
                    #want to keep a copy for each side of sale, A=row, B=row+1
                    'listed_on_A', 'contracted_on_A', 'sold_on_A', 'list_price_A', 'sold_price_A',
                    'listed_on_B', 'contracted_on_B', 'sold_on_B', 'list_price_B', 'sold_price_B',
                    #just want change in value from B to A
                    'change_above_grade_square_feet', 'change_finished_square_feet',
                    'change_derived_basement_square_feet', 'change_garages', 'change_beds',
                    'change_baths', 'change_year_built', 'change_lot_size_square_feet',
                    #just want to know if change in value from B to A
                    'change_zoned_tf', 'change_is_attached_tf'
                   ]




print ("calculating flipped metrics ...")
df_flipped = pd.DataFrame(columns = flipped_metrics)

total_num = df_flip.shape[0]
cnt=1
for id in xrange(0, df_flip.shape[0]):
    print ('%i/%i' % (cnt,total_num))
    cnt += 1

    df_temp = df_flip_metrics.loc[ df['property_id'] == df_flip.iloc[id,0] ]

    df_row = []
    for row in xrange(0, df_temp.shape[0]-1):
        A = row
        B = row + 1

        #should be same for all enetries with the same property id
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['property_id'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['lat'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['lng'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['zip'] ] )

        #want to keep a copy for each side of sale, A=row, B=row+1
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['listed_on'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['contracted_on'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['sold_on'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['list_price'] ] )
        df_row.append( df_temp.iloc[ A, flip_metrics_dict['sold_price'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['listed_on'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['contracted_on'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['sold_on'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['list_price'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['sold_price'] ] )

        #just want change in value from B to A
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['above_grade_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['above_grade_square_feet'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['finished_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['finished_square_feet'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['derived_basement_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['derived_basement_square_feet'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['garages'] ] - df_temp.iloc[ A, flip_metrics_dict['garages'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['beds'] ] - df_temp.iloc[ A, flip_metrics_dict['beds'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['baths'] ] - df_temp.iloc[ A, flip_metrics_dict['baths'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['year_built'] ] - df_temp.iloc[ A, flip_metrics_dict['year_built'] ] )
        df_row.append( df_temp.iloc[ B, flip_metrics_dict['lot_size_square_feet'] ] - df_temp.iloc[ A, flip_metrics_dict['lot_size_square_feet'] ] )

        #just want to know if change in value from B to A
        if df_temp.iloc[ A, flip_metrics_dict['zoned'] ] == df_temp.iloc[ B, flip_metrics_dict['zoned'] ]:
            df_row.append(True)
        else:
            df_row.append(False)
        if df_temp.iloc[ A, flip_metrics_dict['is_attached'] ] == df_temp.iloc[ B, flip_metrics_dict['is_attached'] ]:
            df_row.append(True)
        else:
            df_row.append(False)

    df_flipped = df_flipped.append(df_row)
print ('')

print ("writting flipped metrics to csv...")
filename = '~/Desktop/df_flipped_test.csv'
df_flipped.to_csv(filename, header=True, index=False, mode='w')
