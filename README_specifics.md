# HotSpots

### Quick Start Specifics

#### directory structure for data
    - HotSpots
      -privy_data
        -full, flip data
      -data
        - month_ave_#
            - DEN, DC
              - target
                - metadata
                - price range
                  - month csv
                  - calc specific (grid_x X grid_y _ sigmablocks _ #)
                    - png files

#### MLS sql database to 'full' csv  
  - code/init_data.py  

#### 'full' csv to 'flip' csv  
  - done when initializing mls object
  - mls object has to be initialized with:
  ```python
  MLS(
        csv_full_filename=csv_full_den, #path the 'full' csv
        csv_flip_filename=csv_flip_den, #path to the already created, or where you want 'flip' csv
        base_dir=base, #path to the base directory for data
        location='DEN', #location for the maps, currently either DEN or DC
        target_name=val, #name of the variable to be mapped ('spread', 'hold_time', 'project_days', 'initial_days_to_contract', 'final_days_to_contract')
        target_price=price, #price range for the 1st sale ('0_200000', '200000_400000', '400000_600000', '600000_800000', '800000_1000000', '1000000_plus', 'all')
        grid_x=grid_max, #the pixel x-dimensions of the png
        grid_y=grid_max, #the pixel y-dimensions of the png
        grid_square=grid_sqare_tf, #if True don't change grid_x(y) to fit lat/lon proportions of location
        distance_cutoff=10, #where to cutoff the gaussian convolution, measured in units of sigma=0.001 * self.sigma_blocks
        sigma_blocks=sigmab, #how many approximate blocks to make sigma for the gaussian convolution, (sigma=0.001 * self.sigma_blocks)
        month_dir=date_dir, #month_ave_#, says how many past months to include in a given month's csv/png
        parallelize=True #weather or not to parallelize calls to gaussify.cpp
    )
  ```

#### 'flip' csv to 'month' csv  
  - done with:
    ```python
    df = mls.load_flip()
    mls.write_month_to_csv(df)
    ```
  - specific to mapping variable (target), price range, and date

#### 'month' csv to png
  - done with:
  ```python
  mls.write_conv_data_to_png()
  ```

#### app
  - all files in code/HS_app
  - main file: HS_app.py
    - have to first specify the variables that determine the path to the data:
    ```python
    #pre-set variables
    month_ave = 4
    s = 2
    grid_dc = '1000X648'
    grid_den = '799X1000'
    #current values that can be mapped
    map_values = ['spread', 'hold_time', 'project_days', 'initial_days_to_contract', 'final_days_to_contract']
    #current price points that can be mapped
    map_prices = ['all', '0_200000', '200000_400000', '400000_600000', '600000_800000', '800000_1000000', '1000000_plus']
    #base directory for each location
    dict_base_filename['DC'] = '/Users/victoriakortan/Desktop/HotSpots/data/month_ave_%s/DC' % (month_ave)
    dict_base_filename['DEN'] = '/Users/victoriakortan/Desktop/HotSpots/data/month_ave_%s/DEN' % (month_ave)
    ```
    - will also have to set set_APIkey() up, so the google maps API key can be read from HotSpots/code/HS_app/GMkey.rtf.  The rtf file should look like:
    ```text
    GMkey:key
    ```
  - main file refers to map template, HS_app/templates/map.html, which is where all the map overlaying and variable selecting takes place


### Method Specifics

#### MLS.py: initialization
  - will set variables for the object
  - will make the 'flip' stats if they do not already exist (df = self.load_full_csv(), self.make_flip_csv(df))
    - considers anytime a property_id is listed more than once
    - writes the necessary stats for the 1st sale (A) and the 2nd sale (B), as well as some of the differences between the two as new variables, e.x. change in number of bedrooms  

#### MLS.py: mls.load_flip()
  - loads the flip stats and removes any strange entries
  - sets the max/min lat/lon, available dates for the given target variable (set when MLS initialized)
  - calls function to calculate the target variable
  - sets max/min for target variable as well as the limits for the 17 different possible colors in the png (buckets)  

#### MLS.py: mls.write_month_to_csv(df)
  - writes the flip stats for a given date (month) to a csv file to be read in by gaussify.cpp, also used to set the text for the markers in the app
    - can choose to change min_data_points in the initialization function, this would produce a blank csv if the number of points for a given month were less than min_data_points.  The reason for the bank csv is so that the number of png files for a given location remains the same across all price ranges and target variables

#### MLS.py: mls.write_conv_data_to_png()
  - calls gausify.cpp (by seting up a string to pass to os.sys()) to write the png, based on my limited testing using c++ seemed to be faster than python for this

#### gaussify.cpp:
  - parses the command line options
  - for each grid point in the png, runs through the actual flip data points and if they are within the distance cutoff adds their value weighted by their distance to the grid point currently being calculated
  - based on the bucket values passed through the command line sets a color for each grid point, then writes the png


### NOTES
  - to change what is written in the marker text for each flip (in the app)
    - will have to change things in several places:
      - MLS.py
        - load_flip(), change what metrics to keep when loading file (search for 'change_finished_square_feet')
        - write_month_to_csv(), change what metrics are written to the 'date' csv (search for 'change_finished_square_feet')
      - gausify.cpp
        - where the month csv file is read in the only numbers gaussify needs are the target variable, latitude and longitude.  Any additional information has to be popped off the end of the vector (search for 'change_finished_square_feet')
      -  HS_app.py
        - get_markers(), where the dictionary is set up to hold the marker information read in from the month csv file
      - HS_app/templates/map.html
        - getMarkers(), where var txt created, this controls how the marker pop-up text is displayed
    - if you want to add something that isn't already written to the month csv file the more changes will be necessary when the moth csv is created
      - make_flip_csv()
        - makes a dictionary based off of all the flip stats we want to keep (var flip_metrics) from the 'full' csv file, the relates the name of the stat to the column number
        - the values that are currently written to the 'fip' csv file are
        ```python
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
        ```
        - these metrics are then calculated and appended onto a list in the order given in flipped_metrics for a given flip
        - all of the given flips are combined into one list and written to a csv file
