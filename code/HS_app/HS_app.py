from flask import Flask, request, url_for, render_template, send_file, jsonify
app = Flask(__name__)
import json
import os
import re

###################
## HELPER FUNCTIONS
###################
def set_APIkey():
    #API key
    with open('GMkey.rtf') as f:
        key = f.readlines()
    return 'https://maps.googleapis.com/maps/api/js?key=' + key[0].split(':')[1]

def read_matadata(dict_dc, dict_den, dict_base_filename):
    #read in json file-names, parse to get nested dictionary for each location:
        #dict_DC['target_date'] = {min_lon: , max_lon: ...}
        #need to add dates available, map_names_available
    json_files_dc = [f for f in os.listdir(dict_base_filename['DC']) if os.path.isfile(os.path.join(dict_base_filename['DC'], f)) and "metadata.json" in f]
    json_files_den = [f for f in os.listdir(dict_base_filename['DEN']) if os.path.isfile(os.path.join(dict_base_filename['DEN'], f)) and "metadata.json" in f]
    #read in json
    for f in json_files_dc:
        for value in map_values:
            for price in map_prices:
                k = "-".join([value,price])
                if k in f:
                    file_dc = dict_base_filename['DC'] + '/' + f
                    with open(file_dc) as json_data:
                        dict_dc[k] = json.load(json_data)
                        json_data.close()
    for f in json_files_den:
        for value in map_values:
            for price in map_prices:
                k = "-".join([value,price])
                if k in f:
                    file_den = dict_base_filename['DEN'] + '/' + f
                    with open(file_den) as json_data:
                        dict_den[k] = json.load(json_data)
                        json_data.close()

def get_mapnames(map_values, map_prices, dict_dc, dict_den):
    #get possible map names
    for value in map_values:
        for price in map_prices:
            k = "-".join([value,price])
            map_dir_dc = dict_base_filename['DC'] + '/' + value + '/' + price + '/' + dict_calc_specific_filename['DC']
            dict_dc[k]['map_names'] = [f for f in os.listdir(map_dir_dc) if os.path.isfile(os.path.join(map_dir_dc, f)) and "png" in f]
            map_dir_den = dict_base_filename['DEN'] + '/' + value + '/' + price + '/' + dict_calc_specific_filename['DEN']
            dict_den[k]['map_names'] = [f for f in os.listdir(map_dir_den) if os.path.isfile(os.path.join(map_dir_den, f)) and "png" in f]


###################
## APP
###################
# Welcome page
@app.route('/')
def index():
    return render_template("index.html")

# contact page
@app.route('/contact')
def contact():
    return render_template("index-contact.html")

# slides page
@app.route('/slides')
def slides():
    return render_template("index-slides.html")

# DC maps
@app.route('/DC')
def dc_maps():
    #these are default values, can be changed in app
    k = "-".join([map_values[0], map_prices[0]])
    #get list of dates
    filename = dict_base_filename['DC'] + '/' + map_values[0] + '/' + map_prices[0] + '/' + dict_calc_specific_filename['DC']
    dates = [f for f in os.listdir(filename) if os.path.isfile(os.path.join(filename, f)) and ".png" in f]
    dates = [f.split('.')[0] for f in dates]
    #send
    return render_template("map.html", map_names=dict_dc[k]['map_names'], variable=map_values[0], price=map_prices[0], min_lon=dict_dc[k]['min_lon'], max_lon=dict_dc[k]['max_lon'], min_lat=dict_dc[k]['min_lat'], max_lat=dict_dc[k]['max_lat'], buck=dict_dc[k]['bucket_devisions'], loc=dict_dc[k]['location'], dates=dates, api_key=api_key)

# DEN maps
@app.route('/DEN')
def den_maps():
    #these are default values, can be changed in app
    k = "-".join([map_values[0], map_prices[0]])
    #get list of dates
    filename = dict_base_filename['DEN'] + '/' + map_values[0] + '/' + map_prices[0] + '/' + dict_calc_specific_filename['DEN']
    dates = [f for f in os.listdir(filename) if os.path.isfile(os.path.join(filename, f)) and ".png" in f]
    dates = [f.split('.')[0] for f in dates]
    #send
    return render_template("map.html", map_names=dict_den[k]['map_names'], variable=map_values[0], price=map_prices[0], min_lon=dict_den[k]['min_lon'], max_lon=dict_den[k]['max_lon'], min_lat=dict_den[k]['min_lat'], max_lat=dict_den[k]['max_lat'], buck=dict_den[k]['bucket_devisions'], loc=dict_den[k]['location'], dates=dates, api_key=api_key)

# map images
@app.route('/get_map')
def get_map():
    loc = request.args.get('loc')
    var = request.args.get('map_var')
    pri = request.args.get('map_pri')
    map_name = request.args.get('name')
    filename = '%s/%s/%s/%s/%s.png' % (dict_base_filename[loc], var, pri, dict_calc_specific_filename[loc], map_name)

    return send_file(filename, mimetype='image/png')

# update buckets
@app.route('/update_buckets')
def get_buckets():
    loc = request.args.get('loc')
    var = request.args.get('map_var')
    pri = request.args.get('map_pri')
    k = '%s-%s' % (var,pri)
    if loc=='DC':
        buckets=dict_dc[k]['bucket_devisions']
    elif loc=='DEN':
        buckets=dict_den[k]['bucket_devisions']

    buckets = sorted(buckets, reverse=True)
    return json.dumps(buckets)

# get markers
@app.route('/get_markers')
def get_markers():
    loc = request.args.get('loc')
    var = request.args.get('map_var')
    pri = request.args.get('map_pri')
    map_name = request.args.get('name')
    if loc=='DC':
        filename = dict_base_filename[loc]
    elif loc=='DEN':
        filename = dict_base_filename[loc]
    filename += '/' + str(var) + '/' + str(pri) + '/' + map_name + '.csv'

    with open(filename) as f:
        cnt = 0
        markers_dict = {}
        for line in f:
            if line != '':
                marker = line.split(',')
                marker = [char.strip() for char in marker]
                markers_dict[cnt] = {"var": marker[0], "lon": marker[1], "lat": marker[2], "priceA": marker[3], "bedsA": marker[4], "bedsB": marker[5], "bathsA": marker[6], "bathsB": marker[7], "change_finished_sqft": marker[8]}
            cnt += 1
    return json.dumps(markers_dict)




###################
## MAIN
###################
if __name__ == '__main__':
    #pre-set variables
    month_ave = 4
    s = 2
    grid_dc = '1000X648'
    grid_den = '799X1000'
    #current values that can be mapped
    map_values = ['spread', 'hold_time', 'project_days', 'initial_days_to_contract', 'final_days_to_contract']
    map_values = ['spread']
    #current price points that can be mapped
    map_prices = ['all', '0_200000', '200000_400000', '400000_600000', '600000_800000', '800000_1000000', '1000000_plus']
    map_prices = ['400000_600000', '600000_800000']
    #have two base dirs
    dict_base_filename = {}
    #local
    dict_base_filename['DC'] = '/Users/victoriakortan/Desktop/HotSpots/data/month_ave_%s/DC' % (month_ave)
    dict_base_filename['DEN'] = '/Users/victoriakortan/Desktop/HotSpots/data/month_ave_%s/DEN' % (month_ave)
    #AWS
    #dict_base_filename['DC'] = '/home/ubuntu/moredata/data/month_ave_%s/DC' % (month_ave)
    #dict_base_filename['DEN'] = '/home/ubuntu/moredata/data/month_ave_%s/DEN' % (month_ave)


    #current grid choice
    dict_calc_specific_filename = {}
    dict_calc_specific_filename['DC'] = '%s_sigmablocks_%i' % (grid_dc, s)
    dict_calc_specific_filename['DEN'] = '%s_sigmablocks_%i' % (grid_den, s)
    #set google maps API key
    api_key = set_APIkey()
    #read-in metat data
    dict_dc = {}
    dict_den = {}
    read_matadata(dict_dc, dict_den, dict_base_filename)
    #set map names (combination of varaible and price bracket)
    get_mapnames(map_values, map_prices, dict_dc, dict_den)

    #WHEN PUT ON AWS DEBUG=FALSE
    app.run(host='0.0.0.0', port=8000, debug=True)
