from flask import Flask, request, url_for, render_template, send_file, jsonify
app = Flask(__name__)
import json
import os
import re


#helper vars/functions
monthN_to_monthS_dict = {'01': 'January',
                        '02': 'February',
                        '03': 'March',
                        '04': 'April',
                        '05': 'May',
                        '06': 'June',
                        '07': 'July',
                        '08': 'August',
                        '09': 'September',
                        '10': 'October',
                        '11': 'November',
                        '12': 'December'
                        }
monthS_to_monthN_dict = {'January': '01',
                        'February': '02',
                        'March': '03',
                        'April': '04',
                        'May': '05',
                        'June': '06',
                        'July': '07',
                        'August': '08',
                        'September': '09',
                        'October': '10',
                        'November': '11',
                        'December': '12'
                        }
def showDatesS(d_str):
    dates = []
    for d in d_str:
        dates.append( '%s %s' % ( d.split('_')[0], monthN_to_monthS_dict[d.split('_')[1]] ) )
    return dates
def showDatesN(d_str):
    dates = []
    for d in d_str:
        dates.append( '%s_%s' % ( d.split('_')[0], monthS_to_monthN_dict[d.split('_')[1]] ) )
    return dates

#pre-set variables
month_ave = 8
s = 2
#have two base dir open ie /Users/victoriakortan/Desktop/HotSpots/data/date_data/DC/ and /Users/victoriakortan/Desktop/HotSpots/data/date_data/DEN
dict_base_filename = {}
#local
#dict_base_filename['DC'] = '/Users/victoriakortan/Desktop/HotSpots/data/month_ave_%s/DC' % (month_ave)
#dict_base_filename['DEN'] = '/Users/victoriakortan/Desktop/HotSpots/data/month_ave_%s/DEN' % (month_ave)
#AWS
dict_base_filename['DC'] = '/home/ubuntu/moredata/data/month_ave_%s/DC' % (month_ave)
dict_base_filename['DEN'] = '/home/ubuntu/moredata/data/month_ave_%s/DEN' % (month_ave)
#current values that can be mapped
map_values = ['spread', 'hold_time', 'project_days', 'initial_days_to_contract', 'final_days_to_contract']
#current price points that can be mapped
map_prices = ['all', '0_200000', '200000_400000', '400000_600000', '600000_800000', '800000_1000000', '1000000_plus']
#current grid choice
dict_calc_specific_filename = {}
dict_calc_specific_filename['DC'] = '2000X2000_sigmablocks_%i' % (s)
dict_calc_specific_filename['DEN'] = '1800X1800_sigmablocks_%i' % (s)


#read in json file-names, parse to get nested dictionary for each location:
    #dict_DC['target_date'] = {min_lon: , max_lon: ...}
    #need to add dates available, map_names_available
json_files_dc = [f for f in os.listdir(dict_base_filename['DC']) if os.path.isfile(os.path.join(dict_base_filename['DC'], f)) and "metadata.json" in f]
json_files_den = [f for f in os.listdir(dict_base_filename['DEN']) if os.path.isfile(os.path.join(dict_base_filename['DEN'], f)) and "metadata.json" in f]
dict_dc = {}
dict_den = {}
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

#get possible map names
for value in map_values:
    for price in map_prices:
        k = "-".join([value,price])
        map_dir_dc = dict_base_filename['DC'] + '/' + value + '/' + price + '/' + dict_calc_specific_filename['DC']
        dict_dc[k]['map_names'] = [f for f in os.listdir(map_dir_dc) if os.path.isfile(os.path.join(map_dir_dc, f)) and "png" in f]
        map_dir_den = dict_base_filename['DEN'] + '/' + value + '/' + price + '/' + dict_calc_specific_filename['DEN']
        dict_den[k]['map_names'] = [f for f in os.listdir(map_dir_den) if os.path.isfile(os.path.join(map_dir_den, f)) and "png" in f]




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
    dates = showDatesS(dates)
    #send
    return render_template("map.html", map_names=dict_dc[k]['map_names'], variable=map_values[0], price=map_prices[0], min_lon=dict_dc[k]['min_lon'], max_lon=dict_dc[k]['max_lon'], min_lat=dict_dc[k]['min_lat'], max_lat=dict_dc[k]['max_lat'], buck=dict_dc[k]['bucket_devisions'], loc=dict_dc[k]['location'], dates=dates)

# DEN maps
@app.route('/DEN')
def den_maps():
    #these are default values, can be changed in app
    k = "-".join([map_values[0], map_prices[0]])
    #get list of dates
    filename = dict_base_filename['DEN'] + '/' + map_values[0] + '/' + map_prices[0] + '/' + dict_calc_specific_filename['DEN']
    dates = [f for f in os.listdir(filename) if os.path.isfile(os.path.join(filename, f)) and ".png" in f]
    dates = [f.split('.')[0] for f in dates]
    dates = showDatesS(dates)
    #send
    return render_template("map.html", map_names=dict_den[k]['map_names'], variable=map_values[0], price=map_prices[0], min_lon=dict_den[k]['min_lon'], max_lon=dict_den[k]['max_lon'], min_lat=dict_den[k]['min_lat'], max_lat=dict_den[k]['max_lat'], buck=dict_den[k]['bucket_devisions'], loc=dict_den[k]['location'], dates=dates)

# map images
@app.route('/get_map')
def get_map():
    loc = request.args.get('loc')
    var = request.args.get('map_var')
    pri = request.args.get('map_pri')
    map_name = request.args.get('name')
    map_name = showDatesN([map_name])[0]
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
    map_name = showDatesN([map_name])[0]
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
                markers_dict[cnt] = {"var": marker[0], "lon": marker[1], "lat": marker[2], "priceA": marker[3]}
            cnt += 1
    return json.dumps(markers_dict)

# get dates
@app.route('/get_dates')
def get_dates():
    loc = request.args.get('loc')
    var = request.args.get('map_var')
    pri = request.args.get('map_pri')
    if loc=='DC':
        filename = dict_base_filename[loc]
    elif loc=='DEN':
        filename = dict_base_filename[loc]
    filename += '/' + str(var) + '/' + str(pri) + '/' + dict_calc_specific_filename[loc]

    dates = [f for f in os.listdir(filename) if os.path.isfile(os.path.join(filename, f)) and ".png" in f]
    dates = [f.split('.')[0] for f in dates]
    dates = showDatesS(dates)
    dates_dict = {}
    for i in xrange(0,len(dates)):
        dates_dict[i] = dates[i]
    return json.dumps(dates_dict)





if __name__ == '__main__':


    #WHEN PUT ON AWS DEBUG=FALSE
    app.run(host='0.0.0.0', port=8000, debug=False)
