from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash, Response
from flask_login import LoginManager, UserMixin, login_user, login_required,logout_user
from sqlalchemy import create_engine
import pandas as pd
import datetime, json
from datetime import datetime, timedelta
import numpy as np
import os,sys
import json
import re
import requests
from airport_codes import *
app = Flask(__name__)
LOCAL_DB_URI = "mysql+mysqldb://root:root@localhost/Searching_mca"

@app.route('/dashboard/getCompanyListData', methods=['POST'])
def post_report():
    #drop_list = dropdown_list()
    params = {}
    params['from'] = request.form.get('cmp_list')
    params['to'] = request.form.get('code_list')
    params['depart_date'] = request.form.get('date')
    flights_info = report_format(params)
    fin_op = {'flights_info' : flights_info}
    return fin_op
@app.route('/dashboard/details', methods=['GET'])
def get_details():
    return render_template('booking.html')

@app.route('/dashboard', methods=['GET'])
def report():
    drop_list = dropdown_list()
    params = {}
    #resp = report_format(params)
    resp = {}
    return render_template('mca_report.html', code=drop_list, table_data=resp)

def dropdown_list():
    data = [{airport : airport_list[airport]} for airport in airport_list.keys()]
    return data

def report_format(params):
    if params:
        departures = params['from']
        arrivals = params['to']
        depart_date = params['depart_date']
        departures = re.search(r'\((.*?)\)',departures).group(1)
        payload = { "airlines": [ "AA" ], "arrivals": [arrivals], "cabins": [ "ECONOMY", "FIRST_CLASS" ], "departure_date": { "flexibility": 0, "when": depart_date }, "departures": [departures], "currencies": [ "USD" ], "max_stops": 3, "passengers": 1 }
        headers = {'content-type': 'application/json'}
        response = requests.post('http://95.217.60.158/api/miles', headers = headers, data=json.dumps(payload))
        result = response.json()
        if result: 
            li = []
            # import pdb;pdb.set_trace()
            # data = [{'departure': 'LAX', 'departure_time': '2020-09-15T17:20:00.000-07:00', 'arrival_time': '2020-09-16T12:05:00.000+01:00', 'cabin': 'Economy', 'arrival': 'LHR', 'airline': 'AA'}]
            for i in range(len(result['routes'])):
                df1 = {}
                df1['miles'] = result['routes'][i]['redemptions'][0]['miles']
                df1['program'] = result['routes'][i]['redemptions'][0]['program']
                df1['num_stops'] = result['routes'][i]['num_stops']
                df1['taxes'] = result['routes'][i]['payments'][0]['taxes']
                #for j in range(len(result['routes'][i]['connections'])):
                conn = result['routes'][i]['connections'][0]
                df1['airline'] = conn['airline']
                df1['flight'] = conn['flight']
                df1['cabin'] = conn['cabin']
                df1['departure'] = conn['departure']['airport']
                df1['departure_time'] = conn['departure']['when']
                df1['arrival'] = conn['arrival']['airport']
                df1['arrival_time'] = conn['arrival']['when']
                df1['aircraft_model'] = conn['aircraft']['manufacturer'] + '(' +conn['aircraft']['model']+')'
                # df1['link'] = '<a href="#" id="myBtn">Flights & Fare Details</a>'
                df1['link'] =  '<button  href="booking.html" onclick="booking.html" target="booking.html" class="themeBtn" id="themeBtn">Book Now</button>'
                li.append(df1)
            print(li)
            return li

def logic(params, column):
    val = json.loads(params)
    return val.get(column,'')
# 

def json_error_response(msg, status=None):
    data = {'error': msg}
    status = status if status else 500
    return Response(
        json.dumps(data), status=status, mimetype="application/json")

