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

@app.route('/dashboard', methods=['GET','POST'])
def dashboard():
    drop_list = dropdown_list()
    params = {}
    if request.method == 'POST':
        params['from'] = request.form.get('cmp_list')
        params['to'] = request.form.get('code_list')
        params['depart_date'] = request.form.get('date')
        flights_info = report_format(params)
        if flights_info:
            return render_template('airmiles.html', code=drop_list, data=flights_info)
        else: 
            return render_template('airmiles.html', code=drop_list, error={"data":"No Routes Found !"}) 
    return render_template('airmiles.html', code=drop_list)

def dropdown_list():
    data = [{airport : airport_list[airport]} for airport in airport_list.keys()]
    return data

def report_format(params):
    if params:
        departures = params['from']
        arrivals = params['to']
        depart_date = params['depart_date']
        airline_list = ["AA","VS","DL","UA","AF"]
        li = []
        for air in airline_list:
            payload = { "airlines": [air], "arrivals": [arrivals], "cabins": [ "ECONOMY", "FIRST_CLASS" ], "departure_date": { "flexibility": 0, "when": depart_date }, "departures": [departures], "currencies": [ "USD" ], "max_stops": 3, "passengers": 1 }
            headers = {'content-type': 'application/json'}
            print(payload)
            response = requests.post('http://95.217.60.158/api/miles', headers = headers, data=json.dumps(payload))
            result = response.json()
            if 'routes' in result.keys():
                for i in range(len(result['routes'])):
                    df1 = {}
                    df1['miles'] = result['routes'][i]['redemptions'][0]['miles']
                    df1['program'] = result['routes'][i]['redemptions'][0]['program']
                    df1['num_stops'] = result['routes'][i]['num_stops']
                    df1['taxes'] = result['routes'][i]['payments'][0]['taxes']
                    if df1['num_stops'] == 0:
                        conn = result['routes'][i]['connections'][0]
                        df1['airline'] = conn['airline']
                        df1['flight'] = conn['flight'][0]
                        df1['cabin'] = conn['cabin']
                        df1['departure'] = conn['departure']['airport']
                        df1['departure_time'] = conn['departure']['when']
                        df1['arrival'] = conn['arrival']['airport']
                        df1['arrival_time'] = conn['arrival']['when']
                        df1['aircraft_model'] = conn['aircraft']['manufacturer'] + '(' +conn['aircraft']['model']+')'
                    elif df1['num_stops'] == 1:
                        conn = result['routes'][i]['connections'][0]
                        df1['airline'] = conn['airline']
                        df1['flight'] = conn['flight'][0]
                        df1['cabin'] = conn['cabin']
                        df1['departure'] = conn['departure']['airport']
                        df1['departure_time'] = conn['departure']['when']
                        df1['arrival'] = conn['arrival']['airport']
                        df1['arrival_time'] = conn['arrival']['when']
                        df1['aircraft_model'] = conn['aircraft']['manufacturer'] + '(' +conn['aircraft']['model']+')'
                        conn_2 = result['routes'][i]['connections'][1]
                        df1['airline2'] = conn_2['airline']
                        df1['flight2'] = conn_2['flight'][0]
                        df1['cabin2'] = conn_2['cabin']
                        df1['departure2'] = conn_2['departure']['airport']
                        df1['departure_time2'] = conn_2['departure']['when']
                        df1['arrival2'] = conn_2['arrival']['airport']
                        df1['arrival_time2'] = conn_2['arrival']['when']
                        df1['aircraft_model2'] = conn_2['aircraft']['manufacturer'] + '(' +conn_2['aircraft']['model']+')'
                    elif df1['num_stops'] == 2:
                        conn = result['routes'][i]['connections'][0]
                        df1['airline'] = conn['airline']
                        df1['flight'] = conn['flight'][0]
                        df1['cabin'] = conn['cabin']
                        df1['departure'] = conn['departure']['airport']
                        df1['departure_time'] = conn['departure']['when']
                        df1['arrival'] = conn['arrival']['airport']
                        df1['arrival_time'] = conn['arrival']['when']
                        df1['aircraft_model'] = conn['aircraft']['manufacturer'] + '(' +conn['aircraft']['model']+')'
                        conn_2 = result['routes'][i]['connections'][1]
                        df1['airline2'] = conn_2['airline']
                        df1['flight2'] = conn_2['flight'][0]
                        df1['cabin2'] = conn_2['cabin']
                        df1['departure2'] = conn_2['departure']['airport']
                        df1['departure_time2'] = conn_2['departure']['when']
                        df1['arrival2'] = conn_2['arrival']['airport']
                        df1['arrival_time2'] = conn_2['arrival']['when']
                        df1['aircraft_model2'] = conn_2['aircraft']['manufacturer'] + '(' +conn_2['aircraft']['model']+')'
                        conn_3 = result['routes'][i]['connections'][1]
                        df1['airline3'] = conn_3['airline']
                        df1['flight3'] = conn_3['flight'][0]
                        df1['cabin3'] = conn_3['cabin']
                        df1['departure3'] = conn_3['departure']['airport']
                        df1['departure_time3'] = conn_3['departure']['when']
                        df1['arrival3'] = conn_3['arrival']['airport']
                        df1['arrival_time3'] = conn_3['arrival']['when']
                        df1['aircraft_model3'] = conn_3['aircraft']['manufacturer'] + '(' +conn_3['aircraft']['model']+')'
                    df1 = {k:str(v) for k,v in df1.items() }
                    li.append(df1)
        # print(li)
        return li
