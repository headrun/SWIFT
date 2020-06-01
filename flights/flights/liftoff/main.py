from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash, Response
from flask_login import LoginManager, UserMixin, login_user, login_required,logout_user
from sqlalchemy import create_engine
import pandas as pd
import datetime, json
from datetime import datetime, timedelta
import numpy as np
import os,sys
import json
import re, ast
import requests
from airport_codes import *
app = Flask(__name__)
LOCAL_DB_URI = "mysql+mysqldb://root:root@localhost/Searching_mca"

@app.route('/', methods=['GET','POST'])
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
    with open('airport_codes.txt', 'r') as _file:
        airport_list = json.loads(_file.read())
        data = [{airport : airport_list[airport]} for airport in airport_list.keys()]
        return data 

def report_format(params):
    if params:
        departures = params['from']
        arrivals = params['to']
        depart_date = params['depart_date']
        airline_list = ["AA","VS","DL","UA","AF","AC"]
        li = []
        final_data = []
        for air in airline_list:
            payload = { "airlines": [air], "arrivals": [arrivals], "cabins": [ "ECONOMY", "FIRST_CLASS" ], "departure_date": { "flexibility": 0, "when": depart_date }, "departures": [departures], "currencies": [ "USD" ], "max_stops": 10, "passengers": 1 }
            headers = {'content-type': 'application/json'}
            print(payload)
            response = requests.post('http://95.217.60.158/api/miles', headers = headers, data=json.dumps(payload))
            try: result = response.json()
            except: pass
            if 'routes' in result.keys():
                for i in range(len(result['routes'])):
                    df1 = {}
                    df1['miles'] = result['routes'][i]['redemptions'][0]['miles']
                    df1['program'] = result['routes'][i]['redemptions'][0]['program']
                    df1['num_stops'] = result['routes'][i]['num_stops']
                    df1['taxes'] = result['routes'][i]['payments'][0]['taxes']
                    for j in range(df1['num_stops']+1):
                        conn = result['routes'][i]['connections'][j]
                        df1['airline_'+'{0}'.format(j)] =  conn['airline']
                        df1['flight_'+'{0}'.format(j)] = conn['flight'][0]
                        df1['cabin_'+'{0}'.format(j)] = conn['cabin']
                        df1['departure_'+'{0}'.format(j)] = conn['departure']['airport']
                        departure_time = conn['departure']['when']
                        dept = datetime.strptime(departure_time[:16], "%Y-%m-%dT%H:%M")
                        df1['departure_time_'+'{0}'.format(j)] = dept.strftime("%H:%M, %d %B, %Y")
                        df1['arrival_'+'{0}'.format(j)] = conn['arrival']['airport']
                        arrival_time = conn['arrival']['when']
                        arr = datetime.strptime(arrival_time[:16], "%Y-%m-%dT%H:%M")
                        df1['arrival_time_'+'{0}'.format(j)] = arr.strftime("%H:%M, %d %B, %Y")
                        df1['aircraft_model_'+'{0}'.format(j)] = conn['aircraft']['manufacturer'] + '(' +conn['aircraft']['model']+')'
                        df1 = {k:str(v) for k,v in df1.items() }
                        li.append(df1)
        print(len(li))
        final_data = [ast.literal_eval(el1) for el1 in set([str(el2) for el2 in li])]
        print(len(final_data))
        return final_data
