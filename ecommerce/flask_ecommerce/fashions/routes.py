from flask import render_template, session, request, redirect, url_for
from flask import jsonify
import json
import datetime
from fashions import app, db
from flask_mysqldb import MySQL
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'Ecomm@34^$'
app.config['MYSQL_DB'] = 'ECOMMERCEDB'

mysql = MySQL(app)

@app.route('/')
def home():
	cur = mysql.connection.cursor()
	today_date = str(datetime.datetime.now().date())
	cur.execute("SELECT t1.image_url,t1.title,t2.mrp,t2.selling_price,t2.brand FROM (SELECT * FROM products_info limit 50) t1 join products_insights t2 on t1.hd_id = t2.hd_id")
	# cur.execute("select * from products_info where created_at >= STR_TO_DATE('2013-07-11','%Y-%m-%d') and created_at <= STR_TO_DATE('2013-07-11','%Y-%m-%d') limit 1;")
	row_headers=[x[0] for x in cur.description]
	results = cur.fetchall()
	json_data=[]
	for result in results:
	    json_data.append(dict(zip(row_headers,result)))
	products = json.dumps(json_data)
	cur.close()
	return render_template('details.html',products=json.loads(products))

@app.route('/search')
def data():
	cur = mysql.connection.cursor()
	today_date = str(datetime.datetime.now().date())
	cur.execute("SELECT t1.image_url,t1.title,t2.mrp,t2.selling_price,t2.brand FROM (SELECT * FROM products_info limit 50) t1 join products_insights t2 on t1.hd_id = t2.hd_id")
	# cur.execute("select * from products_info where created_at >= STR_TO_DATE('2013-07-11','%Y-%m-%d') and created_at <= STR_TO_DATE('2013-07-11','%Y-%m-%d') limit 1;")
	row_headers=[x[0] for x in cur.description]
	results = cur.fetchall()
	json_data=[]
	for result in results:
		json_data.append(dict(zip(row_headers,result)))
	products = json.dumps(json_data)
	cur.close()
	return render_template('details.html',products=json.loads(products))

