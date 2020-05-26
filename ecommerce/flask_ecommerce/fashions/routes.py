import json
from flask import render_template, request
from flask_mysqldb import MySQL
from fashions import app

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'Ecomm@34^$'
app.config['MYSQL_DB'] = 'ECOMMERCEDB'

mysql = MySQL(app)


@app.route('/')
def home():
    return render_template('details.html')

@app.route('/search')
def data():
    start_date = request.args.get('startdate')
    end_date = request.args.get('enddate')
    source = request.args.get('source')
    sort_by = request.args.get('sortBy')
    page_num = request.args.get('page_num', 1)
    limit = 20
    offset = 0
    print("page_num, limit", page_num, limit)
    if(page_num == 0 or page_num == 1):
        offset = 0
    else:
        offset = (int(page_num)-1)*limit
    cur = mysql.connection.cursor()
    if sort_by == "popularity":
    	if source == "all":
    		cur.execute("select DISTINCT t1.hd_id,t2.brand,t2.ratings_count,t2.source,t2.selling_price,t2.mrp,t1.image_url,t1.title from products_insights t2,products_info t1 where t1.hd_id = t2.hd_id and t1.created_at >= STR_TO_DATE(%s,'%%Y-%%m-%%d') and t1.created_at <= STR_TO_DATE(%s,'%%Y-%%m-%%d') ORDER BY t2.ratings_count DESC limit %s offset %s;", [start_date, end_date, limit, offset])
    	else:
            cur.execute("select DISTINCT t1.hd_id,t2.brand,t2.ratings_count,t2.source,t2.selling_price,t2.mrp,t1.image_url,t1.title from products_insights t2,products_info t1 where t1.hd_id = t2.hd_id and t1.created_at >= STR_TO_DATE(%s,'%%Y-%%m-%%d') and t1.created_at <= STR_TO_DATE(%s,'%%Y-%%m-%%d') and t1.source=%s ORDER BY t2.ratings_count DESC limit %s offset %s;", [start_date, end_date, source, limit, offset])

    else:
    	if source == "all":
    		cur.execute("select DISTINCT t1.hd_id,t2.brand,t2.ratings_count,t2.source,t2.selling_price,t2.mrp,t1.image_url,t1.title from products_insights t2,products_info t1 where t1.hd_id = t2.hd_id and t1.created_at >= STR_TO_DATE(%s,'%%Y-%%m-%%d') and t1.created_at <= STR_TO_DATE(%s,'%%Y-%%m-%%d') limit %s offset %s;", [start_date, end_date, limit, offset])
    	else:
    		cur.execute("select DISTINCT t1.hd_id,t2.brand,t2.ratings_count,t2.source,t2.selling_price,t2.mrp,t1.image_url,t1.title from products_insights t2,products_info t1 where t1.hd_id = t2.hd_id and t1.created_at >= STR_TO_DATE(%s,'%%Y-%%m-%%d') and t1.created_at <= STR_TO_DATE(%s,'%%Y-%%m-%%d') and t1.source=%s limit %s offset %s;", [start_date, end_date, source, limit, offset])

    row_headers = [x[0] for x in cur.description if x]
    results = cur.fetchall()
    json_data = []
    for result in results:
        json_data.append(dict(zip(row_headers, result)))
    products = json.dumps(json_data)
    cur.close()
    return products

