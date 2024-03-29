import json
from flask_mysqldb import MySQL
from pymysql.cursors import DictCursor
from flask import Flask, render_template, request
from datetime import date, datetime
import math

app = Flask(__name__, static_url_path="/static", static_folder="static")
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'Ecomm@34^$'
# app.config['MYSQL_DB'] = 'ECOMMERCEDB'
mysql = MySQL(app)
country_list={"IND":"ECOMMERCEDB","US":"ECOMMERCEDB_US"}

@app.route('/')
def home():
    return render_template('details.html')

@app.route('/brands')
def brand():
    country = request.args.get('country')
    country_db = country_list.get(country)
    brandCur = mysql.connection.cursor()
    sql_query = "select brand from %s.products_info group by brand;"%country_db
    brandCur.execute(sql_query)
    row_headers = [x[0] for x in brandCur.description if x]
    results = brandCur.fetchall()
    json_data = []
    for result in results:
        json_data.append(dict(zip(row_headers, result)))
    brands = json.dumps(json_data,default=json_serial)
    brandCur.close()
    return brands
@app.route('/sources')
def source():
    country = request.args.get('country')
    country_db = country_list.get(country)
    sourceCur = mysql.connection.cursor()
    sql_query = "select source from %s.products_info group by source;"%country_db
    sourceCur.execute(sql_query)
    row_headers = [x[0] for x in sourceCur.description if x]
    results = sourceCur.fetchall()
    json_data = []
    for result in results:
        json_data.append(dict(zip(row_headers, result)))
    sources = json.dumps(json_data,default=json_serial)
    sourceCur.close()
    return sources
@app.route('/totalCount')
def totalCount():
    start_date = request.args.get('startdate')
    end_date = request.args.get('enddate')
    source = request.args.get('source', 'all')
    sort_by = request.args.get('sortBy')
    page_num = request.args.get('page_num', 1)
    category = request.args.get('category','all')
    brandName = request.args.get('brandName', 'all')
    country = request.args.get('country')
    country_db = country_list.get(country)
    if source == '':
        source = 'all'
    if brandName == '':
        brandName = 'all'
    totalcount_cur = mysql.connection.cursor()
    if source == "all":
        if category == "all":
            if brandName == "all":
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s'"%(country_db, start_date, end_date)
            else:
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s'"%(country_db, start_date, end_date, brandName)
        else:
            if brandName == "all":
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s'"%(country_db, start_date, end_date, category)
            else:
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s'"%(country_db, start_date, end_date, category, brandName)
    else:
        if category == "all":
            if brandName == "all":
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s'"%(country_db, start_date, end_date, source)
            else:
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s'"%(country_db, start_date, end_date, source, brandName)
        else:
            if brandName == "all":
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s'"%(country_db, start_date, end_date, source, category)
            else:
                sql_query = "select count(*) from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s'" %(country_db, start_date, end_date, source, category, brandName)
    totalcount_cur.execute(sql_query)
    total_count = totalcount_cur.fetchall()
    totalCount = math.ceil(total_count[0][0]/20)
    totalcount_cur.close()
    return {"totalCount":totalCount}

@app.route('/search')
def data():
    start_date = request.args.get('startdate')
    end_date = request.args.get('enddate')
    source = request.args.get('source', 'all')
    sort_by = request.args.get('sortBy')
    page_num = request.args.get('page_num', 1)
    category = request.args.get('category')
    brandName = request.args.get('brandName', 'all')
    country = request.args.get('country')
    country_db = country_list.get(country)
    if source == '':
        source = 'all'
    if brandName == '':
        brandName = 'all'
    limit = 20
    offset = 0
    print("page_num, limit", page_num, limit)
    if page_num in (0, 1):
        offset = 0
    else:
        offset = (int(page_num)-1)*limit
    cur = mysql.connection.cursor()
    if sort_by == "popularity":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, brandName, limit, offset)
    elif sort_by == "discount":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, brandName, limit, offset)
    elif sort_by == "hightolow":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY mrp DESC limit %s offset %s;"%( country_db, start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, brandName, limit, offset)

    elif sort_by == "lowtohigh":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(country_db, start_date, end_date, source, category, brandName, limit, offset)
    elif sort_by == "whatsNew":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":   
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(country_db, start_date, end_date, source, category, brandName, limit, offset)
    else:
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' limit %s offset %s;"%(country_db, start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' limit %s offset %s;"%(country_db, start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' limit %s offset %s;"%(country_db, start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' limit %s offset %s;"%(country_db, start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' limit %s offset %s;"%(country_db, start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' limit %s offset %s;"%(country_db, start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' limit %s offset %s;"%(country_db, start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from %s.products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' limit %s offset %s;"%(country_db, start_date, end_date, source, category, brandName, limit, offset)
    cur.execute(sql_query)
    row_headers = [x[0] for x in cur.description if x]
    results = cur.fetchall()
    json_data = []
    for result in results:
        json_data.append(dict(zip(row_headers, result)))
    products = json.dumps(json_data,default=json_serial)
    cur.close()
    return {"products":products}

def json_serial(obj):
    if isinstance(obj, (datetime, date)): 
        return obj.isoformat() 
    raise TypeError("Type %s not serializable" % type(obj))

if __name__ == "__main__":
    app.run()
