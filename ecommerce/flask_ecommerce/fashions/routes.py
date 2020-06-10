import json
from flask import Flask, render_template, request
from datetime import date, datetime
import math

from sqlalchemy import create_engine
import pymysql
import pandas as pd

app = Flask(__name__, static_url_path="/static", static_folder="static")

@app.route('/')
def home():
    return render_template('details.html')

@app.route('/brands')
def brand():
    start_date = request.args.get('startdate')
    end_date = request.args.get('enddate')
    source = request.args.get('source', 'all')
    category = request.args.get('category','all')
    if source == '':
        source = 'all'
    if category == '':
        category = 'all'
    db_connection_str = 'mysql+pymysql://root:Ecomm@34^$@127.0.0.1/ECOMMERCEDB'
    db_connection = create_engine(db_connection_str)
    if source == "all":
        if category == "all":
            sql_query = "select brand from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' group by brand;"%(start_date, end_date)
        else:
            sql_query = "select brand from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' group by brand;"%(start_date, end_date, category)
    else:
        if category == "all":
            sql_query = "select brand from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' group by brand;"%(start_date, end_date, source)
        else:
            sql_query = "select brand from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' group by brand;"%(start_date, end_date, source, category)
    brands_data = pd.read_sql(sql_query, con=db_connection)
    return brands_data.to_json(orient='records')
@app.route('/sources')
def source():
    db_connection_str = 'mysql+pymysql://root:Ecomm@34^$@127.0.0.1/ECOMMERCEDB'
    db_connection = create_engine(db_connection_str)
    source_data = pd.read_sql("select source from products_info group by source;", con=db_connection)
    return source_data.to_json(orient='records')
@app.route('/search')
def data():
    start_date = request.args.get('startdate')
    end_date = request.args.get('enddate')
    source = request.args.get('source', 'all')
    sort_by = request.args.get('sortBy')
    page_num = request.args.get('page_num', 1)
    category = request.args.get('category')
    brandName = request.args.get('brandName', 'all')
    if source == '':
        source = 'all'
    if brandName == '':
        brandName = 'all'
    limit = 20
    offset = 0
    totalCount = 0
    print("page_num, limit", page_num, limit)
    if page_num in (0, 1):
        offset = 0
    else:
        offset = (int(page_num)-1)*limit
    db_connection_str = 'mysql+pymysql://root:Ecomm@34^$@127.0.0.1/ECOMMERCEDB'
    db_connection = create_engine(db_connection_str)
    if page_num == 1:
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s'"%(start_date, end_date)
                else:
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s'"%(start_date, end_date, brandName)
            else:
                if brandName == "all":
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s'"%(start_date, end_date, category)
                else:
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s'"%(start_date, end_date, category, brandName)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s'"%(start_date, end_date, source)
                else:
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s'"%(start_date, end_date, source, brandName)
            else:
                if brandName == "all":
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s'"%(start_date, end_date, source, category)
                else:
                    sql_query = "select count(*) from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s'" %(start_date, end_date, source, category, brandName)
        df = pd.read_sql(sql_query, con=db_connection)
        response = df.to_json(orient='records')
        response = json.loads(response)
        total_count = response[0]["count(*)"]
        totalCount = math.ceil(total_count/20)
    if sort_by == "popularity":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY ratings_count DESC limit %s offset %s;"%(start_date, end_date, source, category, brandName, limit, offset)
    elif sort_by == "discount":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY discount_percentage DESC limit %s offset %s;"%(start_date, end_date, source, category, brandName, limit, offset)
    elif sort_by == "hightolow":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY mrp DESC limit %s offset %s;"%(start_date, end_date, source, category, brandName, limit, offset)

    elif sort_by == "lowtohigh":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY mrp ASC limit %s offset %s;"%(start_date, end_date, source, category, brandName, limit, offset)
    elif sort_by == "whatsNew":
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":   
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' ORDER BY created_at DESC limit %s offset %s;"%(start_date, end_date, source, category, brandName, limit, offset)
    else:
        if source == "all":
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' limit %s offset %s;"%(start_date, end_date, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and brand='%s' limit %s offset %s;"%(start_date, end_date, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' limit %s offset %s;"%(start_date, end_date, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and category='%s' and brand='%s' limit %s offset %s;"%(start_date, end_date, category, brandName, limit, offset)
        else:
            if category == "all":
                if brandName == "all":
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' limit %s offset %s;"%(start_date, end_date, source, limit, offset)
                else:
                    sql_query = "select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and brand='%s' limit %s offset %s;"%(start_date, end_date, source, brandName, limit, offset)
            else:
                if brandName == "all":
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' limit %s offset %s;"%(start_date, end_date, source, category, limit, offset)
                else:
                    sql_query = "select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>='%s' and DATE(created_at)<= '%s' and source='%s' and category='%s' and brand='%s' limit %s offset %s;"%(start_date, end_date, source, category, brandName, limit, offset)

    query_data = pd.read_sql(sql_query, con=db_connection)
    if totalCount != 0:
        return {"totalCount":totalCount, "products":query_data.to_json(orient='records')}
    else:
        return {"products":query_data.to_json(orient='records')}

def json_serial(obj):
    if isinstance(obj, (datetime, date)): 
        return obj.isoformat() 
    raise TypeError("Type %s not serializable" % type(obj))

if __name__ == "__main__":
    app.run()
