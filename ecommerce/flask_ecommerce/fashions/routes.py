import json
from flask import Flask, render_template, request
from flask_mysqldb import MySQL
from datetime import date, datetime
import math
app = Flask(__name__, static_url_path="/static", static_folder="static")
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
    category = request.args.get('category')
    limit = 20
    offset = 0
    totalCount = 0
    print("page_num, limit", page_num, limit)
    if page_num in (0, 1):
        offset = 0
    else:
        offset = (int(page_num)-1)*limit
    cur = mysql.connection.cursor()
    if page_num == 1:
        if source == "all":
            if category == "all":
                cur.execute("select count(*) from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s",[start_date, end_date])
            else:
                cur.execute("select count(*) from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s",[start_date, end_date, category])
        else:
            if category == "all":
                cur.execute("select count(*) from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s",[start_date, end_date, source])
            else:
                cur.execute("select count(*) from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s",[start_date, end_date, source, category])

        total_count = cur.fetchall()
        totalCount = math.ceil(total_count[0][0]/20)
    if sort_by == "popularity":
        if source == "all":
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s ORDER BY ratings_count DESC limit %s offset %s;",[start_date, end_date, limit, offset])
            else:
                cur.execute("select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s ORDER BY ratings_count DESC limit %s offset %s;",[start_date, end_date,category, limit, offset])
        else:
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s ORDER BY ratings_count DESC limit %s offset %s;",[start_date, end_date, source, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s ORDER BY ratings_count DESC limit %s offset %s;",[start_date, end_date, source,category,limit, offset])

    elif sort_by == "discount":
        if source == "all":
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s ORDER BY discount_percentage DESC limit %s offset %s;",[start_date, end_date, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s ORDER BY discount_percentage DESC limit %s offset %s;",[start_date, end_date, category, limit, offset])
        else:
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s ORDER BY discount_percentage DESC limit %s offset %s;",[start_date, end_date, source, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s ORDER BY discount_percentage DESC limit %s offset %s;",[start_date, end_date, source, category, limit, offset])

    elif sort_by == "hightolow":
        if source == "all":
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s ORDER BY mrp DESC limit %s offset %s;",[start_date, end_date, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s ORDER BY mrp DESC limit %s offset %s;",[start_date, end_date, category, limit, offset])
        else:
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s ORDER BY mrp DESC limit %s offset %s;",[start_date, end_date, source, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s ORDER BY mrp DESC limit %s offset %s;",[start_date, end_date, source, category, limit, offset])
    elif sort_by == "lowtohigh":
        if source == "all":
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s ORDER BY mrp ASC limit %s offset %s;",[start_date, end_date, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s ORDER BY mrp ASC limit %s offset %s;",[start_date, end_date, category, limit, offset])
        else:
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s ORDER BY mrp ASC DESC limit %s offset %s;",[start_date, end_date, source, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s ORDER BY mrp ASC DESC limit %s offset %s;",[start_date, end_date, source, category, limit, offset])
    elif sort_by == "whatsNew":
        if source == "all":
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s ORDER BY created_at DESC limit %s offset %s;",[start_date, end_date, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s ORDER BY created_at DESC limit %s offset %s;",[start_date, end_date, category, limit, offset])
        else:
            if category == "all":   
                cur.execute("select brand,reference_url,ratings_count,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s ORDER BY created_at DESC limit %s offset %s;",[start_date, end_date, source, limit, offset])
            else:
                cur.execute("select brand,reference_url,ratings_count,category,discount_percentage,source,created_at,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s ORDER BY created_at DESC limit %s offset %s;",[start_date, end_date, source, category, limit, offset])

    else:
        if source == "all":
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s limit %s offset %s;",[start_date, end_date, limit, offset])
            else:
                cur.execute("select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and category=%s limit %s offset %s;",[start_date, end_date, category, limit, offset])
        else:
            if category == "all":
                cur.execute("select brand,reference_url,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s limit %s offset %s;",[start_date, end_date, source, limit, offset])
            else:
                cur.execute("select brand,reference_url,category,ratings_count,source,selling_price,mrp,image_url,title from products_info where DATE(created_at)>=%s and DATE(created_at)<= %s and source=%s and category=%s limit %s offset %s;",[start_date, end_date, source, category, limit, offset])

    row_headers = [x[0] for x in cur.description if x]
    results = cur.fetchall()
    json_data = []
    for result in results:
        json_data.append(dict(zip(row_headers, result)))
    products = json.dumps(json_data,default=json_serial)
    cur.close()
    if totalCount != 0:
        return {"totalCount":totalCount,"products":products}
    else:
        return {"products":products}

def json_serial(obj):
    if isinstance(obj, (datetime, date)): 
        return obj.isoformat() 
    raise TypeError ("Type %s not serializable" % type(obj))

if __name__ == "__main__":
    app.run()
