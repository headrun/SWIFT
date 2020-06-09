import pymysql
from flask import Flask, jsonify, request
from flaskext.mysql import MySQL
import requests

app = Flask(__name__)
mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'mca'
app.config['MYSQL_DATABASE_PASSWORD'] = 'H3@drunMcaMy07'
app.config['MYSQL_DATABASE_DB'] = 'bse'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)

@app.route('/api/v1', methods=["GET"])
def corp_announcement():
	params = {}
	params['category'] = request.args.get('category', '')
	params['id'] = request.args.get('id', '')
	try:
		conn = mysql.connect()
		cursor = conn.cursor(pymysql.cursors.DictCursor)
		if params['category'] in ('corp_announcement'):
			query = "SELECT * FROM {0} WHERE SCRIP_CD = {1} ".format(params['category'],params['id'])	
		if params['category'] in ('insider_2015','sast_annual','voting','corp_annexure_2'):
			query = "SELECT * FROM {0} WHERE Fld_ScripCode = {1} ".format(params['category'],params['id'])	
		elif params['category'] in ('block_deals', 'bulk_deals'):
			query = "SELECT * FROM {0} WHERE SCRIP_CODE = {1} ".format(params['category'],params['id'])
		elif params['category'] in ('board_meeting','insider_1992','insider_sast','share_holder_meeting'):
			query = "SELECT * FROM {0} WHERE scrip_code = {1} ".format(params['category'],params['id'])
		elif params['category'] in ('consolidated_pledge'):
			query = "SELECT * FROM {0} WHERE ScripCode = {1} ".format(params['category'],params['id'])
		elif params['category'] in ('peer'):
			query = "SELECT * FROM {0} WHERE scrip_cd = {1} ".format(params['category'],params['id'])
		elif params['category'] in ('notice','corp_annexure_1','corp_action','slb','annual_report','debt', 'corp_info', 'results_annual', 'results_qrt', 'shareholding_pattern'):
			query = "SELECT * FROM {0} WHERE scrip_code = {1} ".format(params['category'],params['id'])

		cursor.execute(query)
		res = cursor.fetchall()
		response = jsonify(res)
		return response
	except Exception as e:
		print(e)
	finally:
		cursor.close()
		conn.close()
		


if __name__ == "__main__":
    app.run(debug=True)
