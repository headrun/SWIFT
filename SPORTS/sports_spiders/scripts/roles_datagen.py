import MySQLdb

class PlayerRolesDatagen():
        def __init__(self):
                #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="DATATESTDB_DEV", charset='utf8', use_unicode=True)
                self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="DATATESTDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()

        def main(self):
                #sel_query = 'select id, record from test_cases where suite_id=19 and record like %s'
		#values = '%' + "#baseball#" + '%'
                #self.cursor.execute(sel_query, values)
		sel_query = "select id, record from test_cases where suite_id in ('18', '19', '94', '95', '96')"
		self.cursor.execute(sel_query)
                data = self.cursor.fetchall()
                for data_ in data:
			id_ = data_[0]
			record = data_[1]
			'''in_field = "Infielder"
			out_filed = "Outfielder"
			record = record.replace('First baseman', in_field).replace('Third baseman', in_field). \
			replace('Second baseman', in_field).replace('Shortstop', in_field). \
			replace('Left fielder', out_filed).replace('Right fielder', out_filed). \
			replace('Center fielder', out_filed)'''
			main_role = record.split('#<>#')[-1]
			update_main_role = main_role.title().replace('-Back', '-back'). \
                        replace('-Distance', '-distance').replace('-Rounder', '-rounder'). \
                        replace('-Keeper', '-keeper').replace('-Handed', '-handed').replace('-Order', '-order'). \
                        replace('-Row', '-row').replace('-Half', '-half').replace('-Eighth', '-eighth'). \
                        replace('Pitchers', 'Pitcher').replace('Central ', '').replace('Center Back', 'Centre-back'). \
                        replace('Centre Back', 'Centre-back').replace('Left Back', 'Left-back'). \
                        replace('Full Back', 'Full-back').replace('Half Back', 'Half-back'). \
                        replace('Right Back', 'Right-back')
			record = record.replace(main_role, update_main_role)
 
			up_qry = 'update test_cases set record=%s where id=%s limit 1'
			values = (record, id_)
			self.cursor.execute(up_qry, values)
			

if __name__ == '__main__':
    OBJ = PlayerRolesDatagen()
    OBJ.main()


