import MySQLdb
def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    #teams_list = open('teams_list', 'r+')
    teams_list= open('rovi_list', 'r+')
    for team in teams_list:
        team_id = team.split(',')[-1].strip()
        cosmo_id= team.split(',')[0].strip()
        sel_qry = 'select participant_id from sports_teams where id=%s'
        values = (team_id)
        cursor.execute(sel_qry, values)
        data = cursor.fetchone()
        team_id = data[0]
        #cosomo_list= open('cosmo_list', 'r+')
        #for cosmo in cosomo_list:
        #con_id = cosmo.split(',')[0].strip()
        #rovi_id = cosmo.split(',')[1].strip()
        #if con_id == cosmo_id:
        rovi_id = cosmo_id
        rovi_qry = 'select entity_id from sports_rovi_merge where rovi_id=%s'
        rovi_value = (rovi_id)
        cursor.execute(rovi_qry, rovi_value)
        rovi_data = cursor.fetchall()
        if not rovi_data:
            #pa_id = data[0][0]
            pa_id = team_id
            IN_QRY = 'insert into sports_rovi_merge(entity_type, entity_id, rovi_id, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'
            rv_values = ('team', pa_id, rovi_id)
            cursor.execute(IN_QRY, rv_values)
        else:
            print rovi_data
    cursor.close()

if __name__ == '__main__':
    main()

