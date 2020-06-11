import MySQLdb
import json

def main():

    DICT = {'scoresway soccer person id': ['soccerway_id', 'soccerway_soccer'], 'UEFA player ID': ['uefa_id', 'uefa_soccer'], 'FIFA player ID': ['fifa_id', 'fifa_soccer'], 'MLSSoccer.com ID': ['mls_id', 'MLS'], 'ESPNcricinfo player ID': ['cricket_id', 'espn_cricket'], 'MLB ID': ['mlb_id', 'MLB'], 'NHL.com ID': ['nhl_id', 'NHL'], 'NFL.com ID': ['nfl_id', 'NFL'], 'PremiershipRugby.com ID': ['premier_rugby_id', 'premiership_rugby'], 'ESPN SCRUM ID': ['espn_rubgy_id', 'espn_rugby']}
    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    data = open('id.json', 'r+')
    for data_ in data:
        _data = json.loads(data_.strip())
        pl_gid =  _data.keys()[0].split('{')[0]

        if "WIKI" not in pl_gid:
            continue


        sel_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid=%s'
        values = (pl_gid)
        cursor.execute(sel_qry, values)
        data = cursor.fetchone()
        if data:
            values = (data[0])
            sel_qry = 'select SP.title, P.id from sports_participants P, sports_types SP where SP.id=P.sport_id and P.gid=%s'
            cursor.execute(sel_qry, values)
            data = cursor.fetchone()
            if data:
                sp_title = data[0]
                sp_id = data[1]

                for key, values in DICT.iteritems():
                    data = _data.values()[0].get(key, '')
                    if data:
                        pl_id = data.get('ids', '')[0].get('id', '')
                        pl_id = pl_id.split('/')[-1]
                        source = values[-1]
                        if values[0] in ['soccerway_id', 'uefa_id']:
                            pl_id = "PL" + pl_id
                        sk_qry = 'select entity_id from sports_source_keys where source_key=%s and source=%s'
                        values1 = (pl_id, source)
                        cursor.execute(sk_qry, values1)
                        data1 = cursor.fetchone()
                        INT_QRY  = "insert into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(),now()) on duplicate key update modified_at= now()"
                        if not data1:
                            data1_values = (sp_id, 'participant', source, pl_id) 
                            cursor.execute(INT_QRY, data1_values)



    cursor.close()

if __name__ == '__main__':
    main()

