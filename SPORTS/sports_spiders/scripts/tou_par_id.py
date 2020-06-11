import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    sel_qry = 'select participant_id, tournament_id from sports_tournaments_participants where tournament_id in (4036, 3916, 3917, 3709, 3710, 1173, 1176, 2812, 32, 571, 3601, 4096,2833, 1197,614, 34, 573, 30, 28, 567, 579, 598)'
    #sel_qry = 'select id, short_title, display_title from sports_teams'
    #sel_qry = "select SP.id, SP.title, T.short_title, T.display_title from sports_participants SP, sports_teams T where T.participant_id=SP.id and SP.sport_id=7 and T.display_title =''"
    cursor.execute(sel_qry)
    data = cursor.fetchall()
    for data_ in data:
        pa_id_    = data_[0]
        pa_title = str(data_[1])
        dt_title = pa_title.replace(" women's national football team", "").replace('S.S.D. ', '').replace(' national football team', '').replace(' national soccer team', '').replace(" men's national under-20 soccer team", " under-20").replace(' national under-20 football team', ' under-20').replace(' Football Club', '').replace(' Sport Club', '').replace(' Futebol Clube', '').replace(' F.K.', '').replace(' County Cricket Club', '').replace(' Cricket Union', '').replace(' Sports Club', '').replace(' Sports Club', '').replace(' cricket team', '').replace(' District Cricket Club', '').replace(' Cricket Club', '').split('(')[0].strip().replace(' Basketball Club', '').strip().replace(' Basketball Team', '').replace(' F.C.', '').replace(' A.F.C.', '').replace('FC ', '').replace(' FA', '').replace(' FC', '').replace('FK ', '').replace('fk ', '').replace('A.D. ', '').replace('a.d. ', '').replace('NK ', '').replace('nk ', '').replace('ND ', '').replace('BC ', '').replace('BK ', '').replace('KK ', '').replace(' KK', '').replace(' B.C.', '').replace('GKK ', '').replace(' S.C.', '').replace('S.S. ', '').replace('C.D. ', '').replace('G.D. ', '').replace(' United', '').replace(' SC', '').replace('A.F.C. ', '').replace('K.F.C. ', '').replace('G.F.C. ', '').replace('F.C. ', '').replace(' AFC', '').replace(' CFC', '').replace('FC ', '').replace(' SC', '').replace(' S.S.D.', '').replace(' A.F.C.', '').replace(' S.K.', '')


        st_title = pa_title.replace(' national football team', '').replace(' Football Club', '').replace(' Sport Club', '').replace(' County Cricket Club', '').replace(' Cricket Union', '').replace(' Sports Club', '').replace(' Sports Club', '').replace(' cricket team', '').replace(' District Cricket Club', '').replace(' Cricket Club', '').split('(')[0].strip().replace(' Basketball Club', '').strip().replace(' Basketball Team', '').replace(' F.C.', '').replace(' A.F.C.', '').replace('FC ', '').replace(' FA', '').replace(' FC', '').replace('FK ', '').replace('fk ', '').replace('A.D. ', '').replace('a.d. ', '').replace('NK ', '').replace('nk ', '').replace('ND ', '').replace('BC ', '').replace('BK ', '').replace('KK ', '').replace(' KK', '').replace(' B.C.', '').replace('GKK ', '').replace(' S.C.', '').replace('S.S. ', '').replace('C.D. ', '').replace('G.D. ', '').replace(' United', '').replace(' SC', '').replace('A.F.C. ', '').replace('K.F.C. ', '').replace('G.F.C. ', '').replace('F.C. ', '').replace(' AFC', '').replace(' CFC', '').replace('FC ', '').replace(' SC', '').replace(' S.S.D.', '').replace(' A.F.C.', '').replace(' S.K.', '')


        #up_qry = 'update sports_teams set display_title=%s where participant_id =%s'
        up_qry = 'update sports_teams set tournament_id=%s where participant_id =%s'
        values = (dt_title, pa_id_)
        cursor.execute(up_qry, values)
    cursor.close()

if __name__ == '__main__':
    main()


