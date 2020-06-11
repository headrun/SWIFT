import MySQLdb
import unicodedata
import xlwt
def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    sel_qry = "select T.id, T.title, SP.title, T.type, T.season_start, T.season_end from sports_tournaments T, sports_types SP where SP.id=T.sport_id and T.id in (1850,2905,2235,222,1825,896,52,5476,15,288,25,1491,244,68,242,63,246,290,70,67,78,272,3601,631,357,569,572,1891,3419,2850,267,81,266,359,585,1,378,87,591,89,1015,91,1064,258,92,255,2207,231,228,233,519,277,225,273,597,509,88,276,1115,954,364,2841,586,577,598,596,573,589,34,595,2209,9,197,2553,559,570,213,609,564,578,4096,33,599,574,590,35,215,28,214,575,32,29,579,216,1105,571,567,558,562,610,240,3838,229,3626,580,529,1082,1083, 1084,1085,1116,1117) and T.status !='obsolete'"
    cursor.execute(sel_qry)
    data = cursor.fetchall()
    file_name = open('tournaments_data', 'w+')
    row_count = 1

    header = ['TOU ID', 'League', 'Sport', 'Season Start', 'Season End']
    todays_excel_file = xlwt.Workbook(encoding="utf-8")
    todays_excel_sheet1 = todays_excel_file.add_sheet("sheet1")
    for i, row in enumerate(header):
        todays_excel_sheet1.write(0, i, row) 


    excel_file_name = 'tournaments.xls'
    for data_ in data:
        id_ = str(data_[0])
        title = data_[1]
        if type(title) == unicode:
            title = unicodedata.normalize('NFKD', title).encode('ascii','ignore')

        sport = data_[2]
        type_ = data_[3]
        start_date = str(data_[4])
        end_date = str(data_[5])
        values =  [id_, title, sport, start_date, end_date]
        for col_count, value in enumerate(values):
            #row_count = row_count+1
            todays_excel_sheet1.write(row_count, col_count, value)
        row_count = row_count+1


        if type_ != "tournament":
            sel_qry = "select T.id, T.title, SP.title, T.type, T.season_start, T.season_end from sports_tournaments T, sports_types SP where SP.id=T.sport_id and T.id in (select tournament_id from sports_concepts_tournaments where concept_id=%s) and T.status !='obsolete'"
            values = (id_)
            cursor.execute(sel_qry, values)
            data = cursor.fetchall()
            for data_ in data:
                id_ = str(data_[0])
                title = data_[1]
                if type(title) == unicode:
                    title = unicodedata.normalize('NFKD', title).encode('ascii','ignore')

                sport = data_[2]
                type_ = data_[3]


                start_date = str(data_[4])
                end_date = str(data_[5])
                values =  [id_, title, sport, start_date, end_date]
                for col_count, value in enumerate(values):
                    #row_count = row_count+1
                    todays_excel_sheet1.write(row_count, col_count, value)
                row_count = row_count+1

    cursor.close()
    todays_excel_file.save(excel_file_name)
if __name__ == '__main__':
    main()


