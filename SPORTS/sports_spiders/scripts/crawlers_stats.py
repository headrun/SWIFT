#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

#UP_QRY = 'update sports_tournaments_groups set sport_id=%s where id=%s limit 1'

DRI_DICT = {'Jaideep': '33', 'Bibeejan': '29'}

COU_DICT = {
"2":"AUSTRALIA",
"5":"BRAZIL",
"6":"CANADA",
"16":"ENGLAND",
"17":"FINLAND",
"18":"FRANCE",
"20":"GERMANY",
"24":"INDIA",
"25":"IRELAND",
"32":"NEW ZEALAND",
"42":"RUSSIA",
"43":"SCOTLAND",
"45":"SPAIN",
"47":"SWITZERLAND",
"50":"UAE",
"51":"UK",
"53":"USA"}

SOU_DICT = {"88":"espndeportes",
"223":"afcasiancup",
"224":"afl",
"225":"atpworldtour",
"226":"ausopen",
"227":"baku2015",
"228":"basket.fi",
"229":"basketball.realgm",
"230":"britishtennis",
"231":"cbssports",
"232":"cfl",
"233":"cmsapi",
"234":"concacaf",
"235":"daviscup",
"236":"dubaihoteluae",
"237":"englandrugby",
"238":"espn.go",
"239":"espncricinfo",
"240":"espnfc",
"241":"espnscrum",
"242":"europeantour",
"243":"events.fih.ch",
"244":"fcbarcelona",
"245":"fedcup",
"246":"fiba",
"247":"fide",
"248":"fifa",
"249":"formula1",
"250":"futbol24",
"251":"glasgow",
"252":"hopmancup",
"253":"horseracingnation",
"254":"indiansuperleague",
"255":"int.soccerway",
"256":"iptlworld",
"257":"itmcup",
"258":"khl",
"259":"laureus",
"260":"letour",
"261":"lnr.fr",
"262":"lpga",
"263":"mlb",
"264":"mlssoccer",
"265":"motogp",
"266":"nba",
"267":"ncaa",
"268":"nfl",
"269":"nhl",
"270":"nrl",
"271":"pgatour",
"272":"rabobankhockeyworldcup2014",
"273":"rbs6nations",
"274":"rolandgarros",
"275":"rugbyheartland",
"276":"rugbyworldcup",
"277":"sanzarrugby",
"278":"scoresway",
"279":"sochi2014",
"280":"superleague",
"281":"uci",
"282":"uefa",
"283":"ufc",
"284":"usaprocyclingchallenge",
"285":"usga",
"286":"usopen",
"287":"wbanews",
"288":"wimbledon",
"289":"wnba",
"290":"worldmarathonmajors",
"970":"baseballamerica",
"971":"chennai2013",
"972":"cyclingstage",
"973":"dubai2014wrb",
"974":"espn.co.uk",
"975":"eurosport",
"976":"grandepremio",
"977":"indycar",
"978":"mg.superesportes",
"979":"milb",
"980":"premiershiprugby",
"981":"revistaplacar",
"982":"rio2016",
"983":"sportstats",
"984":"women.cyclingfever",
"985":"worldchess",
"986":"worldgolfchampionships"}

INST_QRY = "insert into Crawler(source_id, project_id, country_id, status_id, rights_id, dri_id, name, reference_url, db_ip, db_name, machine_ip, is_robots, priority, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
def main():

    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTS_COMMONDB", charset='utf8', use_unicode=True).cursor()
    query = 'select source_name, country,  DRI, reference_url from sports_source_stats'
    sp_cur.execute(query)
    data = sp_cur.fetchall()
    for data_ in data:
        sc_name = data_[0]
        country = data_[1]
        dri = data_[2]
        reference_url = data_[3]
            
        for key, value in COU_DICT.iteritems():
            if value == country:
                country_id = key
        for key, value in SOU_DICT.iteritems():
            if value == sc_name:
                source_id = key
        dri_id = DRI_DICT[dri]
        values = (source_id, '14', country_id, '1', '0', dri_id, sc_name, reference_url, '10.28.218.81', 'SPORTSDB', '10.28.216.45', '', '')
        sp_cur.execute(INST_QRY, values)


    sp_cur.close()

if __name__ == '__main__':
    main()


