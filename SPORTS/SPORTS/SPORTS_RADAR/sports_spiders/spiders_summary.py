#!/usr/bin/env python

################################################################################
#$Id: spiders_summary.py,v 1.1 2016/03/23 07:19:22 headrun Exp $
#Copyright(c) 2005 Veveo.tv
################################################################################

import os
import glob
from datetime import date, datetime, timedelta

import jinja2

from vtv_utils import VTV_PY_EXT, VTV_SERVER_DIR, vtv_unpickle, make_dir, copy_file
from vtv_task import VtvTask, vtv_task_main
import copy
import ssh_utils


REPORTS_DIR = '/data/REPORTS/'

AUTO_REPORT_DIR = os.path.join(REPORTS_DIR, 'AUTOMATED_REPORT')
PICKLE_DIR      = os.path.join(REPORTS_DIR, 'PICKLE_FILES')

SCP_SRC_DIR = REPORTS_DIR
SCP_DST_DIR = PICKLE_DIR

NAME, VALUE     = range(2)

ONE_GB = 1
FIVE_GB = 5 * ONE_GB
TEN_GB  = 10 * ONE_GB

TIMEFORMAT  = '%Y-%m-%d'

JINJA_FILE_NAME   = 'spiders_summary.jinja'
HTML_FILE_NAME    = 'spiders_summary.html'
SUMMARY_FILE_NAME = 'SUMMARY_REPORT.html'

#local_dir_name, serve_ip, report_prefix, remote_dir_name, date_opt, title

STATS_DIR_LIST = [
    ( 'MlbRoster',              '10.4.2.197', 'MlbRoster_',                'MlbRoster',                 'D', 'MLB ROSTER'),
    ( 'MLBSpiderSchedules',     '10.4.2.197', 'MLBSpiderSchedules_',       'MLBSpiderSchedules',        'D', 'MLB SPIDER SCHEDULES'),
    ( 'MLBSpiderScores',        '10.4.2.197', 'MLBSpiderScores_',          'MLBSpiderScores',           'D', 'MLB SPIDER SCORES'),
    ( 'MLBStandings',           '10.4.2.197', 'MLBStandings_',             'MLBStandings',              'D', 'MLB STANDINGS'),
    ( 'MexicanBaseballSchedules', '10.4.2.197', 'MexicanBaseballSchedules_', 'MexicanBaseballSchedules', 'D', 'MEXICAN BASEBALL SCHEDULES'),
    ( 'MexicanBaseballScores', '10.4.2.197', 'MexicanBaseballScores_', 'MexicanBaseballScores',          'D', 'MEXICAN BASEBALL SCORES'),
    ( 'MexBaseballStandings',   '10.4.2.197', 'MexBaseballStandings_',     'MexBaseballStandings',      'D', 'MEX BASEBALL STANDINGS'),
    ( 'MexBaseballRoster',      '10.4.2.197', 'MexBaseballRoster_',        'MexBaseballRoster',         'D', 'MEX BASEBALL ROSTER'),

    ( 'NbaRoster',              '10.4.2.197', 'NbaRoster_',                'NbaRoster',                 'D', 'NBA ROSTER'),
    ( 'NBASpiderSchedules',     '10.4.2.197', 'NBASpiderSchedules_',       'NBASpiderSchedules',        'D', 'NBA SPIDER SCHEDULES'),
    ( 'NBASpiderScores',        '10.4.2.197', 'NBASpiderScores_',          'NBASpiderScores',           'D', 'NBA SPIDER SCORES'),
    ( 'NBAStandings',           '10.4.2.197', 'NBAStandings_',             'NBAStandings',              'D', 'NBA STANDINGS'),
    ( 'EuroNBASchedules',       '10.4.2.197', 'EuroNBASchedules_',         'EuroNBASchedules',           'D', 'EURO BASKETBALL SCHEDULES'),
    ( 'EuroNBAScores',          '10.4.2.197', 'EuroNBAScores_',            'EuroNBAScores',              'D', 'EURO BASKETBALL SCORES'),
    ( 'BasketfiSpiderSchedules','10.4.2.197', 'BasketfiSpiderSchedules_',  'BasketfiSpiderSchedules',    'D', 'FINLAND BASKETBALL SCHEDULES'),
    ( 'BasketfiSpiderScores',   '10.4.2.197', 'BasketfiSpiderScores_',     'BasketfiSpiderScores',       'D', 'FINLAND BASKETBALL SCORES'),
    ( 'ScandbasketballStandings', '10.4.2.197', 'ScandbasketballStandings_', 'ScandbasketballStandings', 'D', 'SCAND BASKETBALL STANDINGS'),
    ( 'BasketballRealgmScores', '10.4.2.197', 'BasketballRealgmScores_',   'BasketballRealgmScores',    'D', 'BASKETBALL REALGM SCORES'),
    ( 'BasketballRealgmSchedules', '10.4.2.197', 'BasketballRealgmSchedules_', 'BasketballRealgmSchedules', 'D', 'BASKETBALL REALGM SCHEDULES'),
    ( 'BasketballRealgmRoster',  '10.4.2.197', 'BasketballRealgmRoster_',  'BasketballRealgmRoster',    'D', 'BASKETBALL REALGM ROSTER'),
    ( 'CBABasketballScores',     '10.4.2.197', 'CBABasketballScores_',     'CBABasketballScores',       'D', 'CBA BASKETBALL SCORES'),
    ( 'CBABasketballSchedules',  '10.4.2.197', 'CBABasketballSchedules_',  'CBABasketballSchedules',    'D', 'CBA BASKETBALL SCHEDULES'),
    ( 'CBAStandings',            '10.4.2.197', 'CBAStandings_',            'CBAStandings',              'D', 'CBA STANDINGS'),

    ( 'NhlRosterNEW',           '10.4.2.197', 'NhlRosterNEW_',             'NhlRosterNEW',              'D', 'NHL ROSTER'),
    ( 'NHLSpiderSchedules',     '10.4.2.197', 'NHLSpiderSchedules_',       'NHLSpiderSchedules',        'D', 'NHL SPIDER SCHEDULES'),
    ( 'NHLSpiderScores',        '10.4.2.197', 'NHLSpiderScores_',          'NHLSpiderScores',           'D', 'NHL SPIDER SCORES'),
    ( 'NHL_Standings',          '10.4.2.197', 'NHL_Standings_',            'NHL_Standings',             'D', 'NHL STANDINGS'),
    ( 'HockeyChampTrophy',      '10.4.2.197', 'HockeyChampTrophy_',        'HockeyChampTrophy',         'D', 'HOCKEY CHAMP TROPHY'),
    ( 'ChampTrophyStandings',   '10.4.2.197', 'ChampTrophyStandings_',     'ChampTrophyStandings',      'D', 'CHAMP TROPHY STANDINGS'),
    ( 'NCAAHockeySchedules',   '10.4.2.197',  'NCAAHockeySchedules_',      'NCAAHockeySchedules',       'D', 'NCAA HOCKEY SCHEDULES'),
    ( 'NCAAHockeyScores',      '10.4.2.197',  'NCAAHockeyScores_',         'NCAAHockeyScores',          'D', 'NCAA HOCKEY SCORES'),
    ( 'KhlStandings',          '10.4.2.197',  'KhlStandings_',             'KhlStandings',              'D', 'KHL STANDINGS'),
    ( 'KHLSpiderSchedules',    '10.4.2.197',  'KHLSpiderSchedules_',       'KHLSpiderSchedules',        'D', 'KHL SCHEDULES'),
    ( 'KHLSpiderScores',       '10.4.2.197',  'KHLSpiderScores_'   ,       'KHLSpiderScores',           'D', 'KHL SCORES'),
    ( 'FIHHockeyScores',       '10.4.2.197',  'FIHHockeyScores_',          'FIHHockeyScores',           'D', 'FIH HOCKEY SCORES'),
    ( 'FIHHockeySchedues',     '10.4.2.197',  'FIHHockeySchedues_',        'FIHHockeySchedues',         'D', 'FIH HOCKEY SCHEDULES'),
    ( 'FIHStandings',          '10.4.2.197',  'FIHStandings_',             'FIHStandings',              'D', 'FIH STANDINGS'),
    ( 'NHLESPNSpiderSchedules', '10.4.2.197', 'NHLESPNSpiderSchedules_',   'NHLESPNSpiderSchedules',    'D', 'NHL ESPN SPIDER SCHEDULES'),
    ( 'NHLESPNSpiderScores',    '10.4.2.197', 'NHLESPNSpiderScores_',      'NHLESPNSpiderScores',       'D', 'NHL ESPN SPIDER SCORES'),
    ( 'ESPNNHLStandings',      '10.4.2.197',  'ESPNNHLStandings_',         'ESPNNHLStandings',          'D', 'ESPN NHL STANDINGS'),

    ( 'NflRoster',              '10.4.2.197', 'NflRoster_',                'NflRoster',                 'D', 'NFL ROSTER'),
    ( 'NFLSpiderSchedules',     '10.4.2.197', 'NFLSpiderSchedules_',       'NFLSpiderSchedules',        'D', 'NFL SPIDER SCHEDULES'),
    ( 'NFLESPNSpiderScores',    '10.4.2.197', 'NFLESPNSpiderScores_',      'NFLESPNSpiderScores',       'D', 'NFL SPIDER SCORES'),
    ( 'NFLESPNSpiderSchedules', '10.4.2.197', 'NFLESPNSpiderSchedules_',   'NFLESPNSpiderSchedules',    'D', 'NFL SPIDER SCHEDULES'),

    ( 'RosterCheck',            '10.4.2.197', 'RosterCheck_',              'RosterCheck',               'D', 'ROSTER CHECK'),
    ( 'CricketRosterCheck',     '10.4.2.197', 'CricketRosterCheck_',       'CricketRosterCheck',        'D', 'CRICKET ROSTER CHECK'),

    ( 'RulesFootballSchedules', '10.4.2.197', 'RulesFootballSchedules_',   'RulesFootballSchedules',    'D', 'AFL SCHEDULES'),
    ( 'RulesFootballScores',    '10.4.2.197', 'RulesFootballScores_',      'RulesFootballScores',       'D', 'AFL SCORES'),
    ( 'AflStandings',           '10.4.2.197', 'AflStandings_',             'AflStandings',              'D', 'AFL STANDINGS'),

    ( 'CFLSpider',              '10.4.2.197', 'CFLSpider_',                'CFLSpider',                 'D', 'CFL SPIDER'),
    ( 'CFLStandings',           '10.4.2.197', 'CFLStandings_',             'CFLStandings',              'D', 'CFL STANDINGS'),
    ( 'CFLRoster',              '10.4.2.197', 'CFLRoster_',                'CFLRoster',                 'D', 'CFL ROSTER'),

    ( 'NFLSpiderScores',        '10.4.2.197', 'NFLSpiderScores_',          'NFLSpiderScores',           'D', 'NFL SPIDER SCORES'),
    ( 'NFL_Standings',          '10.4.2.197', 'NFL_Standings_',            'NFL_Standings',             'D', 'NFL STANDINGS'),

    ( 'WnbaRoster',             '10.4.2.197', 'WnbaRoster_',               'WnbaRoster',                'D', 'WNBA ROSTER'),
    ( 'WnbaSpiderSchedules',    '10.4.2.197', 'WnbaSpiderSchedules_',      'WnbaSpiderSchedules',       'D', 'WNBA SPIDER SCHEDULES'),
    ( 'WnbaSpiderScores',       '10.4.2.197', 'WnbaSpiderScores_',         'WnbaSpiderScores',          'D', 'WNBA SPIDER SCORES'),
    ( 'WNBAStandings',          '10.4.2.197', 'WNBAStandings_',            'WNBAStandings',             'D', 'WNBA STANDINGS'),

    ( 'TennisSpiderSchedules',  '10.4.2.197', 'TennisSpiderSchedules_',    'TennisSpiderSchedules',     'D', 'TENNIS SPIDER SCHEDULES'),
    ( 'TennisSpiderScores',     '10.4.2.197', 'TennisSpiderScores_',       'TennisSpiderScores',        'D', 'TENNIS SPIDER SCORES'),
    ( 'Wimbledon',              '10.4.2.197', 'Wimbledon_',                'Wimbledon',                 'D', 'WIMBLEDON GAMES'),
    ( 'FrenchOpenSchedules',    '10.4.2.197', 'FrenchOpenSchedules_',      'FrenchOpenSchedules',       'D', 'FRENCH OPEN SCHEDULES'),
    ( 'FrenchOpenDraws',        '10.4.2.197', 'FrenchOpenDraws_',          'FrenchOpenDraws',           'D', 'FRENCH OPEN DRAWS'),
    ( 'FrenchOpenQualifying',   '10.4.2.197', 'FrenchOpenQualifying_',     'FrenchOpenQualifying',      'D', 'FRENCH OPEN QUALIIFYING'),
    ( 'UsopenTennis',           '10.4.2.197', 'UsopenTennis_',             'UsopenTennis',              'D', 'US OPEN TENNIS'),
    ( 'TennisFedCup',           '10.4.2.197', 'TennisFedCup_',             'TennisFedCup',              'D', 'TENNIS FED CUP'),
    ( 'IPTLTennis',             '10.4.2.197', 'IPTLTennis_',               'IPTLTennis',                'D', 'IPTL TENNIS'),
    ( 'IPTLStandings',          '10.4.2.197', 'IPTLStandings_',            'IPTLStandings',             'D', 'IPTL STANDINGS'),
    ( 'WorldTourTennisScores',  '10.4.2.197', 'WorldTourTennisScores_',    'WorldTourTennisScores',     'D', 'WORLD TOUR TENNIS SCORES'),
    ( 'WorldTourTennisSchedules', '10.4.2.197', 'WorldTourTennisSchedules_', 'WorldTourTennisSchedules', 'D', 'WORLD TOUR TENNIS SCHEDULES'),
    ( 'DavisCupSpider',         '10.4.2.197',  'DavisCupSpider_',          'DavisCupSpider',            'D',  'DAVIS CUP SPIDER'),
    ( 'DaviscupRoster',         '10.4.2.197',  'DaviscupRoster_',          'DaviscupRoster',            'D',  'DAVIS CUP ROSTER'),
    ( 'FedcupRoster',           '10.4.2.197',  'FedcupRoster_',            'FedcupRoster',              'D',  'FED CUP ROSTER'),
    ( 'HopmancupRoster',        '10.4.2.197',  'HopmancupRoster_',         'HopmancupRoster',           'D',  'HOPMAN CUP ROSTER'),
    ( 'HopmanCupSchedules',     '10.4.2.197',  'HopmanCupSchedules_',      'HopmanCupSchedules',        'D',  'HOPMAN CUP SCHEDULES'),
    ( 'HopmanCupScores',        '10.4.2.197',  'HopmanCupScores_',         'HopmanCupScores',           'D',   'HOPMAN CUP SCORES'),
    ( 'AtpWtaStandings',        '10.4.2.197',  'AtpWtaStandings_',         'AtpWtaStandings',           'D',   'ATP WTA STANDINGS'),
    ( 'USOpenTennisSeed',       '10.4.2.197',  'USOpenTennisSeed_',        'USOpenTennisSeed',          'D',  'US OPEN TENNIS SEED'),

    ( 'PGATourSchedules',       '10.4.2.197', 'PGATourSchedules_',         'PGATourSchedules',          'D', 'PGA TOUR SCHEDULES'),
    ( 'PGATourOngoing',         '10.4.2.197', 'PGATourOngoing_',           'PGATourOngoing',            'D', 'PGA TOUR ONGOING'),
    ( 'PGATourScores',          '10.4.2.197', 'PGATourScores_',            'PGATourScores',             'D', 'PGA TOUR SOCRES'),
    ( 'PGAStandings',           '10.4.2.197', 'PGAStandings_',             'PGAStandings',              'D', 'PGA STANDINGS'),
    ( 'LPGAGames',              '10.4.2.197', 'LPGAGames_',                'LPGAGames',                 'D', 'LPGA GAMES'),
    ( 'LPGAOngoing',            '10.4.2.197', 'LPGAOngoing_',              'LPGAOngoing',               'D', 'LPGA ONGOING'),
    ( 'LPGAStandings',          '10.4.2.197', 'LPGAStandings_',            'LPGAStandings',             'D', 'LPGA STANDINGS'),
    ( 'EuropeanTourSchedules',  '10.4.2.197', 'EuropeanTourSchedules_',    'EuropeanTourSchedules',     'D', 'EUROPEAN TOUR Schedules'),
    ( 'EuropeanTourOngoing',    '10.4.2.197', 'EuropeanTourOngoing_',      'EuropeanTourOngoing',       'D', 'EUROPEAN TOUR ONGOING'),
    ( 'EuropeanTourScores',     '10.4.2.197', 'EuropeanTourScores_',       'EuropeanTourScores',        'D', 'EUROPEAN TOUR SCORES'),
    ( 'PGAEuropeanStandings',   '10.4.2.197', 'PGAEuropeanStandings_',     'PGAEuropeanStandings',      'D', 'EUROPEAN STANDINGS'),
    ( 'ChampionsUsopenSpider',  '10.4.2.197', 'ChampionsUsopenSpider_',    'ChampionsUsopenSpider',     'D', 'CHAMPIONS US OPEN SPIDER'),
    ( 'ChampionsTour',          '10.4.2.197', 'ChampionsTour_',            'ChampionsTour',             'D', 'CHAMPIONS TOUR'),
    ( 'ChampionsOngoing',       '10.4.2.197', 'ChampionsOngoing_',         'ChampionsOngoing',          'D', 'CHAMPIONS ONGOING'),
    ( 'WebcomTour',             '10.4.2.197', 'WebcomTour_',               'WebcomTour',                'D', 'WEBCOM TOUR GAMES'),
    ( 'WebcomOngoing',          '10.4.2.197', 'WebcomOngoing_',            'WebcomOngoing',             'D', 'WEBCOM ONGOING'),
    ( 'SeniorTour',             '10.4.2.197', 'SeniorTour_',               'SeniorTour',                'D', 'SENIOR TOUR'),
    ( 'GolfSeniorTourScores',   '10.4.2.197', 'GolfSeniorTourScores_',     'GolfSeniorTourScores',      'D', 'GOLF SENIOR TOUR SCORES'),
    ( 'GolfSeniorTourSchedules', '10.4.2.197', 'GolfSeniorTourSchedules_', 'GolfSeniorTourSchedules',   'D', 'GOLF SENIOR TOUR SCHEDULES'),
    ( 'RyderCup',               '10.4.2.197',  'RyderCup_',                'RyderCup',                   'D', 'RYDERCUP'),
    ( 'AustraliaOpenGolf',      '10.4.2.197',  'AustraliaOpenGolf_',       'AustraliaOpenGolf',         'D', 'AUSTRALIA OPEN GOLF'),

    ( 'IccCricket',             '10.4.2.197', 'IccCricket_',               'IccCricket',                'D', 'ICC CRICKET'),
    ( 'CricketStandings',       '10.4.2.197', 'CricketStandings_',         'CricketStandings',          'D', 'CRICKET STANDINGS'),
    ( 'IPLStandings',           '10.4.2.197', 'IPLStandings_',             'IPLStandings',              'D', 'IPL STANDINGS'),
    ( 'IPLCricketRoster',       '10.4.2.197', 'IPLCricketRoster_',         'IPLCricketRoster',          'D',  'IPL CRICKET ROSTER'),
    ( 'ICCIntCupStandings',      '10.4.2.197', 'ICCIntCupStandings_',      'ICCIntCupStandings',        'D', 'ICC INTCUP STANDINGS'),
    ( 'ICCWorldCupStandings',   '10.4.2.197', 'ICCWorldCupStandings_',     'ICCWorldCupStandings',      'D', 'ICC WORLD CUP STANDINGS'),
    ( 'WCLCricketStandings',    '10.4.2.197', 'WCLCricketStandings_',      'WCLCricketStandings',       'D', 'WCL CRICKET STANDINGS'),
    ( 'CountyCricketStandings',  '10.4.2.197', 'CountyCricketStandings_',   'CountyCricketStandings',    'D',  'COUNTY CRICKET STANDINGS'),
    ( 'NatWestCricketStandings', '10.4.2.197', 'NatWestCricketStandings_',  'NatWestCricketStandings',   'D',  'NATWEST CRICKET STANDINGS'),
    ( 'CPLStandings',            '10.4.2.197', 'CPLStandings_',             'CPLStandings',              'D',  'CPL STANDINGS'),
    ( 'ICCInterCupRosters',       '10.4.2.197', 'ICCInterCupRosters_',      'ICCInterCupRosters',         'D',  'ICC INTER CUP ROSTERS'),
    ( 'WCLCricketRosters',       '10.4.2.197',  'WCLCricketRosters_',       'WCLCricketRosters',         'D', 'WCL CRICKET ROSTERS'),
    ( 'RoyalOneDayCupStandings',  '10.4.2.197', 'RoyalOneDayCupStandings_', 'RoyalOneDayCupStandings',   'D', 'ROYAL ONE DAY CUP STANDINGS'),
    ( 'CountyCricketRoster',     '10.4.2.197',  'CountyCricketRoster_',     'CountyCricketRoster',       'D', 'COUNTY CRICKET ROSTER'),
    ( 'HKPLStandings',           '10.4.2.197',  'HKPLStandings_',           'HKPLStandings',             'D', 'HKPL STANDINGS'),
    ( 'BigbashStandings',        '10.4.2.197',  'BigbashStandings_',        'BigbashStandings',          'D', 'BIG BASH STANDINGS'),


    ( 'UefaMajorLegauesScores', '10.4.2.197', 'UefaMajorLegauesScores_',   'UefaMajorLegauesScores',    'D', 'UEFA MAJOR LEAGUE SCORES'),
    ( 'UEFALeaguesRoster',      '10.4.2.197', 'UEFALeaguesRoster_',        'UEFALeaguesRoster',         'D', 'UEFA LEAGUES ROSTER'),
    ( 'UefaLeaguesSchedules',   '10.4.2.197', 'UefaLeaguesSchedules_',     'UefaLeaguesSchedules',      'D', 'UEFA LEAGUE SCHEDULES'),
    ( 'UefaLeaguesScores',      '10.4.2.197', 'UefaLeaguesScores_',        'UefaLeaguesScores',         'D', 'UEFA LEAGUE SCORES'),
    ( 'UEFALeaguesStandings',   '10.4.2.197', 'UEFALeaguesStandings_',     'UEFALeaguesStandings',      'D', 'UEFA LEAGUES STANDINGS'),
    ( 'UefaCupsScores',         '10.4.2.197', 'UefaCupsScores_',           'UefaCupsScores',            'D', 'UEFA CUPS SCORES'),
    ( 'UefaCupsSchedules',      '10.4.2.197', 'UefaCupsSchedules_',        'UefaCupsSchedules',         'D', 'UEFA CUPS SCHEDULES'),
    ( 'UefaAssociationLeaguesSchedules', '10.4.2.197', 'UefaAssociationLeaguesSchedules_', 'UefaAssociationLeaguesSchedules', 'D', 'UEFA ASSOC LEAGUE SCHEDULES'),
    ( 'UefaAssociationLeaguesScores', '10.4.2.197', 'UefaAssociationLeaguesScores_', 'UefaAssociationLeaguesScores', 'D', 'UEFA ASSOC LEAGUE SCORES'),
    ( 'ESPNSoccerSchedules',    '10.4.2.197', 'ESPNSoccerSchedules_',      'ESPNSoccerSchedules',       'D', 'ESPN SOCCER SCHEDULES'),
    ( 'ESPNGroupStandings',     '10.4.2.197', 'ESPNGroupStandings_',       'ESPNGroupStandings',        'D', 'ESPN GROUP STANDINGS'),
    ( 'EspnStandings',          '10.4.2.197', 'EspnStandings_',            'EspnStandings',             'D', 'ESPN SOCCER STANDINGS'),
    ( 'FIFAInternationalSchedules', '10.4.2.197', 'FIFAInternationalSchedules_',    'FIFAInternationalSchedules',   'D', 'FIFA INTERNATIONAL SCHEDULES'),
    ( 'FIFAInternationalScores',    '10.4.2.197', 'FIFAInternationalScores_',       'FIFAInternationalScores',      'D', 'FIFA INTERNATIONAL SCORES'),
    ( 'FIFATeamStandings',      '10.4.2.197', 'FIFATeamStandings_',        'FIFATeamStandings',         'D', 'FIFA TEAM STANDINGS'),
    ( 'FIFAStandings',          '10.4.2.197', 'FIFAStandings_',            'FIFAStandings',             'D', 'FIFA GROUP STANDINGS'),
    ( 'FIFAWorldCup',           '10.4.2.197', 'FIFAWorldCup_',             'FIFAWorldCup',              'D', 'FIFA WORLD CUP'),
    ( 'FIFAWorldCupQualifiers', '10.4.2.197', 'FIFAWorldCupQualifiers_',   'FIFAWorldCupQualifiers',    'D', 'FIFA WORLD CUP QUALIFIERS'),
    ( 'UEFAMajorLeaguesRoster', '10.4.2.197', 'UEFAMajorLeaguesRoster_',   'UEFAMajorLeaguesRoster',    'D', 'UEFA MAJOR LEAGUE ROSTER'),
    ( 'SoccerwaySoccerSchedules', '10.4.2.197', 'SoccerwaySoccerSchedules_', 'SoccerwaySoccerSchedules', 'D', 'SOCCERWAY SOCCER SCHEDULES'),
    ( 'SoccerwaySoccerScores', '10.4.2.197', 'SoccerwaySoccerScores_', 'SoccerwaySoccerScores', 'D', 'SOCCERWAY SOCCER SCORES'),
    ( 'SoccerwayInternationalSchedules', '10.4.2.197', 'SoccerwayInternationalSchedules_', 'SoccerwayInternationalSchedules', 'D', 'SOCCERWAY INTERNATIONAL SCHEDULES'),
    ( 'SoccerwayInternationalScores', '10.4.2.197', 'SoccerwayInternationalScores_', 'SoccerwayInternationalScores', 'D', 'SOCCERWAY INTERNATIONAL SCORES'),
    ( 'SoccerwayStandings',    '10.4.2.197', 'SoccerwayStandings_',        'SoccerwayStandings',        'D', 'SOCCERWAY SOCCER STANDINGS'),
    ( 'SoccerwayRoster',       '10.4.2.197', 'SoccerwayRoster_',           'SoccerwayRoster',           'D', 'SOCCERWAY SOCCER ROSTER'),
    ( 'SoccerwaySoccerFacupSchedules',  '10.4.2.197', 'SoccerwaySoccerFacupSchedules_', 'SoccerwaySoccerFacupSchedules', 'D', 'SOCCERWAY FACUP SCHEDULES'),
    ( 'SoccerwaySoccerFacupScores',     '10.4.2.197', 'SoccerwaySoccerFacupScores_',    'SoccerwaySoccerFacupScores',    'D', 'SOCCERWAY FACUP SCORES'),
    ( 'MLSStandings',          '10.4.2.197', 'MLSStandings_',              'MLSStandings',              'D', 'MLS SOCCER STANDINGS'),
    ( 'FIFAClubWorldCup',      '10.4.2.197', 'FIFAClubWorldCup_',          'FIFAClubWorldCup',          'D', 'FIFA CLUB WORLD CUP'),
    ( 'AFCAsianCup',            '10.4.2.197', 'AFCAsianCup_',              'AFCAsianCup',               'D', 'AFC ASIAN CUP'),
    ( 'AFCStandings',           '10.4.2.197', 'AFCStandings_',             'AFCStandings',              'D', 'AFC STANDINGS'),
    ( 'FIFAWomenStandings',     '10.4.2.197', 'FIFAWomenStandings_',       'FIFAWomenStandings',        'D', 'FIFA WOMENS STANDINGS'),
    ( 'CONCACAFSoccer',         '10.4.2.197', 'CONCACAFSoccer_',           'CONCACAFSoccer',            'D', 'CONCACAF SOCCER'),
    ( 'CCLStandings',           '10.4.2.197', 'CCLStandings_',             'CCLStandings',              'D', 'CCL STANDINGS'),
    ( 'CONCACAFU20',            '10.4.2.197', 'CONCACAFU20_',              'CONCACAFU20',               'D', 'CONCACAF U20'),
    ( 'CONCACAFU20Standings',   '10.4.2.197', 'CONCACAFU20Standings_',     'CONCACAFU20Standings',      'D', 'CONCACAF U20 STANDINGS'),
    ( 'MLSSoccerSchedules',     '10.4.2.197', 'MLSSoccerSchedules_',       'MLSSoccerSchedules',        'D', 'MLS SOCCER SCHEDULES'),
    ( 'MLSSoccerScores',        '10.4.2.197', 'MLSSoccerScores_',          'MLSSoccerScores',           'D', 'MLS SOCCER SCORES'),
    ( 'BallonAwards',           '10.4.2.197', 'BallonAwards_',             'BallonAwards',              'D', 'BALLON AWARDS'),
    ( 'CSLSoccerSchedules',     '10.4.2.197', 'CSLSoccerSchedules_',       'CSLSoccerSchedules',        'D', 'CSL SCHEDULES'),
    ( 'CSLSoccerScores',        '10.4.2.197', 'CSLSoccerScores_',          'CSLSoccerScores',           'D', 'CSL SCORES'),


    ( 'UfcSpiderSchedules',     '10.4.2.197', 'UfcSpiderSchedules_',       'UfcSpiderSchedules',        'D', 'UFC SPIDER SCHEDULES'),
    ( 'UfcSpiderScores',        '10.4.2.197', 'UfcSpiderScores_',          'UfcSpiderScores',           'D', 'UFC SPIDER SCORES'),

    ( 'WBABoxingSpiderSchedules', '10.4.2.197', 'WBABoxingSpiderSchedules_', 'WBABoxingSpiderSchedules', 'D', 'WBA BOXING SPIDER SCHEDULES'),
    ( 'WBABoxingSpiderScores',  '10.4.2.197',   'WBABoxingSpiderScores_',   'WBABoxingSpiderScores',     'D', 'WBA BOXING SPIDER SCORES'),

    ( 'NCAASpiderScores',       '10.4.2.197', 'NCAASpiderScores_',         'NCAASpiderScores',          'D', 'NCAA SPIDER SCORES'),
    ( 'NCAASpiderSchedules',    '10.4.2.197', 'NCAASpiderSchedules_',      'NCAASpiderSchedules',       'D', 'NCAA SPIDER SCHEDULES'),
    ( 'NcfSpiderScores',        '10.4.2.197', 'NcfSpiderScores_',          'NcfSpiderScores',           'D', 'NCF SPIDER SCORES'),
    ( 'NcfSpiderSchedules',     '10.4.2.197', 'NcfSpiderSchedules_',       'NcfSpiderSchedules',        'D', 'NCF SPIDER SCHEDULES'),
    ( 'NCAAFootballScores',     '10.4.2.197', 'NCAAFootballScores_',       'NCAAFootballScores',        'D', 'NCAA FOOTBALL SCORES'),
    ( 'NCAAFootballSchedules',  '10.4.2.197', 'NCAAFootballSchedules_',    'NCAAFootballSchedules',     'D', 'NCAA FOOTBALL SCHEDULES'),
    ( 'NCBRoster',              '10.4.2.197', 'NCBRoster_',                'NCBRoster',                 'D', 'NCB ROSTER'),
    ( 'NCBStandings',           '10.4.2.197', 'NCBStandings_',             'NCBStandings',              'D', 'NCB STANDINGS'),
    ( 'NCAANCFStandings',       '10.4.2.197', 'NCAANCFStandings_',         'NCAANCFStandings',          'D', 'NCAA NCF STANDINGS'),
    ( 'NCFTouStandings',        '10.4.2.197', 'NCFTouStandings_',          'NCFTouStandings',           'D', 'NCF TOU STANDINGS'),

    ( 'RugbySpiderScores',      '10.4.2.197', 'RugbySpiderScores_',        'RugbySpiderScores',         'D', 'RUGBY SPIDER SCORES'),
    ( 'RugbySpiderSchedules',   '10.4.2.197', 'RugbySpiderSchedules_',     'RugbySpiderSchedules',      'D', 'RUGBY SPIDER SCHEDULES'),
    ( 'RugbyStandings',         '10.4.2.197', 'RugbyStandings_',           'RugbyStandings',            'D', 'RUGBY SIXNATIONS STANDINGS'),
    ( 'NRLRugby',               '10.4.2.197', 'NRLRugby_',                 'NRLRugby',                  'D', 'NRL RUGBY'),
    ( 'EuropeRugbyStandings',   '10.4.2.197', 'EuropeRugbyStandings_',     'EuropeRugbyStandings',      'D', 'EUROPE RUGBY STANDINGS'),
    ( 'RugbyEuropeSpiderSchedules', '10.4.2.197', 'RugbyEuropeSpiderSchedules_', 'RugbyEuropeSpiderSchedules', 'D', 'RUGBY EUROPE SPIDER SCHEDULES'),
    ( 'RugbyEuropeSpiderScores', '10.4.2.197', 'RugbyEuropeSpiderScores_', 'RugbyEuropeSpiderScores', 'D', 'RUGBY EUROPE SPIDER SCORES'),
    ( 'RFUChampionsSchedules',   '10.4.2.197', 'RFUChampionsSchedules_',   'RFUChampionsSchedules',     'D', 'RUGBY CHAMPIONS SCHEDULES'),
    ( 'RFUChampionsScores',      '10.4.2.197', 'RFUChampionsScores_',      'RFUChampionsScores',        'D', 'RUGBY CHAMPIONS SCORES'),
    ( 'RFUChampionsStandings',   '10.4.2.197', 'RFUChampionsStandings_',   'RFUChampionsStandings',     'D', 'RUGBY CHAMPIONS STANDINGS'),
    ( 'SuperRugbyStandings',     '10.4.2.197', 'SuperRugbyStandings_',     'SuperRugbyStandings',       'D', 'SUPER RUGBY STANDINGS'),
    ( 'SuperRugbySpiderScores',  '10.4.2.197', 'SuperRugbySpiderScores_',  'SuperRugbySpiderScores',    'D', 'SUPER RUGBY SPIDER SCORES'),
    ( 'SuperRugbySpiderSchedules', '10.4.2.197', 'SuperRugbySpiderSchedules_', 'SuperRugbySpiderSchedules', 'D', 'SUPER RUGBY SPIDER SCHEDULES'),
    ( 'ITMCupRugbySpider',      '10.4.2.197',  'ITMCupRugbySpider_',       'ITMCupRugbySpider',         'D', 'ITM CUP RUGBY SPIDER'),
    ( 'ITMCupRugbyStandings',   '10.4.2.197',  'ITMCupRugbyStandings_',    'ITMCupRugbyStandings',      'D', 'ITM CUP RUGBY STANDINGS'),
    ( 'HeartlandRugby',         '10.4.2.197',  'HeartlandRugby_',          'HeartlandRugby',            'D', 'HEARTLAND RUGBY'),
    ( 'SuperLeagueRugbyScores',  '10.4.2.197',  'SuperLeagueRugbyScores_',  'SuperLeagueRugbyScores',     'D', 'SUPER LEAGUE RUGBY SCORES'),
    ( 'SuperLeagueRugbySchedules', '10.4.2.197', 'SuperLeagueRugbySchedules_', 'SuperLeagueRugbySchedules', 'D', 'SUPER LEAGUE RUGBY SCHEDULES'),
    ( 'SuperLeagueStandings',    '10.4.2.197',  'SuperLeagueStandings_',   'SuperLeagueStandings',      'D',  'SUPER LEAGUE STANDINGS'),
    ( 'SuperRugbyRoster',        '10.4.2.197',  'SuperRugbyRoster_',       'SuperRugbyRoster',          'D', 'SUPER RUGBY ROSTER'),
    ( 'HeartlandStandings',      '10.4.2.197',  'HeartlandStandings_',     'HeartlandStandings',        'D', 'HEARTLAND STANDINGS'),
    ( 'RugbyWorldCupRoster',     '10.4.2.197',  'RugbyWorldCupRoster_',    'RugbyWorldCupRoster',        'D', 'RUGBY WORLD CUP ROSTER'),
    ( 'RugbyWorldCup',           '10.4.2.197',  'RugbyWorldCup_',          'RugbyWorldCup',              'D', 'RUGBY WORLD CUP'),
    ( 'RugbyWorldCupStandings',  '10.4.2.197',  'RugbyWorldCupStandings_', 'RugbyWorldCupStandings',     'D', 'RUGBY WORLD CUP STANDINGS'),
    ( 'RugbyChampStandings',     '10.4.2.197',  'RugbyChampStandings_',    'RugbyChampStandings',        'D', 'RUGBY CHAMP STANDINGS'),

    ( 'MotogpSpider',           '10.4.2.197', 'MotogpSpider_',             'MotogpSpider',              'D', 'MOTOGP SPIDER'),
    ( 'MotoStandings',          '10.4.2.197', 'MotoStandings_',            'MotoStandings',             'D', 'MOTO STANDINGS'),
    ( 'RacingStandings',        '10.4.2.197', 'RacingStandings_',          'RacingStandings',           'D', 'AUTO RACING STANDINGS'),
    ( 'NascarStandings',        '10.4.2.197', 'NascarStandings_',          'NascarStandings',           'D', 'NASCAR STANDINGS'),
    ( 'F1Standings',            '10.4.2.197', 'F1Standings_',              'F1Standings',               'D', 'F1 STANDINGS'),
    ( 'F1Winner',               '10.4.2.197', 'F1Winner_',                 'F1Winner',                  'D', 'F1 WINNER'),

    ( 'MarathonStandings',      '10.4.2.197', 'MarathonStandings_',        'MarathonStandings',         'D', 'MARATHON STANDINGS'),

    ( 'WACMedals',              '10.4.2.197', 'WACMedals_',                'WACMedals',                 'D', 'WAC STANDINGS'),
    ( 'WCAMedals',              '10.4.2.197', 'WCAMedals_',                'WCAMedals',                 'D', 'WCA STANDINGS'),

    ( 'UCIWorldTour',           '10.4.2.197', 'UCIWorldTour_',             'UCIWorldTour',              'D', 'UCI CYCLING WORLD TOUR'),
    ( 'CyclingStandings',       '10.4.2.197', 'CyclingStandings_',         'CyclingStandings',          'D', 'CYCLING STANDINGS'),
    ( 'LetourGames',            '10.4.2.197', 'LetourGames_',              'LetourGames',               'D', 'LETOUR GAMES'),
    ( 'LetourStandings',        '10.4.2.197', 'LetourStandings_',          'LetourStandings',           'D', 'LETOUR STANDINGS'),
    ( 'LetourGroupStandings',   '10.4.2.197', 'LetourGroupStandings_',     'LetourGroupStandings',      'D', 'LETOUR GROUPSTANDINGS'),
    ( 'UCIWomensStandings',     '10.4.2.197', 'UCIWomensStandings_',       'UCIWomensStandings',        'D',  'UCI WOMENS STANDINGS'),
    ( 'UCIWomensWorldcupScores', '10.4.2.197', 'UCIWomensWorldcupScores_', 'UCIWomensWorldcupScores',   'D',  'UCI WOMENS WORLDCUP SCORES'),
    ( 'UCIWomensWorldcupSchedules', '10.4.2.197', 'UCIWomensWorldcupSchedules_', 'UCIWomensWorldcupSchedules', 'D', 'UCI WOMENS WORLDCUP SCHEDULES'),
    ( 'UCIWomensTour',         '10.4.2.197',   'UCIWomensTour_',           'UCIWomensTour',            'D',  'UCI WOMENS TOUR'),

    ( 'ChessRapidScores',       '10.4.2.197', 'ChessRapidScores_',         'ChessRapidScores',          'D', 'CHESS RAPID GAMES'),
    ( 'ChessBlitzScores',       '10.4.2.197', 'ChessBlitzScores_',         'ChessBlitzScores',          'D', 'CHESS BLITZ GAMES'),
    ( 'RapidBlitzStandings',    '10.4.2.197', 'RapidBlitzStandings_',      'RapidBlitzStandings',       'D', 'RAPID & BLITZ STANDINGS'),
    ( 'WorldRapidBlitzStandings',   '10.4.2.197', 'WorldRapidBlitzStandings_',      'WorldRapidBlitzStandings',     'D', 'WORLD RAPID & BLITZ STANDINGS'),
    ( 'WorldChess',             '10.4.2.197', 'WorldChess_',               'WorldChess',                'D', 'WORLD CHESS SPIDER'),
    ( 'ChessGrandPrix',         '10.4.2.197', 'ChessGrandPrix_',           'ChessGrandPrix',            'D', 'CHESS FIDE GRAND PRIX'),

    ( 'FieldHockeyRoster',      '10.4.2.197', 'FieldHockeyRoster_',        'FieldHockeyRoster',         'D', 'FIELD HOCKEY ROSTER'),
    ( 'FieldHockeySchedules',   '10.4.2.197', 'FieldHockeySchedules_',     'FieldHockeySchedules',      'D', 'FIELD HOCKEY SCHEDULES'),
    ( 'FieldHockeyScores',      '10.4.2.197', 'FieldHockeyScores_',        'FieldHockeyScores',         'D', 'FIELD HOCKEY SCORES'),

    ( 'CommonWealthGames',      '10.4.2.197', 'CommonWealthGames_',        'CommonWealthGames',         'D', 'COMMONWEALTH GAMES'),
    ( 'CWStandings',            '10.4.2.197', 'CWStandings_',              'CWStandings',               'D', 'COMMONWEALTH STANDINGS'),
    ( 'CommonWealthTeamGames',  '10.4.2.197', 'CommonWealthTeamGames_',    'CommonWealthTeamGames',     'D', 'COMMONWEALTH TEAM GAMES'),
    ( 'CommonWealthHockeyGames', '10.4.2.197', 'CommonWealthHockeyGames_',  'CommonWealthHockeyGames',   'D', 'COMMONWEALTH HOCKEY GAMES'),
    ( 'CricketRoster',          '10.4.2.197', 'CricketRoster_',             'CricketRoster',              'D', 'CRICKET ROSTER'),
    ( 'FIBASpider',             '10.4.2.197', 'FIBASpider_',                'FIBASpider',                 'D',  'FIBA SPIDER'),
    ( 'CollegeWorldSeries',     '10.4.2.197', 'CollegeWorldSeries_',        'CollegeWorldSeries',         'D',  'COLLEGE WORLD SERIES SCHEDULES'),
    ( 'NCFRoster',              '10.4.2.197', 'NCFRoster_',                 'NCFRoster',                 'D',  'NCF ROSTER'),
    ( 'SuperLeague',            '10.4.2.197', 'SuperLeague_',               'SuperLeague',               'D',  'INDIAN SUPER LEAGUE'),
    ( 'NCAABowl',               '10.4.2.197', 'NCAABowl_',                  'NCAABowl',                  'D',  'NCAA BOWL GAMES'),
    ( 'MLBMVPSpider',           '10.4.2.197', 'MLBMVPSpider_',              'MLBMVPSpider',              'D',  'MLB MVP'),
    ( 'ISLStandings',           '10.4.2.197', 'ISLStandings_',              'ISLStandings',              'D',  'ISL STANDINGS'),
    ( 'RacingGames',            '10.4.2.197', 'RacingGames_',               'RacingGames',               'D',  'AUTO RACING GAMES'),
    ( 'UEFAGroupStandings',     '10.4.2.197', 'UEFAGroupStandings_',        'UEFAGroupStandings',        'D',  'UEFA GROUP STANDINGS'),

    ( 'EOCWaterPolo',           '10.4.2.197', 'EOCWaterPolo_',              'EOCWaterPolo',              'D',  'EOC WATER POLO'),
]

JINJA_LOOP_FORMAT_STR = """
{%% if loop.index == %d %%}
   <tr>
   <th colspan=%d><a href="http://%s/REPORTS/">%s</a></th>
   </tr>
{%% endif %%}
"""

JINJA_ROW_FORMAT_STR = """
{%% if key.startswith('%s') %%}
<tr>
%s
</tr>
{%% endif %%}
"""

JINJA_COLUMN_FORMAT_STR = '<th colspan=%d>%s</th>'


MAX_DAYS = 30


class Report(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)

        my_name = 'SPIDERS_SUMMARY'

        self.OUT_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, my_name)

        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, my_name)
        self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)

        self.BASE_DATE = (datetime.now() - timedelta(MAX_DAYS)).date()

        date_yesterday = datetime.now() - timedelta(1)
        self.run_date = date_yesterday.strftime("%Y-%m-%d")
        self.run_graph_date = date_yesterday.strftime("%Y%m%d")

        if self.options.debug:
            prefix = 'DEBUG_'
        else:
            prefix = ''
        self.summary_file_name = os.path.join(AUTO_REPORT_DIR, '%s%s' % (prefix, SUMMARY_FILE_NAME))

    def set_options(self):
        self.parser.add_option('', '--run', default='all', help='all, report, scp, html')
        self.parser.add_option('', '--only-scp', help='if some directory names are given, only those directories will be scped in scp phase')

    def get_latest_file_remote(self, host, path_pattern):
        ls_cmd = 'ls -t %s' % path_pattern
        status, process = ssh_utils.ssh_cmd_output(host, self.vtv_username, self.vtv_password, cmd = ls_cmd)
        if status == 0:
            file_list = process.before.split('\r\n')
            if len(file_list) > 1:
                return file_list[1]

        return None

    def run_report(self):
        report_cmd = 'cd %s; python data_report.%s --report-dir /data/REPORTS' % (VTV_SERVER_DIR, VTV_PY_EXT)

        ip_list = [ dir_info[1] for dir_info in STATS_DIR_LIST ]
        ip_list = list(set(ip_list))
        for ip in ip_list:
            print 'Run Report: %s' % ip

            #Remotely running of data_report.py
            status, process = ssh_utils.ssh_cmd_output(ip, self.vtv_username, self.vtv_password, report_cmd)
            if status != 0:
                ssh_utils.ssh_cmd_output(ip, self.vtv_username, self.vtv_password, report_cmd.replace(VTV_PY_EXT, 'pyc'))

            # Deleting log file
            status_del = ssh_utils.ssh_cmd(ip, self.vtv_username, self.vtv_password, 'rm -f %s/data_report*.log' % VTV_SERVER_DIR)

        print 'Done - HTML Generation'

    def run_scp(self):
        for dir_info in STATS_DIR_LIST:
            local_dir_name, ip, report_prefix, remote_dir_name, date_opt, title = dir_info

            if self.options.only_scp and local_dir_name not in self.options.only_scp:
                continue

            if date_opt == 'N':
                date_str = self.run_graph_date
            else:
                date_str = self.run_date

            pickle_file_pattern = os.path.join(SCP_SRC_DIR, remote_dir_name, '%s%s*.pickle' % (report_prefix, date_str))

            pickle_file_name = self.get_latest_file_remote(ip, pickle_file_pattern)
            if not pickle_file_name:
                self.logger.error('Cannot SCP : %s : %s to %s' % (ip, pickle_file_pattern, local_dir_name))
                print 'Error', 'SCP: %s : %s to %s' % (ip, pickle_file_pattern, local_dir_name)
                continue

            src = '-p %s@%s:%s' % (self.vtv_username, ip, pickle_file_name)
            dst = os.path.join(SCP_DST_DIR, local_dir_name)
            make_dir(dst)

            status = ssh_utils.scp(self.vtv_password, src, dst)
            if status:
                self.logger.error('SCP failed for: %s %s' % (src, dst))
            else:
                self.logger.info('SCP succeeded for: %s %s' % (src, dst))

        print 'Done - SCP Pickle files from machines'

    def init_summary(self):
        pass

    def get_files_in_date_range(self, base_file_name, start_date, end_date = None, num_of_days = 0, suffix = '', time_format = TIMEFORMAT):
        start_date = datetime.strptime(start_date, time_format)
        if end_date:
            end_date = datetime.strptime(end_date, time_format)
        else:
            end_date = start_date - timedelta(days=num_of_days)
        file_list = []
        while start_date >= end_date:
            filename = "%s%s%s" % (base_file_name, start_date.strftime(time_format), suffix)
            file_list.append(filename)
            start_date -= timedelta(days=1)
        return file_list

    def get_stats_dict(self, need_stats_name_list, stats_obj, op = 'single'):
        stats_str, stats_list = stats_obj['stats']
        stats_dict = {}

        if op == 'union' and not need_stats_name_list[0]:
            stats_dict = dict(stats_list)
            return stats_dict

        if op in ['nested', 'union']:
            first_dict_list = need_stats_name_list[0]
        else:
            first_dict_list = need_stats_name_list

        for stats_name, stats_dict in stats_list:
            if stats_name in first_dict_list:
                break
        else:
            return {}

        if op in ['nested', 'union']:
            found = True
            for stats_dict_list in need_stats_name_list[1:]:
                found = False
                old_stats_dict = stats_dict
                for stats_dict_name in stats_dict_list:
                    stats_dict = old_stats_dict.get(stats_dict_name, {})
                    if stats_dict:
                        found = True
                        break
                else:
                    break
            if not found:
                return {}

        return stats_dict

    def get_aggregate_stats_dict(self, obj):
        stats_dict = obj.get('aggregate_stats', {})
        return stats_dict

    def get_dict_stats(self, row_list, stats_list, stats_dict):
        if isinstance(stats_dict, dict):
            for key in stats_list:
                value = stats_dict.get(key, 0)
                row_list.append(value)
        else:
            row_list.extend([ 0 ] * len(stats_list))

    def get_stats_obj(self, dir_names, obj):
        empty = { 'stats' : ('', []) }
        if not obj:
            return empty

        dir_list = dir_names.split('#')
        for new_script in dir_list:
            if new_script in obj:
                return obj[new_script]
        else:
            self.logger.error('%s %s' % (new_script, dir_list))
            return empty

    def get_stats_obj_list(self, dir_names_list, obj):
        return [ self.get_stats_obj(dir_names, obj) for dir_names in dir_names_list ]

    def get_common_header(self, heading, stats_func):
        COMMON_HEADER = [ 'Date', 'Status', 'Failed Script', 'Final Report', 'Incr Report', 'Overall Time', 'Mem-Peak', 'Mem-Diff' ]

        column_list = [ JINJA_COLUMN_FORMAT_STR % (len(COMMON_HEADER), 'Program Stats') ]

        if stats_func:
            stats_func([], None)
            header_info = self.stats_header[0]
            for i, stats_info in enumerate(self.stats_header[1:]):
                column_list.append(JINJA_COLUMN_FORMAT_STR % (len(stats_info), self.group_header[i]))
        else:
            header_info = []

        table_format = JINJA_ROW_FORMAT_STR % (heading, '\n'.join(column_list))

        header_row = COMMON_HEADER + header_info

        return header_row, table_format

    def get_percent_color(self, percent):
        if percent <= 50:
            color = 'red'
        elif percent <= 70:
            color = 'orange'
        else:
            color = 'black'
        return color

    def get_time_function_name(self, initial_val):
        split_val = initial_val.split(':')
        int_val = map(int, split_val)
        timein_sec = int_val[0]*3600+int_val[1]*60+int_val[2]
        return timein_sec

    def get_time_color(self, peak_seconds):
        if peak_seconds >= 3 * 3600:
            color = 'red'
        elif peak_seconds >= 2 * 3600:
            color = 'orange'
        else:
            color = 'black'
        return color

    def get_time_stats(self, obj):
        peak_time = ''
        try:
            time_value = []
            for script_name in obj:
                initial_val = obj[script_name]['time'][0][2][0:7]
                timein_sec = self.get_time_function_name(initial_val)
                time_value.append((timein_sec, initial_val))
            if time_value:
                time_value.sort(reverse=True)
                peak_seconds, peak_time = time_value[0]
                peak_time = '<font color="%s">%s</font>' % (self.get_time_color(peak_seconds), peak_time)
        except (ValueError, IndexError):
            pass

        return peak_time

    def get_memory_color(self, memory):
        if memory >= TEN_GB:
            color = 'red'
        elif memory >= FIVE_GB:
            color = 'orange'
        else:
            color = 'black'
        return color

    def get_memory_stats(self, obj):
        peak_memory = ''
        peak_diff = ''
        memory_peak_values = []
        memory_diff_values = []
        try:
            for script_name in obj:
                memory_peak = round(obj[script_name]['memory']['Main'][1][4]/1000.00/1000.00, 2)
                memory_diff = round((obj[script_name]['memory']['Main'][1][4] - obj[script_name]['memory']['Main'][0][4])/1000.0/1000.0, 2)
                memory_peak_values.append(memory_peak)
                memory_diff_values.append(memory_diff)
                memory_peak_values.sort(reverse=True)
                memory_diff_values.sort(reverse=True)

            memory = memory_peak_values[0]
            peak_memory = '<font color="%s">%10.2fG</font>' % (self.get_memory_color(memory), memory)
            memory = memory_diff_values[0]
            peak_diff = '<font color="%s">%10.2fG</font>' % (self.get_memory_color(memory), memory)
        except (IndexError, TypeError):
            pass

        return peak_memory, peak_diff

    def get_general_header(self):
        HEADER = ['inserted', 'updated', 'skipped', 'responses', 'runs']
        CRAWL_STATS = ['inserted', 'updated', 'skipped', 'responses', 'runs']
        self.group_header = ['Crawl Stats']
        return [ HEADER, CRAWL_STATS]


    def get_sports_header(self):
        HEADER = ['games', 'completed', 'winners', 'inserted', 'updated', 'skipped', 'responses', 'runs']
        SPORTS_STATS = ['games', 'completed', 'winners']
        CRAWL_STATS = ['inserted', 'updated', 'skipped', 'responses', 'runs']
        self.group_header = ['Sports Stats', 'Crawl Stats']
        return [ HEADER, SPORTS_STATS, CRAWL_STATS]

    def get_spider_stats(self, row_list, obj):
        if not row_list:
            self.stats_header = self.get_sports_header()
            return

        HEADER, SPORTS_STATS, CRAWL_STATS = self.stats_header

        STATS_FILE_LIST = [ self.local_dir_name, 'games_today.py' ]
        stats_obj_list = self.get_stats_obj_list(STATS_FILE_LIST, obj)
        stats_obj = stats_obj_list[0]
        game_stats_obj = stats_obj_list[1]

        game_stats_dict = self.get_stats_dict([ 'stats' ], game_stats_obj)
        self.get_dict_stats(row_list, SPORTS_STATS, game_stats_dict)

        stats_dict = self.get_stats_dict([ self.local_dir_name ], stats_obj)
        self.get_dict_stats(row_list, CRAWL_STATS, stats_dict)

    def get_common_stats(self, row_list, obj):
        if not row_list:
            self.stats_header = self.get_general_header()
            return

        HEADER, CRAWL_STATS = self.stats_header

        STATS_FILE_LIST = [ self.local_dir_name ]
        stats_obj_list = self.get_stats_obj_list(STATS_FILE_LIST, obj)

        stats_dict = self.get_stats_dict([ self.local_dir_name ], stats_obj_list[0])
        self.get_dict_stats(row_list, CRAWL_STATS, stats_dict)


    def get_status_color(self, status):
        COLOR_DICT = { 'SUCCESS' : 'badge-success', 'FAILURE' : 'badge-important', 'DORMANT' : 'badge-warning' }

        return '<span class="badge %s">%s</span>' % (COLOR_DICT.get(status, ''), status)

    def get_program_stats(self, row_list, obj, actual_file_name):
        date_str = self.current_date.strftime(TIMEFORMAT)
        mem_obj = copy.deepcopy(obj)

        if mem_obj:
            mem_obj.pop('games_today.py', None)

        if not mem_obj:
            status_str = self.get_status_color('DORMANT')
            row_list.extend([ date_str, status_str ])
            return False

        #Status Display
        full_status = True
        failed_script = ''
        for script_name in mem_obj:
            status_flag = mem_obj[script_name]['status'][0]
            if status_flag:
                full_status = False
                failed_script = script_name

        if full_status:
            status_str = self.get_status_color('SUCCESS')
        else:
            status_str = self.get_status_color('FAILURE')

        #Hyperlink to html files
        pickle_file_name = os.path.basename(actual_file_name)
        link = "http://%s/REPORTS/%s/%s" % (self.ip, self.remote_dir_name, pickle_file_name)
        final_link = "<a href='%s'>Final</a>" % link.replace('.pickle', '_final.html')
        incr_link = "<a href='%s'>Increment</a>" % link.replace('.pickle', '.html')

        peak_time = self.get_time_stats(mem_obj)

        peak_memory, peak_diff = self.get_memory_stats(mem_obj)

        row_list.extend([date_str, status_str, failed_script, final_link, incr_link, peak_time, peak_memory, peak_diff])

        return full_status

    def calculate_diff(self, actual_file_name, domain_list, row_list, common_length):
        if len(domain_list) <= 1:
            return

        for i in reversed(range(len(domain_list))):
            date_str, status_str = domain_list[i][:2]
            if 'SUCCESS' in status_str:
                break
        else:
            return

        prev_row_list = domain_list[i]
        if len(prev_row_list) != len(row_list):
            print actual_file_name, len(prev_row_list), len(row_list)
            return

        for i, value in enumerate(row_list):
            if i < common_length:
                continue

            try:
                value = int(value)
                last_value = int(prev_row_list[i])
            except ValueError:
                continue
            except TypeError:
                print actual_file_name, domain_list, row_list
                raise

            INFO_VALUE = 5
            diff_value = abs(last_value - value)

            direction, color = '', ''
            if diff_value > 0 and diff_value <= INFO_VALUE:
                color = 'warning'
            if last_value > value:
                direction = 'up'
                if not color:
                    color = 'success'
            elif last_value < value:
                direction = 'down'
                if not color:
                    color = 'important'

            if direction and color:
                prev_row_list[i] = '<span class="badge badge-%s">%s<i class="icon-arrow-%s"></i></span>' % (color, last_value, direction)

    def generate_summary(self):
        DUMMY = ''
        STATS_FUNC_DICT = {
             'MLBSpiderSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'MLBSpiderScores'         : ( self.get_spider_stats,       DUMMY ),
             'NBASpiderSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'NBASpiderScores'         : ( self.get_spider_stats,       DUMMY ),
             'NHLSpiderSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'NHLSpiderScores'         : ( self.get_spider_stats,       DUMMY ),
             'NFLSpiderSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'RosterCheck'             : ( self.get_spider_stats,       DUMMY ),
             'NFLSpiderScores'         : ( self.get_spider_stats,       DUMMY ),
             'ESPNSoccerSchedules'     : ( self.get_spider_stats,       DUMMY ),
             'FrenchOpenSchedules'     : ( self.get_spider_stats,       DUMMY ),
             'FrenchOpenDraws'         : ( self.get_spider_stats,       DUMMY ),
             'FrenchOpenQualifying'    : ( self.get_spider_stats,       DUMMY ),
             'Wimbledon'               : ( self.get_spider_stats,       DUMMY ),
             'UefaMajorLegauesScores'  : ( self.get_spider_stats,       DUMMY ),
             'UefaLeaguesSchedules'    : ( self.get_spider_stats,       DUMMY ),
             'UefaLeaguesScores'       : ( self.get_spider_stats,       DUMMY ),
             'IccCricket'              : ( self.get_spider_stats,       DUMMY ),
             'UefaCupsScores'          : ( self.get_spider_stats,       DUMMY ),
             'NFL_Standings'           : ( self.get_common_stats,       DUMMY ),
             'RugbyStandings'          : ( self.get_common_stats,       DUMMY ),
             'RacingStandings'         : ( self.get_common_stats,       DUMMY ),
             'NascarStandings'         : ( self.get_common_stats,       DUMMY ),
             'NHL_Standings'           : ( self.get_common_stats,       DUMMY ),
             'CyclingStandings'        : ( self.get_common_stats,       DUMMY ),
             'UfcSpiderSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'UfcSpiderScores'         : ( self.get_spider_stats,       DUMMY ),
             'WnbaSpiderSchedules'     : ( self.get_spider_stats,       DUMMY ),
             'WnbaSpiderScores'        : ( self.get_spider_stats,       DUMMY ),
             'NcfSpiderScores'         : ( self.get_spider_stats,       DUMMY ),
             'NcfSpiderSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'NCAASpiderScores'        : ( self.get_spider_stats,       DUMMY ),
             'NCAASpiderSchedules'     : ( self.get_spider_stats,       DUMMY ),
             'RugbySpiderScores'       : ( self.get_spider_stats,       DUMMY ),
             'RugbySpiderSchedules'    : ( self.get_spider_stats,       DUMMY ),
             'MotoStandings'           : ( self.get_common_stats,       DUMMY ),
             'ChessRapidScores'        : ( self.get_spider_stats,       DUMMY ),
             'ChessBlitzScores'        : ( self.get_spider_stats,       DUMMY ),
             'RapidBlitzStandings'     : ( self.get_common_stats,       DUMMY ),
             'WorldRapidBlitzStandings': ( self.get_common_stats,       DUMMY ),
             'WorldChess'              : ( self.get_common_stats,       DUMMY ),
             'FIFAStandings'           : ( self.get_common_stats,       DUMMY ),
             'FIFATeamStandings'       : ( self.get_common_stats,       DUMMY ),
             'IPLStandings'            : ( self.get_common_stats,       DUMMY ),
             'EspnStandings'           : ( self.get_common_stats,       DUMMY ),
             'CricketStandings'        : ( self.get_common_stats,       DUMMY ),
             'PGAStandings'            : ( self.get_common_stats,       DUMMY ),
             'PGATourSchedules'        : ( self.get_spider_stats,       DUMMY ),
             'PGATourOngoing'          : ( self.get_spider_stats,       DUMMY ),
             'PGATourScores'           : ( self.get_spider_stats,       DUMMY ),
             'WebcomOngoing'           : ( self.get_spider_stats,       DUMMY ),
             'WebcomTour'              : ( self.get_spider_stats,       DUMMY ),
             'LPGAGames'               : ( self.get_spider_stats,       DUMMY ),
             'LPGAOngoing'             : ( self.get_spider_stats,       DUMMY ),
             'UCIWorldTour'            : ( self.get_spider_stats,       DUMMY ),
             'LetourGames'             : ( self.get_spider_stats,       DUMMY ),
             'LetourStandings'         : ( self.get_spider_stats,       DUMMY ),
             'LetourGroupStandings'    : ( self.get_spider_stats,       DUMMY ),
             'FIFAInternationalScores' : ( self.get_spider_stats,       DUMMY ),
             'FIFAInternationalSchedules' : ( self.get_spider_stats,       DUMMY ),
             'MotogpSpider'            : ( self.get_spider_stats,       DUMMY ),
             'ChampionsUsopenSpider'   : ( self.get_spider_stats,       DUMMY ),
             'ChampionsTour'           : ( self.get_spider_stats,       DUMMY ),
             'ChampionsOngoing'        : ( self.get_spider_stats,       DUMMY ),
             'EuropeanTourSchedules'   : ( self.get_spider_stats,       DUMMY ),
             'EuropeanTourScores'      : ( self.get_spider_stats,       DUMMY ),
             'EuropeanTourOngoing'     : ( self.get_spider_stats,       DUMMY ),
             'F1Standings'             : ( self.get_common_stats,       DUMMY ),
             'FIFAWorldCup'            : ( self.get_spider_stats,       DUMMY ),
             'FieldHockeyScores'       : ( self.get_spider_stats,       DUMMY ),
             'FieldHockeySchedules'    : ( self.get_spider_stats,       DUMMY ),
             'TennisSpiderScores'      : ( self.get_spider_stats,       DUMMY ),
             'TennisSpiderSchedules'   : ( self.get_spider_stats,       DUMMY ),
             'NflRoster'               : ( self.get_spider_stats,       DUMMY ),
             'NbaRoster'               : ( self.get_spider_stats,       DUMMY ),
             'WnbaRoster'              : ( self.get_spider_stats,       DUMMY ),
             'FieldHockeyRoster'       : ( self.get_spider_stats,       DUMMY ),
             'UEFALeaguesStandings'    : ( self.get_spider_stats,       DUMMY ),
             'EuropeanOngoing'         : ( self.get_spider_stats,       DUMMY ),
             'MlbRoster'               : ( self.get_spider_stats,       DUMMY ),
             'NhlRosterNEW'            : ( self.get_spider_stats,       DUMMY ),
             'CWStandings'             : ( self.get_spider_stats,       DUMMY ),
             'SeniorTour'              : ( self.get_spider_stats,       DUMMY ),
             'UEFALeaguesRoster'       : ( self.get_spider_stats,       DUMMY ),
             'CommonWealthGames'       : ( self.get_spider_stats,       DUMMY ),
             'CommonWealthHockeyGames' : ( self.get_spider_stats,       DUMMY ),
             'CommonWealthTeamGames'   : ( self.get_spider_stats,       DUMMY ),
             'UefaAssociationLeaguesScores' : ( self.get_spider_stats,       DUMMY ),
             'UefaAssociationLeaguesSchedules' : ( self.get_spider_stats,       DUMMY ),
             'GolfSeniorTourScores'    : ( self.get_spider_stats,       DUMMY ),
             'GolfSeniorTourSchedules' : ( self.get_spider_stats,       DUMMY ),
             'CricketRoster' : ( self.get_spider_stats,       DUMMY ),
             'MLBStandings'            : ( self.get_spider_stats,       DUMMY ),
             'NRLRugby'                : ( self.get_spider_stats,       DUMMY ),
             'UsopenTennis'            : ( self.get_spider_stats,       DUMMY ),
             'FIBASpider'              : ( self.get_spider_stats,       DUMMY ),
             'UEFAMajorLeaguesRoster'  : ( self.get_spider_stats,       DUMMY ),
             'SoccerwaySoccerSchedules': ( self.get_spider_stats,       DUMMY ),
             'SoccerwaySoccerScores'   : ( self.get_spider_stats,       DUMMY ),
             'SoccerwaySoccerFacupSchedules': ( self.get_spider_stats,       DUMMY ),
             'SoccerwaySoccerFacupScores'   : ( self.get_spider_stats,       DUMMY ),
             'MLSStandings'            : ( self.get_spider_stats,       DUMMY ),
             'CollegeWorldSeries'      : ( self.get_spider_stats,       DUMMY ),
             'NCFRoster'               : ( self.get_spider_stats,       DUMMY ),
             'SuperLeague'               : ( self.get_spider_stats,       DUMMY ),
             'RyderCup'                : ( self.get_spider_stats,       DUMMY ),
             'NCAAFootballScores'      : ( self.get_spider_stats,       DUMMY ),
             'NCAAFootballSchedules'   : ( self.get_spider_stats,       DUMMY ),
             'NCAABowl'                : ( self.get_spider_stats,       DUMMY ),
             'NCBRoster'               : ( self.get_spider_stats,       DUMMY ),
             'NBAStandings'            : ( self.get_spider_stats,       DUMMY ),
             'MLBMVPSpider'            : ( self.get_spider_stats,       DUMMY ),
             'NCBStandings'            : ( self.get_spider_stats,       DUMMY ),
             'ISLStandings'            : ( self.get_spider_stats,       DUMMY ),
             'NCAANCFStandings'        : ( self.get_spider_stats,       DUMMY ),
             'RacingGames'             : ( self.get_spider_stats,       DUMMY ),
             'UEFAGroupStandings'      : ( self.get_spider_stats,       DUMMY ),
             'TennisFedCup'            : ( self.get_spider_stats,       DUMMY ),
             'IPTLTennis'              : ( self.get_spider_stats,       DUMMY ),
             'WorldTourTennisScores'   : ( self.get_spider_stats,       DUMMY ),
             'WorldTourTennisSchedules' : ( self.get_spider_stats,       DUMMY ),
             'DavisCupSpider'          : ( self.get_spider_stats,       DUMMY ),
             'CFLSpider'               : ( self.get_spider_stats,       DUMMY ),
             'AustraliaOpenGolf'       : ( self.get_spider_stats,       DUMMY ),
             'DaviscupRoster'          : ( self.get_spider_stats,       DUMMY ),
             'FedcupRoster'            : ( self.get_spider_stats,       DUMMY ),
             'CFLStandings'            : ( self.get_spider_stats,       DUMMY ),
             'F1Winner'                : ( self.get_spider_stats,       DUMMY ),
             'IPTLStandings'           : ( self.get_spider_stats,       DUMMY ),
             'FIFAClubWorldCup'        : ( self.get_spider_stats,       DUMMY ),
             'HockeyChampTrophy'       : ( self.get_spider_stats,       DUMMY ),
             'NCFTouStandings'         : ( self.get_spider_stats,       DUMMY ),
             'ChampTrophyStandings'    : ( self.get_spider_stats,       DUMMY ),
             'AFCAsianCup'             : ( self.get_spider_stats,       DUMMY ),
             'AFCStandings'            : ( self.get_spider_stats,       DUMMY ),
             'HopmancupRoster'         : ( self.get_spider_stats,       DUMMY ),
             'FIFAWomenStandings'      : ( self.get_spider_stats,       DUMMY ),
             'CONCACAFSoccer'          : ( self.get_spider_stats,       DUMMY ),
             'WNBAStandings'           : ( self.get_spider_stats,       DUMMY ),
             'UefaCupsSchedules'       : ( self.get_spider_stats,       DUMMY ),
             'HopmanCupSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'HopmanCupScores'         : ( self.get_spider_stats,       DUMMY ),
             'CONCACAFU20'             : ( self.get_spider_stats,       DUMMY ),
             'CONCACAFU20Standings'    : ( self.get_spider_stats,       DUMMY ),
             'MLSSoccerSchedules'      : ( self.get_spider_stats,       DUMMY ),
             'MLSSoccerScores'         : ( self.get_spider_stats,       DUMMY ),
             'BallonAwards'            : ( self.get_spider_stats,       DUMMY ),
             'ICCWorldCupStandings'    : ( self.get_spider_stats,       DUMMY ),
             'CFLRoster'               : ( self.get_spider_stats,       DUMMY ),
             'EuropeRugbyStandings'    : ( self.get_spider_stats,       DUMMY ),
             'RugbyEuropeSpiderSchedules' : ( self.get_spider_stats,       DUMMY ),
             'RugbyEuropeSpiderScores' : ( self.get_spider_stats,       DUMMY ),
             'CricketRosterCheck'      : ( self.get_spider_stats,       DUMMY ),
             'RFUChampionsSchedules'   : ( self.get_spider_stats,       DUMMY ),
             'RFUChampionsScores'      : ( self.get_spider_stats,       DUMMY ),
             'RFUChampionsStandings'   : ( self.get_spider_stats,       DUMMY ),
             'SoccerwayStandings'      : ( self.get_spider_stats,       DUMMY ),
             'CCLStandings'            : ( self.get_spider_stats,       DUMMY ),
             'UCIWomensStandings'      : ( self.get_spider_stats,       DUMMY ),
             'UCIWomensWorldcupScores' : ( self.get_spider_stats,       DUMMY ),
             'UCIWomensWorldcupSchedules' : ( self.get_spider_stats,       DUMMY ),
             'MarathonStandings'       : ( self.get_spider_stats,       DUMMY ),
             'NFLESPNSpiderScores'     : ( self.get_spider_stats,       DUMMY ),
             'NFLESPNSpiderSchedules'  : ( self.get_spider_stats,       DUMMY ),
             'UCIWomensTour'           : ( self.get_spider_stats,       DUMMY ),
             'IPLCricketRoster'        : ( self.get_spider_stats,       DUMMY ),
             'ICCIntCupStandings'      : ( self.get_spider_stats,       DUMMY ),
             'SuperRugbyStandings'     : ( self.get_spider_stats,       DUMMY ),
             'SuperRugbySpiderScores'  : ( self.get_spider_stats,       DUMMY ),
             'SuperRugbySpiderSchedules' : ( self.get_spider_stats,       DUMMY ),
             'ITMCupRugbySpider'       : ( self.get_spider_stats,       DUMMY ),
             'ITMCupRugbyStandings'    : ( self.get_spider_stats,       DUMMY ),
             'ChessGrandPrix'          : ( self.get_spider_stats,       DUMMY ),
             'WCLCricketStandings'     : ( self.get_spider_stats,       DUMMY ),
             'HeartlandRugby'          : ( self.get_spider_stats,       DUMMY ),
             'NCAAHockeySchedules'     : ( self.get_spider_stats,       DUMMY ),
             'NCAAHockeyScores'        : ( self.get_spider_stats,       DUMMY ),
             'CountyCricketStandings'  : ( self.get_spider_stats,       DUMMY ),
             'NatWestCricketStandings' : ( self.get_spider_stats,       DUMMY ),
             'SuperLeagueRugbyScores'  : ( self.get_spider_stats,       DUMMY ),
             'SuperLeagueRugbySchedules' : ( self.get_spider_stats,       DUMMY ),
             'SuperLeagueStandings'    : ( self.get_spider_stats,       DUMMY ),
             'RulesFootballSchedules'  : ( self.get_spider_stats,       DUMMY ),
             'RulesFootballScores'  : ( self.get_spider_stats,       DUMMY ),
             'SoccerwayInternationalSchedules'  : ( self.get_spider_stats,       DUMMY ),
             'SoccerwayInternationalScores'  : ( self.get_spider_stats,       DUMMY ),
             'EOCWaterPolo'           : ( self.get_spider_stats,       DUMMY ),
             'CPLStandings'           : ( self.get_spider_stats,       DUMMY ),
             'AtpWtaStandings'        : ( self.get_spider_stats,       DUMMY ),
             'AflStandings'           : ( self.get_spider_stats,       DUMMY ),
             'ESPNGroupStandings'     : ( self.get_spider_stats,       DUMMY ),
             'PGAEuropeanStandings'   : ( self.get_spider_stats,       DUMMY ),
             'WACMedals'              : ( self.get_spider_stats,       DUMMY ),
             'CSLSoccerSchedules'     : ( self.get_spider_stats,       DUMMY ),
             'CSLSoccerScores'        : ( self.get_spider_stats,       DUMMY ),
             'KhlStandings'           : ( self.get_spider_stats,       DUMMY ),
             'KHLSpiderSchedules'     : ( self.get_spider_stats,       DUMMY ),
             'KHLSpiderScores'        : ( self.get_spider_stats,       DUMMY ),
             'EuroNBASchedules'       : ( self.get_spider_stats,       DUMMY ),
             'WBABoxingSpiderSchedules' : ( self.get_spider_stats,       DUMMY ),
             'WBABoxingSpiderScores'  : ( self.get_spider_stats,       DUMMY ),
             'SuperRugbyRoster'       : ( self.get_spider_stats,       DUMMY ),
             'HeartlandStandings'     : ( self.get_spider_stats,       DUMMY ),
             'WCAMedals'              : ( self.get_spider_stats,       DUMMY ),
             'MexicanBaseballSchedules' : ( self.get_spider_stats,       DUMMY ),
             'MexicanBaseballScores'  : ( self.get_spider_stats,       DUMMY ),
             'MexBaseballStandings'   : ( self.get_spider_stats,       DUMMY ),
             'USOpenTennisSeed'       : ( self.get_spider_stats,       DUMMY ),
             'FIFAWorldCupQualifiers' : ( self.get_spider_stats,       DUMMY ),
             'ICCInterCupRosters'     : ( self.get_spider_stats,       DUMMY ),
             'WCLCricketRosters'      : ( self.get_spider_stats,       DUMMY ),
             'RugbyWorldCupRoster'    : ( self.get_spider_stats,       DUMMY ),
             'MexBaseballRoster'      : ( self.get_spider_stats,       DUMMY ),
             'RoyalOneDayCupStandings' : ( self.get_spider_stats,       DUMMY ),
             'CountyCricketRoster'    : ( self.get_spider_stats,       DUMMY ),
             'RugbyWorldCup'          : ( self.get_spider_stats,       DUMMY ),
             'RugbyWorldCupStandings' : ( self.get_spider_stats,       DUMMY ),
             'HKPLStandings'          : ( self.get_spider_stats,       DUMMY ),
             'RugbyChampStandings'    : ( self.get_spider_stats,       DUMMY ),
             'EuroNBAScores'          : ( self.get_spider_stats,       DUMMY ),
             'BasketfiSpiderSchedules': ( self.get_spider_stats,       DUMMY ),
             'BasketfiSpiderScores'   : ( self.get_spider_stats,       DUMMY ),
             'SoccerwayRoster'        : ( self.get_spider_stats,       DUMMY ),
             'ScandbasketballStandings' : ( self.get_spider_stats,       DUMMY ),
             'BasketballRealgmScores' : ( self.get_spider_stats,       DUMMY ),
             'BasketballRealgmSchedules' : ( self.get_spider_stats,       DUMMY ),
             'BasketballRealgmRoster' : ( self.get_spider_stats,       DUMMY ),
             'CBABasketballScores' :   ( self.get_spider_stats,       DUMMY ),
             'CBABasketballSchedules' : ( self.get_spider_stats,       DUMMY ),
             'FIHHockeyScores'        : ( self.get_spider_stats,       DUMMY ),
             'FIHHockeySchedues'      : ( self.get_spider_stats,       DUMMY ),
             'FIHStandings'           : ( self.get_spider_stats,       DUMMY ),
             'BigbashStandings'       : ( self.get_spider_stats,       DUMMY ),
             'CBAStandings'           : ( self.get_spider_stats,       DUMMY ),
             'NHLESPNSpiderSchedules' : ( self.get_spider_stats,       DUMMY ),
             'NHLESPNSpiderScores'    : ( self.get_spider_stats,       DUMMY ),
             'ESPNNHLStandings'       : ( self.get_spider_stats,       DUMMY ),

        }
        BIG_LIST = []

        date_yesterday = date.today()
        date_all = date_yesterday.strftime("%Y-%m-%d")
        date_graph = date_yesterday.strftime("%Y%m%d")

        date_diff = (date_yesterday - self.BASE_DATE).days

        table_list = []

        for dir_info in STATS_DIR_LIST:
            self.local_dir_name, self.ip, report_prefix, self.remote_dir_name, date_opt, title = dir_info
            print dir_info

            domain_list = []

            heading = '%s ON %s' % (title, self.ip)

            self.current_date = date_yesterday

            if date_opt == 'N':
                yesterday = self.run_graph_date
                time_format = '%Y%m%d'
            else:
                yesterday = self.run_date
                time_format = TIMEFORMAT

            stats_func, args = STATS_FUNC_DICT.get(self.local_dir_name, (None, ''))

            header_row, table_format = self.get_common_header(heading, stats_func)
            max_column_length = len(header_row)

            domain_list.append(header_row)
            table_list.append(table_format)

            local_pickle_prefix = os.path.join(self.local_dir_name, report_prefix)
            file_list = self.get_files_in_date_range(local_pickle_prefix, yesterday, num_of_days = date_diff, time_format = time_format)
            first = False

            last_status, last_peak_time, last_peak_memory = '' , '', ''
            total_count, pass_count = 0, 0

            #Processing each pickle file from respective directory
            for file_name in file_list:
                row_list = []
                obj = None
                actual_file_name = ''

                try:
                    actual_file_list = glob.glob("%s/%s*" % (PICKLE_DIR, file_name))
                    actual_file_list.sort(reverse=True)
                    if actual_file_list:
                        actual_file_name = actual_file_list[0]
                    else:
                        break            
                    obj = vtv_unpickle(actual_file_name, None)
                except (IndexError, AttributeError):
                    self.logger.error('Unpickle failed for %s' % actual_file_name)

                full_status = self.get_program_stats(row_list, obj, actual_file_name)

                total_count += 1
                if full_status:
                    pass_count += 1

                common_length = len(row_list)
                if obj and full_status and stats_func:
                    if args:
                        stats_func(row_list, obj, args)
                    else:
                        stats_func(row_list, obj)
                else:
                    row_list.extend(['']*(max_column_length - common_length))

                if not last_status:
                    last_date_str, last_status, failed_script, final_link, incr_link, last_peak_time, last_peak_memory = row_list[:7]

                self.current_date -= timedelta(days=1)

                self.calculate_diff(actual_file_name, domain_list, row_list, common_length)

                domain_list.append(row_list)
            if total_count == 0:
                percentage = 50
            else:
                percentage = (pass_count * 100) / total_count
            percent_str = '<font color="%s">%d</font>' % (self.get_percent_color(percentage), percentage)
            BIG_LIST.append((heading, last_status, percent_str, last_peak_time, last_peak_memory, domain_list))

        self.create_report_file(date_yesterday, BIG_LIST, table_list)

    def create_report_file(self, date_yesterday, BIG_LIST, table_list):
        graph_list = ['Date', 'New']

        JINJA_LIST = []
        count = 0
        old_ip = ''
        blue_list = []
        col_span = 6
        for dir_info in STATS_DIR_LIST:
            local_dir_name, ip, report_prefix, remote_dir_name, date_opt, title = dir_info
            count += 1
            if old_ip != ip:
                JINJA_LIST.append(JINJA_LOOP_FORMAT_STR % (count, col_span, ip, ip))
                blue_list.append(count - 1)
            old_ip = ip
        if old_ip and old_ip != ip:
            JINJA_LIST.append(JINJA_LOOP_FORMAT_STR % (count, col_span, ip, ip))
            blue_list.append(count - 1)
        blue_list.append(count)
        blue_list.pop(0)

        jinja_text = open(JINJA_FILE_NAME).read()
        jinja_text = jinja_text.replace('#BLUE_ITEMS#', ', '.join(map(str, blue_list)))
        jinja_text = jinja_text.replace('#LIST_ITEMS#', ''.join(JINJA_LIST))
        jinja_text = jinja_text.replace('#TABLE_ITEMS#', '\n'.join(table_list))

        open(HTML_FILE_NAME, 'w').write(jinja_text)

        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template(HTML_FILE_NAME).render(yesterday=date_yesterday, big_list=BIG_LIST, graph_data=graph_list)
        open(self.summary_file_name, 'w').write(table_html)

        today_file_name = os.path.join(self.REPORTS_DIR, '%s_report_%s.html' % (self.script_prefix, self.today_str))
        copy_file(self.summary_file_name, today_file_name, self.logger)

    def run_summary(self):
        self.init_summary()
        self.generate_summary()

        print 'Done - Summary Report Generation'

    def run_main(self):
        if self.options.run in [ 'all', 'report' ]:
            self.run_report()

        if self.options.run in [ 'all', 'scp' ]:
            self.run_scp()

        if self.options.run in [ 'all', 'html' ]:
            self.run_summary()

    def post_print_stats(self):
        self.print_report_link()

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', '*.log') ] )
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


if __name__ == '__main__':
    vtv_task_main(Report)
