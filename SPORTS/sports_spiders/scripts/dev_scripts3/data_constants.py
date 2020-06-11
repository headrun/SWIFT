#!/usr/bin/env python
# coding: utf-8

###############################################################################
# $Id: data_constants.py,v 1.1 2016/03/23 07:10:05 headrun Exp $
# Copyright(c) 2005 Veveo.tv
###############################################################################


# Content Type Definitions

CONTENT_TYPE_MOVIE                   = 'movie'
CONTENT_TYPE_MOVIE_NUM               = 1
CONTENT_TYPE_TVSERIES                = 'tvseries'
CONTENT_TYPE_TVSERIES_NUM            = 2
CONTENT_TYPE_TVVIDEO                 = 'tvvideo'
CONTENT_TYPE_TVVIDEO_NUM             = 3
CONTENT_TYPE_EPISODE                 = 'episode'
CONTENT_TYPE_EPISODE_NUM             = 4
CONTENT_TYPE_SEQUEL                  = 'sequel'
CONTENT_TYPE_SEQUEL_NUM              = 5
CONTENT_TYPE_PERSON                  = 'person'
CONTENT_TYPE_PERSON_NUM              = 6
CONTENT_TYPE_ROLE                    = 'role'
CONTENT_TYPE_ROLE_NUM                = 7
CONTENT_TYPE_CHANNEL                 = 'channel'
CONTENT_TYPE_CHANNEL_NUM             = 8
CONTENT_TYPE_CHANNEL_AFFILIATION     = 'channelaffiliation'
CONTENT_TYPE_CHANNEL_AFFILIATION_NUM = 9
CONTENT_TYPE_VOD                     = 'vod'
CONTENT_TYPE_VOD_NUM                 = 10
CONTENT_TYPE_THEATRE                 = 'theatre'
CONTENT_TYPE_THEATRE_NUM             = 11
CONTENT_TYPE_ALBUM                   = 'album'
CONTENT_TYPE_ALBUM_NUM               = 12
CONTENT_TYPE_SONG                    = 'song'
CONTENT_TYPE_SONG_NUM                = 13
CONTENT_TYPE_TRACK                   = 'track'
CONTENT_TYPE_TRACK_NUM               = 14
CONTENT_TYPE_TEAM                    = 'team'
CONTENT_TYPE_TEAM_NUM                = 15
CONTENT_TYPE_TOURNAMENT              = 'tournament'
CONTENT_TYPE_TOURNAMENT_NUM          = 16
CONTENT_TYPE_SPORTS_GROUP            = 'sportsgroup'
CONTENT_TYPE_SPORTS_GROUP_NUM        = 17
CONTENT_TYPE_STADIUM                 = 'stadium'
CONTENT_TYPE_STADIUM_NUM             = 18
CONTENT_TYPE_GAME                    = 'game'
CONTENT_TYPE_GAME_NUM                = 19
CONTENT_TYPE_PHRASE                  = 'phrase'
CONTENT_TYPE_PHRASE_NUM              = 20
CONTENT_TYPE_GENRE                   = 'genre'
CONTENT_TYPE_GENRE_NUM               = 21
CONTENT_TYPE_DECADE                  = 'decade'
CONTENT_TYPE_DECADE_NUM              = 22
CONTENT_TYPE_FILTER                  = 'filter'
CONTENT_TYPE_FILTER_NUM              = 23
CONTENT_TYPE_AWARD                   = 'award'
CONTENT_TYPE_AWARD_NUM               = 24
CONTENT_TYPE_LANGUAGE                = 'language'
CONTENT_TYPE_LANGUAGE_NUM            = 25
CONTENT_TYPE_RATING                  = 'rating'
CONTENT_TYPE_RATING_NUM              = 26
CONTENT_TYPE_SIMILAR                 = 'similar'
CONTENT_TYPE_SIMILAR_NUM             = 27
CONTENT_TYPE_ATTRIBUTE               = 'attribute'
CONTENT_TYPE_ATTRIBUTE_NUM           = 28
CONTENT_TYPE_LEXICAL                 = 'lexical'
CONTENT_TYPE_LEXICAL_NUM             = 29
CONTENT_TYPE_INTERSECTION            = 'intersection'
CONTENT_TYPE_INTERSECTION_NUM        = 30
CONTENT_TYPE_LIST                    = 'list'   # Used in Cablevision - kitchensink
CONTENT_TYPE_LIST_NUM                = 31
CONTENT_TYPE_IPC                     = 'ipc'
CONTENT_TYPE_IPC_NUM                 = 39
CONTENT_TYPE_USER                    = 'user'
CONTENT_TYPE_USER_NUM                = 40
CONTENT_TYPE_PLAYLIST                = 'playlist'
CONTENT_TYPE_PLAYLIST_NUM            = 41

# Content Xt Definitions

CONTENT_XT_PERSON                    = 'person'
CONTENT_XT_PERSON_NUM                = 1
CONTENT_XT_ACTOR                     = 'actor'
CONTENT_XT_ACTOR_NUM                 = 2
CONTENT_XT_DIRECTOR                  = 'director'
CONTENT_XT_DIRECTOR_NUM              = 3
CONTENT_XT_PRODUCER                  = 'producer'
CONTENT_XT_PRODUCER_NUM              = 4
CONTENT_XT_HOST                      = 'host'
CONTENT_XT_HOST_NUM                  = 5
CONTENT_XT_COMEDIAN                  = 'comedian'
CONTENT_XT_COMEDIAN_NUM              = 6
CONTENT_XT_PLAYER                    = 'player'
CONTENT_XT_PLAYER_NUM                = 7
CONTENT_XT_MODEL                     = 'model'
CONTENT_XT_MODEL_NUM                 = 8
CONTENT_XT_MUSIC_ARTIST              = 'musicartist'
CONTENT_XT_MUSIC_ARTIST_NUM          = 9
CONTENT_XT_MUSIC_BAND                = 'musicband'
CONTENT_XT_MUSIC_BAND_NUM            = 10
CONTENT_XT_DUO                       = 'duo'
CONTENT_XT_DUO_NUM                   = 11
CONTENT_XT_GENRE                     = 'genre'
CONTENT_XT_GENRE_NUM                 = 12
CONTENT_XT_SUBGENRE                  = 'subgenre'
CONTENT_XT_SUBGENRE_NUM              = 13
CONTENT_XT_MOOD                      = 'mood'
CONTENT_XT_MOOD_NUM                  = 14
CONTENT_XT_CLASSIC_GAME              = 'classicgame'
CONTENT_XT_CLASSIC_GAME_NUM          = 15
CONTENT_XT_PHRASE                    = 'phrase'
CONTENT_XT_PHRASE_NUM                = 16
CONTENT_XT_CONCEPT                   = 'concept'
CONTENT_XT_CONCEPT_NUM               = 17
CONTENT_XT_WIKI_CONCEPT              = 'wikiconcept'
CONTENT_XT_WIKI_CONCEPT_NUM          = 18
CONTENT_XT_PRIZE                     = 'prize'
CONTENT_XT_PRIZE_NUM                 = 19
CONTENT_XT_INSTRUMENT                = 'instrument'
CONTENT_XT_INSTRUMENT_NUM            = 20
CONTENT_XT_THEME                     = 'theme'
CONTENT_XT_THEME_NUM                 = 21
CONTENT_XT_PLAYING_STYLE             = 'playingstyle'
CONTENT_XT_PLAYING_STYLE_NUM         = 22
CONTENT_XT_VOCAL_TYPE                = 'vocaltype'
CONTENT_XT_VOCAL_TYPE_NUM            = 23
CONTENT_XT_TONE                      = 'tone'
CONTENT_XT_TONE_NUM                  = 24
CONTENT_XT_SONG_TYPE                 = 'songtype'
CONTENT_XT_SONG_TYPE_NUM             = 25
CONTENT_XT_SINGLE                    = 'single'
CONTENT_XT_SINGLE_NUM                = 26
CONTENT_XT_DUMMPY_TO_BE_USED         = 'to_be_used'
CONTENT_XT_DUMMPY_TO_BE_USED_NUM     = 27
CONTENT_XT_AVAIL_FILTER              = 'avail_filter'
CONTENT_XT_AVAIL_FILTER_NUM          = 28
CONTENT_XT_AWARD_CATEGORY            = 'award_category'
CONTENT_XT_AWARD_CATEGORY_NUM        = 29
CONTENT_XT_FILTER                    = 'filter'
CONTENT_XT_FILTER_NUM                = 30
CONTENT_XT_RATING_FILTER             = 'rating_filter'
CONTENT_XT_RATING_FILTER_NUM         = 31
CONTENT_XT_RESULT_FILTER             = 'result_filter'
CONTENT_XT_RESULT_FILTER_NUM         = 32
CONTENT_XT_SORT_FILTER               = 'sort_filter'
CONTENT_XT_SORT_FILTER_NUM           = 33
CONTENT_XT_SPORTS_FILTER             = 'sports_filter'
CONTENT_XT_SPORTS_FILTER_NUM         = 34
CONTENT_XT_STAR_FILTER               = 'star_filter'
CONTENT_XT_STAR_FILTER_NUM           = 35
CONTENT_XT_TAG_FILTER                = 'tag_filter'
CONTENT_XT_TAG_FILTER_NUM            = 36
CONTENT_XT_GUEST                     = 'guest'
CONTENT_XT_GUEST_NUM                 = 37
CONTENT_XT_ROVI_UNBOUND              = 'rovi_unbound'
CONTENT_XT_ROVI_UNBOUND_NUM          = 38
CONTENT_XT_SIMILAR_ARTIST            = 'similar_artist'
CONTENT_XT_SIMILAR_ARTIST_NUM        = 39
CONTENT_XT_SIMILAR_COMPOSER          = 'similar_composer'
CONTENT_XT_SIMILAR_COMPOSER_NUM      = 40
CONTENT_XT_SIMILAR_SOUND             = 'similar_sound'
CONTENT_XT_SIMILAR_SOUND_NUM         = 41
CONTENT_XT_SIMILAR_ALBUM             = 'similar_album'
CONTENT_XT_SIMILAR_ALBUM_NUM         = 42
CONTENT_XT_INFLUENCED                = 'influenced'
CONTENT_XT_INFLUENCED_NUM            = 43
CONTENT_XT_INFLUENCED_BY             = 'influenced_by'
CONTENT_XT_INFLUENCED_BY_NUM         = 44
CONTENT_XT_MOVIE_TYPE_THEATRE        = 'film'
CONTENT_XT_MOVIE_TYPE_THEATRE_NUM    = 45
CONTENT_XT_MOVIE_TYPE_TV             = 'tvfilm'
CONTENT_XT_MOVIE_TYPE_TV_NUM         = 46
CONTENT_XT_MOVIE_TYPE_VIDEO          = 'directtovideo'
CONTENT_XT_MOVIE_TYPE_VIDEO_NUM      = 47
CONTENT_XT_MOVIE_TYPE_WEB            = 'directtoweb'
CONTENT_XT_MOVIE_TYPE_WEB_NUM        = 48
CONTENT_XT_MOVIE_TYPE_CABLE          = 'madeforcable'
CONTENT_XT_MOVIE_TYPE_CABLE_NUM      = 49
CONTENT_XT_PRIZE_NORMALIZED          = 'prize_normalized'
CONTENT_XT_PRIZE_NORMALIZED_NUM      = 50

FOLD_TYPE_LIST = [ CONTENT_TYPE_LANGUAGE, CONTENT_TYPE_GENRE, CONTENT_TYPE_FILTER,
                   CONTENT_TYPE_DECADE, CONTENT_TYPE_STADIUM, CONTENT_TYPE_PHRASE,
                   CONTENT_TYPE_TOURNAMENT, CONTENT_TYPE_TEAM, CONTENT_TYPE_SPORTS_GROUP,
                   CONTENT_TYPE_AWARD, CONTENT_TYPE_PERSON
                 ]

ATTR_TO_XT_HASH = {
    'Ca' : CONTENT_XT_ACTOR,
    'Di' : CONTENT_XT_DIRECTOR,
    'Pr' : CONTENT_XT_PRODUCER,
    'Ho' : CONTENT_XT_HOST,
    'Co' : CONTENT_XT_MUSIC_ARTIST,
    'Zc' : CONTENT_XT_GUEST
    }

CREW_ATTR_LIST = ('Ca', 'Di', 'Pr', 'Ho', 'Co', 'Uc', 'Ic', 'Zc', 'Wr')

IN_FILE_SUFFIX     = 'in'
SORTED_FILE_SUFFIX = 'sorted'

ROVI_GENRE_PREFIX = 'RVG'

GID_PREFIX_PREFERENCE = [
    'WIKI',
    'WGER',
    'WFRA',
    'WSPA',
    'WSWE',
    'WNOR',
    'FRB',
    'CHAF',
    'G',
    'CG',
    'SG',
    'AR',
    'LFMSO',
    'LFMAR',
    'TEAM',
    'TOU',
    'PL',
    'RV',
    ]

LANG_MAPPING = {
    u'Basque Generic'                : u'BAQ',
    u'Catalán Generic'               : u'CAT',
    u'Chinese-Simplified-Mandarin'   : u'ZHO',
    u'Croatian_Generic'              : u'HRV',
    u'Danish Generic'                : u'DAN',
    u'Dutch Generic'                 : u'DUT',
    u'English - AU'                  : u'ENG',
    u'English - NA'                  : u'ENG',
    u'English - UK'                  : u'ENG',
    u'Finnish Generic'               : u'FIN',
    u'French Generic'                : u'FRA',
    u'French - Québec'               : u'FRA',
    u'Gallegan Generic'              : u'GLG',
    u'German Generic'                : u'GER',
    u'Hungarian Generic'             : u'HUN',
    u'Irish Generic'                 : u'GLE',
    u'Italian Generic'               : u'ITA',
    u'Japanese Generic'              : u'JPN',
    u'Luxembourgish Generic'         : u'LTZ',
    u'Malay Generic'                 : u'MSA',
    u'Norwegian Generic'             : u'NOR',
    u'Polish Generic'                : u'POL',
    u'Portuguese Generic'            : u'POR',
    u'Russian-C Generic'             : u'RUS',
    u'Scots Generic'                 : u'SCO',
    u'Scottish Gaelic Generic'       : u'GLA',
    u'Serbian_Generic'               : u'SRP',
    u'Spanish Generic'               : u'SPA',
    u'Swedish Generic'               : u'SWE',
    u'Tamil Generic'                 : u'TAM',
    u'Welsh Generic'                 : u'WEL',
    u'Turkish Generic'               : u'TUR',
    }

ROVI_LANG_FILES_CODE_LIST = [
    'FRA', 'JPN', 'DAN', 'SWE', 'GER',
    'ITA', 'NOR', 'POL', 'DUT', 'RUS',
    'SPA', 'KOR', 'FIN', 'POR', 'ZHO',
    'MSA', 'CAT', 'ENG', 'TUR'
]

COMMON_LANGUAGE_CODE_TO_REGIONAL_LANGUAGE_CODE = {
    'GER': 'DEU',
    'DUT': 'NLD',
}

ZL_MAP = {
    'DEU': 'GER',
    'NLD': 'DUT',
}

# %s -> 3 letter iso lang code
ROVI_OTHER_LANGUAGE_FILE_PATTERN = "rovi_%s.data"

PERSON_OTHER_LANGUAGE_FILE_PATTERN = "crew_%s.data"

# in the fd field for rovi 2.0 id identifier
FD_ROVI_ID_NAME = "rovi_id_2.0"
FD_ROVI_GROUP_ID_NAME = "group_id"
