import re
import datetime
import time
import pytz
import urllib
import urllib2
from urlparse import urlparse

def X(data):
    try:
        if isinstance(data, int):
            return data
        return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
    except:
        try: return ''.join([chr(ord(x)) for x in data]).decode('cp1252').encode("utf-8")
        except: return data.encode('utf8')

def format_data(data):
    return ' '.join(i.strip() for i in data.split() if i)

def get_sk(player_url, player_name):
    try:
        redir_url = urllib2.urlopen(player_url).url
        sk = redir_url.split('/')[-1].split('.asp')[0].strip()
    except urllib2.HTTPError:
        print "Player url Failed with 404, Name: %s --- Url: %s " % (player_name, player_url)
        sk = '-'.join(i.lower().strip() for i in player_name.split() if i)
        old_id = player_url.split('/')[-1].split('.asp')[0].strip()
        print "PLayer Old id: %s --- New Id: %s" % (old_id, sk)
    except:
        print "Player url Failed, Name: %s --- Url: %s " % (player_name, player_url)
        sk = '-'.join(i.lower().strip() for i in player_name.split() if i)
        old_id = player_url.split('/')[-1].split('.asp')[0].strip()
        print "PLayer Old id: %s --- New Id: %s" % (old_id, sk)

    return sk

def get_str_end_dates(tou_date):
    #Feb 13-17 - Feb 27-Mar 3
    str_date, e_date = [i.strip() for i in tou_date.split('-')]
    if len(e_date.split()) < 2:
        str_month, str_day = str_date.split()
        e_date = '%s %s' % (str_month, e_date)

    return str_date, e_date

def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    pool = tuple(iterable)
    n = len(pool)
    #r = n if r is None else r
    if r is None:
        r = n

    if r > n:
        return
    indices = range(n)
    cycles = range(n, n-r, -1)
    yield tuple(pool[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:r])
                break
        else:
            return

def get_position(position):
    pos = position
    posi = "".join(re.findall(r'T', position))
    if posi and position.endswith('T') and position not in ["CUT"]:
        pos = position.replace('T', '')
        pos = "T" + pos
    else:
        pos = position

    return pos

def get_tou_dates(start_date, end_date, start_date_format, end_date_format):
    #format '%b %d, %Y'
    end_date = (datetime.datetime(*time.strptime(end_date.strip(), end_date_format)[0:6])).date()
    game_year = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    if game_year.month > end_date.month:
        tou_year = end_date.year - 1
    else:
        tou_year = end_date.year

    if len(start_date.split()) == 2:
        start_date += ', %s' % tou_year
        start_date_format += ', %Y'

    start_date = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    tou_date = start_date.strftime('%b %-d') +' - '+ end_date.strftime('%b %-d')

    return tou_date, start_date, end_date

def get_full_url(exp_url, base_url):
    url_data = urlparse(exp_url)
    if isinstance(url_data, tuple):
        scheme, domain, path = url_data[:3]
    else:
        scheme, domain, path = url_data.scheme, url_data.netloc, url_data.path

    if not path:
        print "Path is mandatory, Given Url: %s" % exp_url
        return exp_url
    else:
        path = path.strip()

    if domain and scheme:
        return exp_url

    if not path.startswith('/'):
        path += '/'

    base_url_data = urlparse(base_url)
    if isinstance(base_url_data, tuple):
        base_scheme, base_domain = base_url_data[:2]
    else:
        base_scheme, base_domain = base_url_data.scheme, base_url_data.netloc

    if not base_scheme and not base_domain:
        print "Provide valid Base Url, Given Url: %s" % base_url
        return exp_url

    return base_scheme + '://' + base_domain + path

def get_utc_time(date_val, pattern):
    utc          = pytz.utc
    eastern      = pytz.timezone('US/Eastern')
    fmt          = '%Y-%m-%d %H:%M:%S'
    date         = datetime.datetime.strptime(date_val, pattern)
    date_eastern = eastern.localize(date, is_dst=None)
    date_utc     = date_eastern.astimezone(utc)
    utc_date     = date_utc.strftime(fmt)

    return utc_date

def get_game_datetime(tou_date, year):
    game_dates = [i.strip() for i in tou_date.split('-') if i]
    if len(game_dates) == 2:
        start_date, end_date = game_dates
        game_datetime = start_date + ' %s' % year
    elif len(game_dates) >= 1:
        game_datetime = game_dates[0] + ' %s' % year
    else:
        game_datetime = ''

    if game_datetime:
        game_datetime = get_utc_time(game_datetime, '%b %d %Y')

    return game_datetime

def get_golfers_info(to_par, total_score, pl_pos, rounds_scores, tee_time):
    players = {}
    players = {'TO PAR' : to_par, 'final' : total_score, 'position' : pl_pos}
    for i in xrange(len(rounds_scores)):
        rt = "R%s" %str(i + 1)
        rv = rounds_scores[i]
        players.update({rt : rv})

    tee_times = []
    for t in tee_time:
        if t == '':
            break
        else:
            tee_times.append(t)

    for i in xrange(len(tee_times)):
        rt = "R%s_tee_time" % str(i+1)
        rv = tee_times[i]
        players.update({rt : rv})


    return players


