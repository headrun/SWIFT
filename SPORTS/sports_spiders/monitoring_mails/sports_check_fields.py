import MySQLdb

#Charcters to check
count       = 0
qu          = '&quot;'
a           = '&amp;' 
n           = '\n'  
z           = '\0'  
e           = '  '  
t           = '\t' 
 
MAIN_DICT   = {}
HOST        = '10.28.218.81'
DBNAME      = 'SPORTSDB'

cursor  = MySQLdb.connect(host=HOST, user="veveo", db=DBNAME, passwd="veveo123").cursor()
query   = 'show tables like "sports_%"'
cursor.execute(query)
tables  = cursor.fetchall()
MAIN_DICT = {}

def checking_fields(title, check_list):
    message = ''
    if 'qu' in check_list:
        if (qu in title):
            message = message +" "+"(has quotes &quot;)"

    if "a" in check_list:
        if (a in title):
             message =  message +" "+"(has ampresand)"

    if "n" in check_list:
        if (n in title):
            message = message +" "+"(has_newline)"

    if "z" in check_list:
        if (z in title):
            message = message +" "+"(null charcter)"

    if "e" in check_list:
        if (z in title):
            message = message +" "+"(double charcters)"

    if "t" in check_list:
        if (z in title):
            message = message +" "+"(tab charcters)"


    return message

def check_fields():
    count = 0
    for table in tables: 
        #Getting the field for the tables
        MAIN_DICT[table[0]] = {}
        query = 'desc %s' %table[0]
        cursor.execute(query)
        fields = cursor.fetchall()

        for field in fields: 
            field_name  = field[0]
            field_type  = field[1]

            if 'varchar' in field_type or "text" in field_type:
                MAIN_DICT[table[0]][field_name] = []
                query   = 'select %s from %s;' %(field_name, table[0])
                cursor.execute(query)
                titles  = cursor.fetchall()

                for title in titles:
                    if title[0]:
                        if "varchar" in field_type:
                            """Checking everything except ;amp in the urls and Links"""
                            if "link" in field_name or "url" in field_name:
                                message = checking_fields(title[0], ['qu', 'n', 'z', 't'])
                                if message:
                                    count   = count + 1
                                    message = "%s --> <b>%s</b>" %(title[0], message)
                                    MAIN_DICT[table[0]][field_name].append(message)
                                    continue

                            else:
                                message = checking_fields(title[0], ['qu', 'n', 'z', 't', 'a'])
                                if message:
                                    count = count + 1
                                    message = "%s --> <b>%s</b>" %(title[0], message)
                                    MAIN_DICT[table[0]][field_name].append(message)
                                    continue

                        else:
                            """Checking everythin except double spaces for description& Video titles,
                                game_note fields"""
                            if "description" in field_name or "video" in table[0] \
                                    or 'game_note' in field_name:
                                message = checking_fields(title[0], ['qu', 'n', 'z', 't', 'a'])
                                if message:
                                    count = count + 1
                                    message = "%s --> <b>%s</b>" %(title[0], message)
                                    MAIN_DICT[table[0]][field_name].append(message)
                                    continue
                            else:
                                message = checking_fields(title[0], ['qu', 'n', 'z', 't', 'a'])
                                if message:
                                    count = count + 1
                                    message = "%s --> <b>%s</b>" %(title[0], message)
                                    MAIN_DICT[table[0]][field_name].append(message)
                                    continue

    text = ''
    if count:
        for key, values in MAIN_DICT.iteritems():
            for value in values:
                if MAIN_DICT[key].has_key(value):
                    lis = MAIN_DICT[key][value]
                    if lis:
                        text += '<tr><td><b>%s</b></td><td><b>%s</b></td></tr>' %('Table Name', key)
                        for li in lis:
                            text += '<tr><td>%s</td><td>%s</td></tr> '%(value, li) 

        text = '<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px">'+text+'</table>'
    return text
