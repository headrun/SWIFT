#!/usr/bin/python

import md5
import cgitb; cgitb.enable()
import cgi
import MySQLdb

def main(field_storage):

    val = field_storage.getfirst('action', '')

    #Participant table details
    image = field_storage.getfirst('img', '')
    loc = field_storage.getfirst('loc', '0')
    gid = field_storage.getfirst('gid', '0')

    #Teams table details
    src = field_storage.getfirst('src', '')
    sk = field_storage.getfirst('sk', '')

    print "Content-Type: text/html"
    print

    print '<html>'
    print '<head></head>'
    print '<center><h2><b>IMAGE ADDITION</b></h2></center><br/>'
    print '<body>'
    print '<form><table>'
    print '<tr><td>Im:</td><td><input type="text", size=50, name="img", value="%s"></input><br/></td></tr>' %image
    print '<tr><td>Vt:</td><td><input type="text", name="loc", size=50, value="%s"></input><br/></td></tr>' %loc
    print '<tr><td>Gi:</td><td><input type="text", name="gid", size=50, value="%s"></input><br/></td></tr>' %gid
    print '<tr><td><br/></td></tr>'
    print '<tr><td><b>Source Releated Information</b></td></tr>'
    print '<tr><td>SourceName:</td><td><input type="text", name="src", size=50, value="%s"></input><br/></td></tr>' %src
    print '<tr><td>SK:</td><td><input type="text", name="sk", size=50, value="%s"></input><br/></td></tr>' %sk

    print '<tr><td><br/></td></tr>'
    print '<tr><td></br><input type="submit", name="action" value="submit"><br/></td></tr>'
    print '</table>'

    #Get tournament details
    if val == "submit":
	    con = MySQLdb.connect(host='10.4.2.207', user='root', db='IMAGEDB', charset='utf8', use_unicode=True)
	    cursor = con.cursor()
	    image_hash = md5.md5(image).hexdigest()
	    query = 'insert into image_meta(image_url, aspect_ratio, imagehash, is_valid, created_at, modified_at) values ( %s, %s, %s, 1, now(), now()) on duplicate key update modified_at =now()'
	    values = (image, '', image_hash)
	    cursor.execute(query, values)

	    query = 'select id from image_meta where imagehash="%s"' %image_hash
	    cursor.execute(query)
	    image_meta_id = cursor.fetchall()[0][0]

	    query = "insert into source_image_map(source, sk, type, image_meta_id, created_at, modified_at) values (%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"
	    values = (src, sk, loc, image_meta_id)
	    cursor.execute(query, values)

	    query = "insert into gid_source_map(source, sk, type, gid, iso, created_at, modified_at) values (%s, %s, %s, %s, 'eng', now(), now()) on duplicate key update modified_at=now()"
	    values = (src, sk, loc, gid)
	    cursor.execute(query, values)

	    print '</form></body></html>'
	    cursor.close()

if __name__ == '__main__':
    field_storage = cgi.FieldStorage()
    main(field_storage)
