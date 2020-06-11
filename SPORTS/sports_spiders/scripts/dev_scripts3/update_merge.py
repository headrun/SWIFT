import sys
import MySQLdb
from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd
from vtv_task import VtvTask, vtv_task_main

insert_stadiums_query = 'insert into sports_wiki_merge(exposed_gid, child_gid, action, modified_date) values (%s, %s, "override",now())'

class StadiumUpdation(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)


    def update_stadiums_to_guid_merge(self):
        cursor  = MySQLdb.connect(db="GUIDMERGE", host="10.4.2.187").cursor()
        query   = 'select * from sports_wiki_merge'
        cursor.execute(query)
        records = cursor.fetchall()

        already_mapped_gids = [ i[1] for i in records if "STAD" in i[1]]
        lines = file("org1.txt", "r+").readlines()
        for line in lines:
            wiki_gid, stad_gid = line.strip().split("<>")
            if stad_gid in already_mapped_gids:
                continue
            #cursor.execute(insert_stadiums_query, (wiki_gid, stad_gid))
            print line.strip()

    def find_duplicates(self):
        print "find Duplicates ..."
        duplicates = {}

        lines = file("sports_to_wiki_guid_merge.list", "r+").readlines()
        for line in lines:
            wiki_gid, stad_gid = line.strip().split("<>")
            duplicates.setdefault(wiki_gid, [])
            duplicates[wiki_gid].append(stad_gid)
        
        for key, values in duplicates.iteritems():
            if len(values) != 1:
                print "%s<>%s" %(key, ",".join(values))

    def run_main(self):
        self.find_duplicates()
        #self.update_stadiums_to_guid_merge()


if __name__ == "__main__":
    vtv_task_main(StadiumUpdation)
    sys.exit( 0 )
