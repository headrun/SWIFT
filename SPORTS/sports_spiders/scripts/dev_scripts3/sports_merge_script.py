from vtv_task import VtvTask, vtv_task_main
from optparse import OptionParser

class SportsMerge(VtvTask):
    
    def __init__(self):
        VtvTask.__init__(self)
        self.mysql_ip = '10.4.2.187'
        self.db_name = 'GUIDMERGE'
        self.sports_ip = '10.4.18.34'
        self.sports_merge_db = 'GUIDMERGE'
        self.all_wiki_gids = {}
    	self.unmerged_gids = {}

    def get_all_wiki_gids(self):
    	self.open_cursor(self.mysql_ip, self.db_name)
        query = 'select exposed_gid, child_gid from sports_wiki_merge'
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for record in data:
            wiki_gid, sports_gid = record
            self.all_wiki_gids[sports_gid] = wiki_gid
   
    def get_unmerged_gids(self, gids_file, gid):
        records = open(gids_file, 'r').readlines()
        import pdb; pdb.set_trace()
        for record in records:
            if ',' in record:
                sports_gid, wiki_gid = record.strip().split(',')
            else:
                sports_gid, wiki_gid = record.strip().split('<>')
            if 'WIKI' not in wiki_gid:
                wiki_gid = 'WIKI' + wiki_gid
            if not gid in sports_gid:
                print 'merge type is not matched'
                return
            if not self.all_wiki_gids.get(sports_gid, ''):
                self.unmerged_gids[wiki_gid] = sports_gid
        if self.unmerged_gids:
            print len(self.unmerged_gids)

    def populate_merge(self):
        if self.options.setup == 'dev':
            self.open_cursor(self.sports_ip, self.sports_merge_db)
        elif self.options.setup == 'prod':
            self.open_cursor(self.mysql_ip, self.db_name)
        for key, value in self.unmerged_gids.iteritems():
            query = 'insert into sports_wiki_merge (exposed_gid, child_gid, action, modified_date) values(%s, %s, %s, now())'
            values = (key, value, 'override')
            self.cursor.execute(query, values)

    def set_options(self):
        self.parser.add_option('-t', '--merge_type', default = '', help = 'team or player or tournament')
        self.parser.add_option('-f', '--file_name', default = '', help = 'please pass the file')
        self.parser.add_option('-s', '--setup', default = 'dev', help = 'setup dev or prod')
	    
    def run_main(self):
        if self.options.merge_type == 'team':
            gid = 'TEAM'
        elif self.options.merge_type == 'tournament':
            gid = 'TOU'
        elif self.options.merge_type == 'player':
            gid = 'PL'
        if not self.options.file_name:
            print 'pass the file to populate merge'
            return
        if self.options.file_name:
            self.get_all_wiki_gids()
            self.get_unmerged_gids(self.options.file_name, gid)
            self.populate_merge()


if __name__ == '__main__':
    vtv_task_main(SportsMerge)
