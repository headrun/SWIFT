
class Diff:

    def __init__(self):
        self.merge_file = open('sports_to_wiki_guid_merge_file1', 'w')
        self.file_      = open('missed_gids_october', 'a+')
        self.gids_dict  = {}

    def main(self):
        merge_data = open('sports_to_wiki_guid_merge.list', 'r').readlines()
        for merge in merge_data:
            gid, sports_gid = merge.strip().split('<>')
            if 'TOU' in sports_gid:
                self.gids_dict[sports_gid] = gid
            elif 'TEAM' in sports_gid:
                self.gids_dict[sports_gid] = gid

        missed_gids = open('missed_teams_sports_merge', 'r').readlines()
        for missed in missed_gids:
            if missed.split('<>')[0] in self.gids_dict.keys():
                continue
            else:
                self.file_.write('%s\n' % missed.strip())

if __name__ == '__main__':
    OBJ = Diff()
    OBJ.main()
