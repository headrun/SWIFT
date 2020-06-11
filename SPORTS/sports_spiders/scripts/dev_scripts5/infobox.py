import json
import traceback
class SportsInfoData:
    tou_details = open("info_details1.txt", "w")
    def __init__(self):
        self.json_data = open("DATA_INFOBOX_FILE.json").readlines()
    def main(self):
        for tou_data in self.json_data:
            json_data  = json.loads(tou_data)
            gid = json_data.keys()[0]
            try:
                if json_data.has_key('folded'):
                    if json_data.has_key('title'):
                        title = josn_data['title']
                    elif json_data.has_key('name'):
                        title = josn_data['name']
                    folded = json_data['folded']
                    tou_data = [gid, title, folded]
                    tou_data = "#<>#".join(tou_data)
                    self.tou_details.write('%s\n'%(tou_data))


            except:
                f = open("logfile.info", "w")
                print traceback.format_exc()
                f.write('%s\n'%(traceback.format_exc()))

                continue


if __name__ == "__main__":
    SportsInfoData().main()
