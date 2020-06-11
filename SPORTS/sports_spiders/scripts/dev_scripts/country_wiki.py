import MySQLdb

CURSOR = MySQLdb.connect(host="10.4.15.132", user="root",
db="SPORTSDB_BKP").cursor()

#CURSOR = MySQLdb.connect(host="10.4.18.183", user="root",
#db="SPORTSDB").cursor()

INSERT_QUERY = 'insert into sports_team_countries \
(gid, title, keywords, created_at, modified_at) values \
("%s", "%s", " ", now(), now()) on duplicate key update modified_at = now()'

CHECK_WIKIGID = "select gid from sports_team_countries where gid='%s'"

data_wiki = open('country_gids', 'r')

class CountryWiki:
    data_wiki = open('country_gids', 'r')

    def main(self):
        for data in self.data_wiki:
            country_data = data.replace('\n', '').split('WIKI')
            if "Country" in data:
                continue
            if len(country_data) > 2 or len(country_data) < 2:
                print country_data
                continue
            else:
                country = country_data[0].strip()
                gid = country_data[1].strip()

                if country and gid:
                    wiki_gid = "WIKI" + gid
                    CURSOR.execute(CHECK_WIKIGID % (wiki_gid))
                    db_data = CURSOR.fetchone()
                    if not db_data:
                        values = (wiki_gid, country)
                        query = INSERT_QUERY % values
                        CURSOR.execute(query)
                    else:
                        print db_data

if __name__ == '__main__':
    OBJ = CountryWiki()
    OBJ.main()
