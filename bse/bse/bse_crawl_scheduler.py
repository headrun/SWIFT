import json
from itertools import cycle
from scrapyd_api import ScrapydAPI
from MySQLdb import connect

class CrawlScheduler():

    def main(self):
        # with open('search_tokens.txt', 'r') as token_file:
        #     search_tokens_list = token_file.readlines()

        # round_robin = cycle(search_tokens_list)
        scrapyd = ScrapydAPI('http://127.0.0.1:6800')
        conn = connect(db='bse', host='localhost', user='mca',
                            passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        cursor = conn.cursor()
        query = 'select security_code from bse_crawl where crawl_status = 0'
        cursor.execute(query)
        bse_sec_code = cursor.fetchall()
        bse_sec_code = [
            (bse_sec_code[0]) for sec_code in bse_sec_code if sec_code]
        chuncked_sec_codes = [item for item in self.get_chunks(bse_sec_code, 400) if item]
        for chuncked_sec_code in chuncked_sec_codes:
            scrapyd.schedule('bse', 'bse_final', jsons=json.dumps(chuncked_sec_code))
            scrapyd.schedule('bse', 'bse_xpath', jsons=json.dumps(chuncked_sec_code))
        cursor.close()
        conn.close()

    def get_chunks(self, search_items, count):
        for item in range(0, len(search_items), count):
            yield search_items[item:item + count]

if __name__ == '__main__':
    CrawlScheduler().main()
