import json
from itertools import cycle
from scrapyd_api import ScrapydAPI
from common_utils import get_urlq_cursor

class CrawlScheduler():

    def main(self):
        with open('search_tokens.txt', 'r') as token_file:
            search_tokens_list = token_file.readlines()

        round_robin = cycle(search_tokens_list)
        scrapyd = ScrapydAPI('http://127.0.0.1:6800')
        conn, cursor = get_urlq_cursor()
        query = 'select sk, search_key, end_time from ebay_crawl where crawl_status = 0'
        cursor.execute(query)
        search_items = cursor.fetchall()
        search_items = [
            (search_item[0], search_item[1], search_item[2], round_robin.__next__().strip()) for search_item in search_items if search_item]
        chuncked_search_items = [item for item in self.get_chunks(search_items, 500) if item]
        for chuncked_search_item in chuncked_search_items:
            scrapyd.schedule('impendi', 'ebay_browse', jsons=json.dumps(chuncked_search_item))

        cursor.close()
        conn.close()

    def get_chunks(self, search_items, count):
        for item in range(0, len(search_items), count):
            yield search_items[item:item + count]

if __name__ == '__main__':
    CrawlScheduler().main()
