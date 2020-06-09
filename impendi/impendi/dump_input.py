from datetime import datetime
from pandas import ExcelFile
from common_utils import get_urlq_cursor

#from sqlalchemy import create_engine

class Input2SQL(object):
    def main(self):
        conn, cursor = get_urlq_cursor()
        data = ExcelFile('Impendi_Analytics_Masterfile_Dataset.xlsx')
        df = data.parse('InputData ImpendiAnalytics', skiprows=3)
        df = df[['SKU ID', 'Search Keyword']]
        values = [tuple(item) for item in df._values]
        query = 'insert into ebay_crawl (sk, search_key, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at = now()'
        cursor.executemany(query, values)
        conn.commit()

        cursor.close()
        conn.close()

if __name__ == '__main__':
    Input2SQL().main()
