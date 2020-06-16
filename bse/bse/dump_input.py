import pandas as pd
from MySQLdb import connect

class Input2SQL():
    def main(self):
        conn = connect(db='bse', host='localhost', user='mca',
                            passwd='H3@drunMcaMy07', charset="utf8", use_unicode=True)
        cursor = conn.cursor()
        data = pd.read_csv('/home/mca/SWIFT/bse/bse/TickerCodes.csv')
        df = data[['Security Code', 'Security Name']]
        df.rename(columns={'Security Code': 'security_code', 'Security Name': 'security_name'}, inplace=True)
        values = [tuple(item) for item in df._values]
        query = 'insert into bse_crawl (security_code, security_name, crawl_status, created_at, modified_at ) values(%s, %s, 0, now(), now() )on duplicate key update modified_at = now(), crawl_status = 0'
        cursor.executemany(query, values)
        conn.commit()
        cursor.close()
        conn.close()


if __name__ == '__main__':
    Input2SQL().main()