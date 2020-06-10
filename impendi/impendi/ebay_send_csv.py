import MySQLdb
import pandas as pd
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from common_utils import get_urlq_cursor

headers_dict = {'sk': 'SKU ID', 'search_key': 'Search Keyword', 'item_id': 'Item Id', 'top_rated': 'Top RatedListing', 'title': 'Title', 'location': 'Location', 'postal_code': 'Postalcode', 'returns_accepted': 'Return Accepted', 'is_multi': 'is MultiVariation Listing', 'category_id': 'Category ID', 'category': 'Category', 'expedited_shipping': 'Expedited Shipping', 'ship_to_locations': 'Ship To Locations', 'shipping_type': 'Shipping Type', 'shipping_service_cost': 'Shipping Service Cost_value', 'shipping_service_currency': 'ShippingServiceCost_currency', 'current_price': 'CurrentPrice_value', 'current_price_currency': 'CurrentPrice_currency', 'converted_current_price': 'ConvertedCurrentPrice_value', 'converted_current_price_currency': 'ConvertedCurrentPrice_currency', 'selling_state': ' State', 'condition': 'Condition', 'listing_type': 'ListingType', 'best_offer_enabled': 'BestOfferEnabled', 'buy_it_now_available': 'BuyItNowAvailable', 'start_time': 'Start Time', 'end_time': 'End Time', 'image_url': 'Image', 'reference_url': 'ItemListingURL', 'timestamp': 'Timestamp'}

class CsvPre():
    def __init__(self):
        self.file_name = 'ebay_sold_items_%s.csv' % datetime.now().strftime('%Y_%m_%d')

    def main(self):
        q_conn, q_cursor = get_urlq_cursor()
        q_query = 'select count(*) from ebay_crawl where crawl_status in (0)'
        q_cursor.execute(q_query)
        crawl_count = q_cursor.fetchone()[0]
        if not crawl_count:
            db = MySQLdb.connect(host="localhost", user="root", passwd="P@tN3R#74#$", db="EBAYDB", charset="utf8")
            query = "select * from ebay_sold_items"
            df = pd.read_sql(query, db)
            del df['created_at']
            del df['modified_at']
            df.rename(columns=headers_dict, inplace=True)
            df.to_csv(self.file_name, index=False)
            #attachment = open(self.file_name, "rb")
            #self.send_email(attachment)

            #attachment.close()
            db.close()

        else:
            print("Crawling is still in progress!")

        q_cursor.close()
        q_conn.close()

    def send_email(self, attachment):
        fromaddr = "noreply@headrun.com"
        toaddr = "charan@headrun.com"
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = "Ebay Sold Items"
        body = "PFA"
        msg.attach(MIMEText(body, 'plain'))
        filename = "ebay_sold_items.csv"
        p = MIMEBase('application', 'octet-stream')
        p.set_payload((attachment).read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition',
                     "attachment; filename= %s" % filename)
        msg.attach(p)
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(fromaddr, "hdrn591!")
        text = msg.as_string()
        s.sendmail(fromaddr, toaddr, text)
        s.quit()


if __name__ == '__main__':
    OBJ = CsvPre()
    OBJ.main()
