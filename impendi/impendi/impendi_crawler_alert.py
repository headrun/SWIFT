import argparse
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
import MySQLdb

parser = argparse.ArgumentParser()

class Impendi_alert():
    def __init__(self):
        self.conn = MySQLdb.connect(db='conversion', host='localhost', user='root',
            passwd='', charset="utf8", use_unicode=True)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def send_mail(self, html_content, subject):
        sender = os.environ.get('sender', '')
        recipients = os.environ.get('recipients', '')
        password = os.environ.get('password', '')
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipients

        part = MIMEText(html_content, 'html')
        msg.attach(part)
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients.split(','), msg.as_string())
        print("Mail has been sent")
        server.quit()

    def main(self):
        try:
            parser.add_argument(
            '--date', '-d', help='Date for running alert', default=1, type=int)
            args = parser.parse_args()
            date = datetime.datetime.now() - datetime.timedelta(args.date)
            date_format = str(date.date())
            query = "select distinct date(created_at) from ebay_crawl\
                     where date(created_at) = '{0}'".format(date_format)
            self.cur.execute(query)
            data = self.cur.fetchall()
            html = '<html><head></head><body>'
            if len(data) < 1:
                html += 'Dear Team, <br /> <br />'
                html += 'Impendi Crawler failed to run on <b>{0}</b>. Please check'.format(date_format)
                html += '</table></body>'
                html += '<br /> <br /></html>'
                subject = 'Alert: Impendi Crawler Failed for {0}'.format(date_format)
                self.send_mail(html, subject)
            else:
                print('Data is found in Impendi Crawler for {0}'.format(date_format))
        except:
            import traceback; traceback.format_exc()
            error = traceback.format_exc()
            print(error)

if __name__ == '__main__':
    try:
        OBJ = Impendi_alert()
        OBJ.main()
    except:
        import traceback; traceback.format_exc()
        error = traceback.format_exc()
        print(error)
