import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
import MySQLdb


class Impendi_alert():
    def __init__(self):
        self.conn = MySQLdb.connect(db='conversion', host='localhost', user='root',
            passwd='', charset="utf8", use_unicode=True)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def send_mail(self, html_content, subject):
        sender = 'noreply@headrun.com'
        recipients = [, 'ranjan@headrun.com', 'sankar@headrun.com', 'anandhu@headrun.com']
        password = 'hdrn591!'
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ','.join(recipients) #Recipients should be a list

        part = MIMEText(html_content, 'html')
        msg.attach(part)
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        print("Mail has been sent")
        server.quit()

    def main(self):
        try:
            date = datetime.datetime.now() - datetime.timedelta(1)
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
