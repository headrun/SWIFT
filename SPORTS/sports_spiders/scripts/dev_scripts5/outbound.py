from scrapy.spider import BaseSpider
from scrapy.http import Request,FormRequest

class LoginSpider(BaseSpider):
    name = 'loginspidername'
    start_urls = ['https://onthego.astro.com.my/?_ga=1.146373364.1331792641.1411463504']
    #login_page = 'https://onthego.astro.com.my/?_ga=1.146373364.1331792641.1411463504'

    def parse(self, response):
        return [FormRequest.from_response(response,
                    formdata={'username': 'bibeejan', 'Password': '#123786!'},
                    callback=self.after_login)]

    def after_login(self, response):
        if "logout" in response.body:
            print 'logged in'
        else:
            print 'not logged in'
        return
