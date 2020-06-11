from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider import VTVSpider, extract_data, get_nodes

def modify(data):
    try:
        data = ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
        return data
    except ValueError or UnicodeDecodeError or UnicodeEncodeError:
        try:
            return data.encode('utf8')
        except  ValueError or UnicodeEncodeError or UnicodeDecodeError:
            try:
                return data
            except ValueError or UnicodeEncodeError or UnicodeDecodeError:
                try:
                    return data.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    data = normalize('NFKD', data.decode('utf-8')).encode('ascii')
                    return data


class Chessplayerspider(VTVSpider):
    name = "chess_players"
    allowed_domains = ['ratings.fide.com']
    start_urls = ['http://ratings.fide.com/top.phtml?list=men', 'http://ratings.fide.com/top.phtml?list=women']
    record = SportsSetupItem()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//table//tr//td//a[contains(@href, "top_files")]')
        print "len(nodes)", len(nodes)
        for node in nodes:
            link = extract_data(node, './@href')
            title = extract_data(node, './text()').replace(',', '')
            print "limk>>>", link
            print "title >>>>", title
            pl_sk = link.split("=")[-1]
            pl_link = "http://ratings.fide.com/card.phtml?event=%s" % pl_sk
            print "pl_link******************%s" % pl_link
            yield Request(pl_link, callback= self.parse_players, meta = {'title':title, 'player_sk':pl_sk})

    def parse_players(self, response):
        hxs = HtmlXPathSelector(response)
        reference = response.url
        souece_key = response.meta['player_sk']
        title = response.meta['title']
        source = "fide_chess"

        gender = extract_data(hxs, '//table//tr/td[contains(text(), "Sex")]/following-sibling::td/text()').strip()
        main_role = modify(extract_data(hxs, '//table//tr/td[contains(text(), "FIDE title")]/following-sibling::td/text()')).strip()
        if main_role == " \xa0":
            main_role = "### "
        elif "\xc2\xa0" in main_role:
            main_role = main_role.replace('\xc2\xa0', '### ')
        print main_role
        image = extract_data(hxs, '//table//tr//td/img[contains(@src, "card.php")]/@src')
        image_link = "http://ratings.fide.com/"+ image
        country = extract_data(hxs, '//table//tr/td[contains(text(), "Federation")]/following-sibling::td/text()').strip()

        game = "chess"
        participant_type = "player"
        bp = "200"
        ref = reference
        loc = "0"
        aka = title.replace(',', '').split(' ')[1]+ " " + title.replace(',', '').split(' ')[0]
        debut = "0000-00-00"
        roles = ""
        age = ""
        height = ""
        weight = ""
        birth_date = "0000-00-00 00:00:00"
        salary_pop = ""
        rating_pop = ""
        birth_place = ""
        weight_class = ""
        marital_status = ""
        participant_since = ""
        competitor_since = ""
        
        data = {'title':title, 'souece_key':souece_key, 'source':source,
        'gender':gender, 'main_role':main_role, 'image_link':image_link,
        'game':game, 'ref_url':ref, 'aka':aka}
        print data
