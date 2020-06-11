from juicer.utils import *

class TeleramaMovieBrowse(JuicerSpider):
    name = 'telerama_movie_browse'
    start_urls = ['http://www.telerama.fr/cine/film_datesortie.php']

    domain_url = 'http://www.telerama.fr'
    url_list = []

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="t12-crilst-item"]')

        for node in nodes:
            movie_link = extract_data(node, './/p[@class="t12-crilst-tit"]/a/@href')
            sk = movie_link.split('/')[-1].strip('.php')
            if not 'http' in movie_link:
                movie_link = self.domain_url + movie_link
            if not '/tele/' in movie_link:
                self.get_page('telerama_movie_terminal', movie_link, sk)

        pages = extract_list_data(hxs, '//div[@class="t12-aresoc"]//a/@href')
        for page in pages:
            if not 'http' in page:
                page = self.domain_url + '/cine/' + page
            if page not in self.url_list:
                self.url_list.append(page)
                yield Request(page, self.parse)

