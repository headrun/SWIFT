from juicer.utils import *


class SipeliculasBrowse(JuicerSpider):
    name = 'sipeliculas_browse'
    start_urls = ['http://www.sipeliculas.com/']
    domain_url = 'http://www.sipeliculas.com/'

    def parse(self, response):
        hxs = Selector(response)
        genre_nodes = get_nodes(hxs, '//div[@id="m-izquierda"]//ul[@class="ul"]/li')
        for node in genre_nodes:
            link = extract_data(node, './a/@href')
            genre = extract_data(node, './a/text()')
            if link:
                yield Request(link, self.parse_movies, meta={'genre': genre})

    def parse_movies(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//ul[@class="lista-peliculas"]/li')
        for node in nodes:
            movie_link = extract_data(node, './a/@href')
            if movie_link:
                sk = movie_link.split('/')[-1].strip()
                self.get_page('sipeliculas_movie_terminal', movie_link, sk)

        page = extract_data(hxs, '//ul[@class="paginacion"]//a[contains(@title, "Siguiente")]/@href')
        if page:
            if not 'http' in page:
                page = self.domain_url + page
                yield Request(page, self.parse_movies)
