import re
import datetime
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import Selector
import MySQLdb

conn = MySQLdb.connect(host="10.28.218.81", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
cur = conn.cursor()

sports_dict = {}

class SportsFilmsSpider(BaseSpider):
    name = 'sports_films'
    start_urls  = ['https://en.wikipedia.org/wiki/List_of_sports_films']

    def parse(self, response):
        sel = Selector(response)
        sport_nodes = sel.xpath('//h2[span[@class="mw-headline"]]')
        for node in sport_nodes:
            sport_name =  ''.join(node.xpath('./span[@class="mw-headline"]/text()').extract()) or \
                          ''.join(node.xpath('./span[@class="mw-headline"]/a/text()').extract())
            movie_nodes = node.xpath('./following-sibling::table[@class="wikitable"][1]/tr')
            for movie in movie_nodes:
                movie_titl = ''.join(movie.xpath('./td[1]//text()').extract())
                movie_link = ''.join(movie.xpath('./td[1]//a/@href').extract())
                movie_year = ''.join(movie.xpath('./td[2]//text()').extract())
                movie_genre = ''.join(movie.xpath('./td[3]//text()').extract())
                movie_note = ''.join(movie.xpath('./td[4]//text()').extract())
                if not movie_titl: continue
                if movie_link and 'http' not in movie_link:
                    movie_link = 'https://en.wikipedia.org' + movie_link
                if not movie_link:
                    qry = 'insert into sports_movies (sport_name, movie_title, release_year, genre, movie_notes, reference_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
                    vals = (sport_name, movie_titl, movie_year, movie_genre, movie_note, movie_link)
                    cur.execute(qry, vals)
                else:
                    yield Request(movie_link, callback= self.parse_wiki, meta={'sp_nm': sport_name, 'mv_titl': movie_titl, 'mv_year': movie_year, 'mv_gnr': movie_genre, 'mv_note': movie_note})

    def parse_wiki(self, response):
        sel = Selector(response)
        temp = ''.join(sel.xpath('//script//text()').extract())
        wiki_id = ''.join(re.findall('ArticleId\":(.*?),\"wgIsArt', temp))
        sport_name = response.meta['sp_nm']
        movie_titl = response.meta['mv_titl']
        movie_year = response.meta['mv_year']
        movie_genre = response.meta['mv_gnr'] 
        movie_note = response.meta['mv_note']

        movie_titl = movie_titl+ '{WIKI%s}'%wiki_id
        qry = 'insert into sports_movies (sport_name, movie_title, release_year, genre, movie_notes, reference_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
        vals = (sport_name, movie_titl, movie_year, movie_genre, movie_note, response.url)
        cur.execute(qry, vals)

            
