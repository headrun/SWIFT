# -*- coding: utf-8 -*-
import re
from juicer.utils import *
from juicer.items import *


class TeleramaCrewTerminal(JuicerSpider):
    name = 'telerama_crew_terminal'
    domain_url = 'http://www.telerama.fr'

    def parse(self, response):
        hxs = Selector(response)
        crew_sk = response.meta['sk']
        name = extract_data(hxs, '//h1[contains(@class, "-head-tit")]/a/text()')
        images_link = extract_data(hxs, '//li[a[contains(text(), "Photos")]]/a/@href')
        details_link = extract_data(hxs, '//li[a[contains(text(), "Informations")]]/a/@href')
        nodes = get_nodes(hxs, '//div[@class="tv10-fiche-filmo"]')
        recent_films = ''
        for node in nodes:
            movie_link = extract_data(node, './a/@href')
            movie_sk = movie_link.split('/')[-1].strip('.php')
            if not 'http' in movie_link:
                movie_sk = self.domain_url + movie_link
            self.get_page('telerama_movie_terminal', movie_link, movie_sk)
            if recent_films:
                recent_films += '<>%s' % movie_sk
            else:
                recent_films = movie_sk

        crew_item = CrewItem()
        crew_item['sk'] = normalize(crew_sk)
        crew_item['name'] = normalize(name)
        crew_item['reference_url'] = normalize(response.url)
        crew_item['recent_films'] = normalize(recent_films)
        yield crew_item

        if images_link:
            images_link = self.domain_url + images_link
            yield Request(images_link, self.parse_rich_media, meta={'crew_sk': crew_sk})


    def parse_rich_media(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[contains(@class, "fiche-photos-diapos")]/a')
        images = {}
        for node in nodes:
            small_image = extract_data(node, './img/@src')
            large_image = extract_data(node, './@onclick')
            if large_image:
                large_image = large_image.split('(')[-1].strip(')').replace("'", '')
            images['small'] = small_image
            images['large'] = large_image

        for key, image in images.iteritems():
            image_sk = md5(image)
            richmedia_item = RichMediaItem()
            richmedia_item['sk'] = str(image_sk)
            richmedia_item['program_sk'] = normalize(response.meta['crew_sk'])
            richmedia_item['program_type'] = 'crew'
            richmedia_item['media_type'] = 'image'
            richmedia_item['image_type'] = str(key)
            richmedia_item['image_url'] = normalize(image)
            richmedia_item['reference_url'] = normalize(response.url)
            yield richmedia_item

