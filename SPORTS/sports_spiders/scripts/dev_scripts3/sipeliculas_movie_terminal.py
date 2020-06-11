# -*- coding: utf-8 -*-
import re
from juicer.utils import *
from juicer.items import *

class SipeliculasMovieTerminal(JuicerSpider):
    name = 'sipeliculas_movie_terminal'

    def parse(self, response):
        hxs = Selector(response)
        data = {}
        nodes = get_nodes(hxs, '//div[@class="info"]//ul/li')
        for node in nodes:
            header = extract_data(node, './span[1]//text()').strip(':')
            value = extract_data(node, './span[2]//text()')
            data[header] = value
        description = extract_data(hxs, '//div[@class="sinopsis"]/p//text()')
        views = extract_data(hxs, '//li[@class="vi"]/text()')
        genre = data.get(u'G\xe9nero', '')
        title = data.get(u'Titulo original', '')
        duration = data.get(u'Duraci\xf3n', '')
        if duration:
            duration = re.findall('\d+', duration)[0]
            duration = int(duration) * 60

        movie_item = MovieItem()
        movie_item['sk'] = normalize(response.meta['sk'])
        movie_item['title'] = normalize(title)
        movie_item['description'] = normalize(description)
        movie_item['genres'] = normalize(genre)
        movie_item['duration'] = str(duration)
        movie_item['metadata_language'] = 'Spanish'
        movie_item['reference_url'] = normalize(response.url)
        movie_item['aux_info'] = ''
        yield movie_item

        if views:
            no_of_views = re.findall('\d+', views)[0]
            popularity_item = PopularityItem()
            popularity_item['program_sk'] = normalize(response.meta['sk'])
            popularity_item['program_type'] = 'movie'
            popularity_item['no_of_views'] = str(no_of_views)
            yield popularity_item

        if data.get(u'A\xf1o', ''):
            year = data.get(u'A\xf1o', '')
            release_item = ReleasesItem()
            release_item['program_sk'] = normalize(response.meta['sk'])
            release_item['program_type'] = 'movie'
            release_item['release_year'] = str(year)
            yield release_item

        for key, value in data.iteritems():
            for key in [u'G\xe9nero', u'M\xe1s informaci\xf3n', u'Titulo original', u'Productora', u'A\xf1o', u'Duraci\xf3n']:
                continue
            if ',' in value:
                names = [val.strip() for val in value.split(',')]
            else:
                names = [value]
            rank = 0
            for name in names:
                rank += 1
                crew_sk = md5(name)
                crew_item = CrewItem()
                crew_item['sk'] = normalize(crew_sk)
                crew_item['name'] = normalize(name)
                crew_item['reference_url'] = normalize(response.url)
                yield crew_item

                program_crew = ProgramCrewItem()
                program_crew['program_sk'] = normalize(response.meta['sk'])
                program_crew['program_type'] = 'movie'
                program_crew['role']   = normalize(key)
                program_crew['crew_sk'] = normalize(crew_sk)
                program_crew['rank'] = str(rank)
                yield program_crew

        image = extract_data(hxs, '//li[@class="vi"]//img/@src')
        if image:
            richmedia_item = RichMediaItem()
            richmedia_item['sk'] = md5(image)
            richmedia_item['program_sk'] = normalize(response.meta['sk'])
            richmedia_item['program_type'] = 'movie'
            richmedia_item['media_type'] = 'image'
            richmedia_item['image_type'] = 'small'
            richmedia_item['image_url'] = normalize(image)
            richmedia_item['reference_url'] = normalize(response.url)
            yield richmedia_item

        avail_nodes = get_nodes(hxs, '//ul[@class="opciones"]/li')
        for node in avail_nodes:
            if extract_data(node, './span[@class="opcion"]/text()'):
                continue
            avail_data = extract_list_data(node, './/span/text()')
            quality = avail_data[-1]
            audio_lang = avail_data[-2]
            avail_item = AvailItem()
            avail_item['program_sk'] = normalize(response.meta['sk'])
            avail_item['program_type'] = 'movie'
            avail_item['quality'] = normalize(quality)
            avail_item['reference_url'] = normalize(response.url)
            avail_item['audio_languages'] = normalize(audio_lang)
            yield avail_item
