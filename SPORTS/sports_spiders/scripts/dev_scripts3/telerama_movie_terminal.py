# -*- coding: utf-8 -*-
import re
from juicer.utils import *
from juicer.items import *

MONTHS = {'janvier': 'January', u'février': 'February',
          'mars': 'March', 'avril': 'April',
          'mai': 'May', 'juin': 'June',
          'juillet': 'July', u'août': 'August',
          'septembre': 'September', 'octobre': 'October',
          'novembre': 'November', u'décembre': 'December'}

class TeleramaMovieTerminal(JuicerSpider):
    name = 'telerama_movie_terminal'
    domain_url = 'http://www.telerama.fr'

    def parse(self, response):
        hxs = Selector(response)
        movie_sk = response.meta['sk']
        title = extract_data(hxs, '//h1/a/text()')
        release_date = extract_data(hxs, '//span[contains(text(), "Date de sortie")]/a/text()')
        desc = extract_data(hxs, '//p[@itemprop="description"]/text()')
        genre = extract_data(hxs, '//span[@itemprop="genre"]//text()')
        images_link = extract_data(hxs, '//li[a[contains(text(), "Photos")]]/a/@href')
        details_link = extract_data(hxs, '//li[a[contains(text(), "Informations")]]/a/@href')
        if release_date:
            for month in MONTHS.keys():
                if month not in MONTHS.keys():
                    print month
                if month in release_date:
                    release_date = release_date.replace(month, MONTHS[month])
            year = str(datetime.datetime.strptime(release_date, '%d %B %Y').year)
            release_date = str(datetime.datetime.strptime(release_date, '%d %B %Y'))
        else:
            release_date = ''
            year = ''

        movie_item = MovieItem()
        movie_item['sk'] = normalize(movie_sk)
        movie_item['title'] = normalize(title)
        movie_item['description'] = normalize(desc)
        movie_item['genres'] = normalize(genre)
        movie_item['metadata_language'] = 'French'

        release_item = ReleasesItem()
        release_item['program_sk'] = normalize(movie_sk)
        release_item['program_type'] = 'movie'
        release_item['release_date'] = release_date
        release_item['release_year'] = year
        yield release_item

        if details_link:
            details_link = self.domain_url + details_link
            yield Request(details_link, self.parse_movie_details, meta={'movie_sk': movie_sk, 'movie_item': movie_item})

        if images_link:
            images_link = self.domain_url + images_link
            yield Request(images_link, self.parse_rich_media, meta={'movie_sk': movie_sk})

    def parse_movie_details(self, response):
        hxs = Selector(response)
        movie_item = response.meta['movie_item']
        movie_sk = response.meta['movie_sk']
        duration = extract_data(hxs, '//span[contains(text(), "Dur")]/following-sibling::text()')
        movie_item['reference_url'] = normalize(response.url)
        if duration:
            duration = re.findall('\d+', duration)
            if len(duration) == 1 and duration[0] != '0':
                dur = int(duration[0]) * 60
                movie_item['duration'] = str(dur)
                yield movie_item
            elif len(duration) > 1:
                print response.url
        else:
            yield movie_item

        lists  = [li.strip() for li in extract_list_data(hxs, '//p[contains(@class, "fiche-infos-txt")]//text()') if li.replace('\t', '').strip()]
        data = {}
        for lst in lists:
            if lst.endswith(':'):
                key = lst.replace(':', '').strip()
                continue
            else:
                if ',' in lst:
                    value = [val.strip() for val in lst.split(',') if val.strip()]
                else:
                    value = [lst]
            if value:
                data.setdefault(key, []).append(value)

        for role, crews in data.iteritems():
            if not role in [u'R\xe9alisateur', u'Acteurs / r\xf4les']:
                continue
            crews_role = [val for val in crews if ':' in val]
            crews = [val for val in crews if ':' not in val]

            rank = 0
            for ind, names in enumerate(crews):
                role_title = ''
                if u'Acteurs / r\xf4les' in role and crews_role:
                    if ind + 1 <= len(crews_role):
                       role_title = crews_role[ind]
                if isinstance(names, str):
                    print 'jhansi'
                for name in names:
                    rank += 1
                    crew_sk = md5(name)
                    crew_item = CrewItem()
                    crew_item['sk'] = normalize(crew_sk)
                    crew_item['name'] = normalize(name)
                    crew_item['reference_url'] = ''
                    yield crew_item

                    program_crew = ProgramCrewItem()
                    program_crew['program_sk'] = normalize(movie_sk)
                    program_crew['program_type'] = 'movie'
                    program_crew['crew_sk'] = normalize(crew_sk)
                    program_crew['role'] = normalize(role)
                    program_crew['role_title'] = normalize(role_title)
                    program_crew['rank'] = str(rank)
                    yield program_crew

        crew_links = get_nodes(hxs, '//p[contains(@class, "fiche-infos-txt")]/a')
        for crew_link in crew_links:
            crew_url = extract_data(crew_link, './@href')
            crew_sk = md5(extract_data(crew_link, './text()'))
            if not 'http' in crew_url:
                crew_url =  self.domain_url + crew_url
            self.get_page('telerama_crew_terminal', crew_url, crew_sk)



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
            richmedia_item['program_sk'] = normalize(response.meta['movie_sk'])
            richmedia_item['program_type'] = 'movie'
            richmedia_item['media_type'] = 'image'
            richmedia_item['image_type'] = str(key)
            richmedia_item['image_url'] = normalize(image)
            richmedia_item['reference_url'] = normalize(response.url)
            yield richmedia_item

