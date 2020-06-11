# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class SportsSetupItem(Item):
    # define the fields for your item here like:
    # name = Field()

    rich_data        = Field()
    event            = Field()
    reference_url    = Field()
    source           = Field()
    source_key       = Field()
    participants     = Field()
    game             = Field()
    game_datetime    = Field()
    result           = Field()
    affiliation      = Field()
    game_status      = Field()
    tournament       = Field()
    location_info    = Field()
    participant_type = Field()
    season           = Field()
    result_type      = Field()
    id_type          = Field()
    time_unknown     = Field()
    tz_info          = Field()
    result_sub_type  = Field()
    series_name      = Field()
    sport_name       = Field()
    sport_id         = Field()
    reference_id     = Field()
    season_type      = Field()
    week_id          = Field()
    week_number      = Field()
