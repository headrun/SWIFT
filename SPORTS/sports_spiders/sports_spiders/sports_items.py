# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class SportsSetupItem(Item):

    rich_data        = Field() # The field is a dictonary containing the meta information like channels, Location, etc
    event            = Field() # The event field contains the Name of the Particular event. for example ICC World Cup Semi Finals
    reference_url    = Field() # Reference Urls is he filed that contains he reference url of the game.
    source           = Field() # Source filed is populated with the source string. Example, "espn" if we are crawling from the espn site. 
    source_key       = Field() # Source key is the unique id for a game or participant.
    participants     = Field() # Participants filed is the dictonary which has players / teams as keys and and its qualities(is home or not) as values.
    game             = Field() # The sport for which we are populating the games. Ex, baseball etc.
    game_datetime    = Field() # Game Date time of the game in UTC format.
    result           = Field() # result is the dictionary containing the results of the game. Key shall be the participant source key and its value is another dictionary containing                                  the resuls. For Example, {'australia': {'final': '1', 'half_time': '0'}}
    affiliation      = Field() # Affiliation depends on the game. Ex, 'mlb' for Major League Baseball, 'uefa' for Europa league etc. 
    game_status      = Field() # Scheduled or Ongoing or Completed. 
    tournament       = Field() # Name of the tournament. Ex, ICC World Cup.
    location_info    = Field() # Game location Infomation.
    participant_type = Field() # Team or Player.
    season           = Field() # Year of the season. Ex, 2017, 2015 etc.
    result_type      = Field() 
    id_type          = Field()
    time_unknown     = Field()
    tz_info          = Field()
    result_sub_type  = Field()
    series_name      = Field()
    sport_name       = Field()
