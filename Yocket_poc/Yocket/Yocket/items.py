# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class CSVItem(Item):
    ProfileName = Field()
    UndergradDegree = Field()
    UndergradUniversity = Field()
    UndergradCgpa = Field()
    Experience = Field()
    WorkExperience = Field()
    CompanyName = Field()
    Jobtitle = Field()
    Techpapers = Field()
    Numberofresearch = Field()
    Skills = Field()
    InterestedTermandYear = Field()
    InterestedCourse = Field()
    GRETotalScore = Field()
    GREVerbalScore = Field()
    GREQuantScore = Field()
    TOEFLSCORE = Field()
    IELTSSCORE = Field()
    Applieduniversity = Field()
    Appliedcourse = Field()
    Applieddate = Field()
    Status = Field()
    Link = Field()
    SourceUniversity = Field()

