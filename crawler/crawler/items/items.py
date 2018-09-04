# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html


from scrapy import Item, Field


class IndexCsiQuoteItem(Item):
    index_id = Field()
    date = Field()
    close = Field()


class IndexCsiInfoItem(Item):
    index_id = Field()
    index_name = Field()
    index_name_en = Field()
    base_date = Field()
    base_value = Field()
