# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd


class MariaStoragePipeline(object):
    buffer = []
    cur = 0
    max_ = 10000

    def process_item(self, item, spider):
        if self.cur <= self.max_:
            self.buffer.append(item)
            self.cur += 1
            if self.cur % 500 == 0:
                print(self)
                print(self.cur)

        else:
            print(pd.DataFrame(self.buffer))
            self.cur = 0
            self.buffer = []
