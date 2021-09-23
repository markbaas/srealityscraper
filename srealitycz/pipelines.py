# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import datetime
import os.path

from itemadapter import ItemAdapter
from scrapy import signals

from srealitycz.exporters import SplittedJsonItemExporter


class SrealityczPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        output_path = os.path.join(
            "output",
            datetime.datetime.today().strftime("%Y%m%d"),
            *spider.name.split("_"),
        )
        self.exporter = SplittedJsonItemExporter(output_path, indent=True)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
