import urllib.parse
from math import ceil

import scrapy
from srealitycz.common import SREALITYCZ_CATEGORIES, SREALITYCZ_TYPES


class SrealityBaseSpider(scrapy.Spider):
    allowed_domains = ["sreality.cz"]
    items_per_page = 60
    base_url = "https://www.sreality.cz/api/cs/v2/estates?"

    def start_requests(self):
        _, category, type1 = self.name.split("_")
        url = self.base_url + urllib.parse.urlencode(
            {
                "category_main_cb": SREALITYCZ_CATEGORIES[category],
                "category_type_cb": SREALITYCZ_TYPES[type1],
                "per_page": self.items_per_page,
            }
        )
        yield scrapy.Request(url)

    def parse(self, response):
        result = response.json()
        n_pages = ceil(result["result_size"] / 60)
        for i in range(n_pages):
            url = f"{response.url}&page={i+1}"
            yield scrapy.Request(url, callback=self.parse_listing)

    def parse_listing(self, response):
        for item in response.json()["_embedded"]["estates"]:
            detail_url = response.urljoin("/api/" + item["_links"]["self"]["href"])
            yield scrapy.Request(
                detail_url, callback=self.parse_details, meta={"item": item}
            )

    def parse_details(self, response):
        listing = response.meta["item"]
        yield {
            "_id": listing["hash_id"],
            "detailed_info": response.json(),
            "listing_info": listing
        }
