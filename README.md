# SRealityCzScraper


## Introduction

Scrapes the real estate listings for sreality.cz for 
- houses for sale
- houses for rent
- appartments for sale
- appartments for rent

## How to run?

For each category and type there is one scraper. You can run the with the following commands:

```
scrapy crawl sreality_houses_sale --loglevel INFO
scrapy crawl sreality_houses_rent --loglevel INFO
scrapy crawl sreality_appartments_sale --loglevel INFO
scrapy crawl sreality_appartments_rent --loglevel INF
```

## Technical considerations

The scraper basically plugs in into the sreality.cz estates api. So there is one base class `SrealityBaseSpider` that contains the actual code. The four implementation spiders override the base class and the base spider knows what to do using the scraper name.

In order to avoid having huge files, a custom json exporter is included so that files are output splitted with in each file 1000 items.

File are not really readable using a text editor, so it is recommended to use `jq` for parsing the json files.

## What's next?

Ideally the data should be outputted to a mongo or a redis store and then processed by a Spark job for cleaned data.



