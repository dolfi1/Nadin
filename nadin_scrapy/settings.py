BOT_NAME = "nadin_scrapy"

SPIDER_MODULES = ["nadin_scrapy.spiders"]
NEWSPIDER_MODULE = "nadin_scrapy.spiders"

ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400
RETRY_ENABLED = True
RETRY_TIMES = 2

DOWNLOADER_MIDDLEWARES = {
    "nadin_scrapy.middlewares.RotateUserAgentMiddleware": 400,
    "nadin_scrapy.middlewares.BlockDetectionMiddleware": 550,
}

ITEM_PIPELINES = {
    "nadin_scrapy.pipelines.CompanyProfilePipeline": 300,
}
