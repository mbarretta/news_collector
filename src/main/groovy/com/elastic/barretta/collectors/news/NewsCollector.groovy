package com.elastic.barretta.collectors.news

import com.elastic.barretta.collectors.news.scrapers.APScraper
import com.elastic.barretta.collectors.news.scrapers.NewsAPIScraper

/**
 * Main class
 */
class NewsCollector {

    static class Config {
        final static DEFAULT_ES_URL = "http://localhost:9200"
        final static DEFAULT_ES_INDEX = "news"
        final static DEFAULT_ES_USER = "elastic"
        final static DEFAULT_ES_PASS = "changeme"
        final static DEFAULT_ES_TYPE = "doc"

        final static ES_MAPPING = [
            (DEFAULT_ES_TYPE): [
                properties: [
                    date_published: [type: "date", format: "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss.SSSZ"],
                    title         : [type: "text"],
                    shortId       : [type: "keyword"],
                    url           : [type: "text", fields: [keyword: [type: "keyword", ignore_above: 256]]],
                    byline        : [type: "text"],
                    source        : [type: "keyword"],
                    section       : [type: "keyword"],
                    text          : [type: "text"],
                    entityObjects : [type: "nested"],
                    entityNames   : [type: "text"]
                ]
            ]
        ]

        ESClient.Config es = new ESClient.Config()
        NewsAPIScraper.Config newsApi = new NewsAPIScraper.Config()
        boolean clean = false
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "APNewCollector")
        cli.esUrl(args: 1, argName: "URL", "URL for ES [default: $Config.DEFAULT_ES_URL]")
        cli.esIndex(args: 1, argName: "index", "ES index name [default: $Config.DEFAULT_ES_INDEX]")
        cli.esUser(args: 1, argName: "user", "username for ES authentication [default: $Config.DEFAULT_ES_USER]")
        cli.esPass(args: 1, argName: "pass", "password for ES authentication [default: $Config.DEFAULT_ES_PASS]")
        cli.newsApiKey(args:1, argName: "apiKey", "key for newsapi.org")
        cli.clean("drop and build the ES index")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        run(doConfig(options))
    }

    static def run(Config config) {
        def client = new ESClient(config.es)

        prepESIndex(client, config.clean)
        def results = [:]
        if (config.newsApi.key) {
            results += NewsAPIScraper.scrape(config.newsApi, client)
        }
        results += APScraper.scrape(client)

        return results
    }

    private static def prepESIndex(ESClient client, boolean clean) {
        if (clean) {
            client.deleteIndex()
        }
        client.createIndex(Config.ES_MAPPING)
    }

    /**
     * merge property file config, cli config, and default values: CLI has first priority, then property file, then default
     * @param cliConfig options from CliBuilder
     * @param propertyConfig ConfigObject from ConfigSlurper()
     * @return ConfigObject
     */
    static def doConfig(cliConfig) {
        def appConfig = new Config()
        def esConfig = new ESClient.Config()
        def propertyConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))

        esConfig.with {
            url = cliConfig.esUrl ?: propertyConfig.es.url ?: Config.DEFAULT_ES_URL
            index = cliConfig.esIndex ?: propertyConfig.es.index ?: Config.DEFAULT_ES_INDEX
            type = Config.DEFAULT_ES_TYPE
            user = cliConfig.esUser ?: propertyConfig.es.user ?: Config.DEFAULT_ES_USER
            pass = cliConfig.esPass ?: propertyConfig.es.pass ?: Config.DEFAULT_ES_PASS
        }

        appConfig.newsApi.key = cliConfig.newsApiKey ?: propertyConfig.newsApi.key
        appConfig.clean = cliConfig.clean ?: propertyConfig.clean ?: false
        appConfig.es = esConfig
        return appConfig
    }
}