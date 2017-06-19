package com.elastic.barretta.collectors.news

import com.elastic.barretta.collectors.news.scrapers.APScraper
import com.elastic.barretta.collectors.news.scrapers.NewsAPIScraper

/**
 * Main class
 */
class NewsCollector {

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

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "APNewCollector")
        cli.esUrl(args: 1, argName: "URL", "URL for ES [default: $DEFAULT_ES_URL]")
        cli.esIndex(args: 1, argName: "index", "ES index name [default: $DEFAULT_ES_INDEX]")
        cli.esUser(args: 1, argName: "user", "username for ES authentication [default: $DEFAULT_ES_USER]")
        cli.esPass(args: 1, argName: "pass", "password for ES authentication [default: $DEFAULT_ES_PASS]")
        cli.newsApiKey(args:1, argName: "apiKey", "key for newsapi.org")
        cli.clean("drop and build the ES index")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        run(mergeConfig(options))
    }

    static def run(ConfigObject config) {
        def client = new ESClient(config.es)

        prepESIndex(client, config.clean as boolean)
        def results = [:]
        if (config.newsApi.apiKey) {
            results += NewsAPIScraper.scrape(config, client)
        }
        results += APScraper.scrape(config, client)

        return results
    }

    private static def prepESIndex(client, clean) {
        if (clean) {
            client.deleteIndex()
        } else {
            client.createIndex(ES_MAPPING)
        }
    }

    private static def mergeConfig(cliConfig) {
        def appConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))

        //merge file config, cli config, and default values
        def esConfig = new ESClient.Config()
        esConfig.with {
            url = cliConfig.esUrl ?: appConfig.es.url ?: DEFAULT_ES_URL
            index = cliConfig.esIndex ?: appConfig.es.index ?: DEFAULT_ES_INDEX
            type = DEFAULT_ES_TYPE
            user = cliConfig.esUser ?: appConfig.es.user ?: DEFAULT_ES_USER
            pass = cliConfig.esPass ?: appConfig.es.pass ?: DEFAULT_ES_PASS
        }
        appConfig.newsApi.apiKey = cliConfig.newsApiKey ?: appConfig.newsApi.apiKey
        appConfig.clean = cliConfig.clean ?: appConfig.clean ?: false
        appConfig.es = esConfig
        return appConfig
    }
}