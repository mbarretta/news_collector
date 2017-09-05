package com.elastic.barretta.collectors.news

import com.elastic.barretta.collectors.news.scrapers.APScraper
import com.elastic.barretta.collectors.news.scrapers.NewsAPIScraper
import groovy.json.JsonSlurper

/**
 * Main class
 */
class NewsCollector {

    static class Config {
        final static def DEFAULT_ES_URL = "http://localhost:9200"
        final static def DEFAULT_ES_INDEX = "news"
        final static def DEFAULT_ES_USER = "elastic"
        final static def DEFAULT_ES_PASS = "changeme"
        final static def DEFAULT_ES_TYPE = "doc"
        final static def DEFAULT_ENTITY_ES_INDEX = "news_entity_sentiment"

        ESClient.Config es = new ESClient.Config()
        NewsAPIScraper.Config newsApi = new NewsAPIScraper.Config()
        boolean clean = false
    }

    //TODO: this whole thing is probably OBE and should either be dropped or rewritten to handle the expanded functionality
    static void main(String[] args) {
        def cli = new CliBuilder(usage: "APNewCollector")
        cli.esUrl(args: 1, argName: "URL", "URL for ES [default: $Config.DEFAULT_ES_URL]")
        cli.esIndex(args: 1, argName: "index", "ES index name [default: $Config.DEFAULT_ES_INDEX]")
        cli.esUser(args: 1, argName: "user", "username for ES authentication [default: $Config.DEFAULT_ES_USER]")
        cli.esPass(args: 1, argName: "pass", "password for ES authentication [default: $Config.DEFAULT_ES_PASS]")
        cli.newsApiKey(args: 1, argName: "apiKey", "key for newsapi.org")
        cli.clean("drop and build the ES index")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        run(doConfig(options))
    }

    /**
     * do it
     * @param config config map
     * @return map with some result info
     */
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

    /**
     * drop indices if asked, but always try and create indices for news and entity sentiment from mappings defined in /resources
     * @param client ES client
     * @param clean to clean(drop indices) or not to clean
     */
    private static def prepESIndex(ESClient client, boolean clean) {
        if (clean) {
            client.deleteIndex(Config.DEFAULT_ES_INDEX)
            client.deleteIndex(Config.DEFAULT_ENTITY_ES_INDEX)
        }

        def newsMapping = new JsonSlurper().parse(this.classLoader.getResource("news_mapping.json")) as Map
        def entitySentimentMapping = new JsonSlurper().parse(this.classLoader.getResource("entity_sentiment_mapping.json")) as Map

        client.createIndex(newsMapping, Config.DEFAULT_ES_INDEX)
        client.createIndex(entitySentimentMapping, Config.DEFAULT_ENTITY_ES_INDEX)
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
        appConfig.newsApi.sources = propertyConfig.newsApi.sources
        appConfig.clean = cliConfig.clean ?: propertyConfig.clean ?: false
        appConfig.es = esConfig
        return appConfig
    }
}