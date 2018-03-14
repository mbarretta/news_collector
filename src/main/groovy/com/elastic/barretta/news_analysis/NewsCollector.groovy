package com.elastic.barretta.news_analysis

import com.elastic.barretta.news_analysis.scrapers.APScraper
import com.elastic.barretta.news_analysis.scrapers.NewsAPIScraper
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
/**
 * Main class
 */
@Slf4j
class NewsCollector {

    final static def DEFAULT_ES_URL = "http://localhost:9200"
    final static def DEFAULT_ES_INDEX_PREFIX = "news"
    final static def DEFAULT_ES_USER = "elastic"
    final static def DEFAULT_ES_PASS = "changeme"
    final static def DEFAULT_ES_TYPE = "doc"
    final static def ENTITY_ES_INDEX = "entity_sentiment"
    final static def MOMENTUM_ES_INDEX = "entity_momentum"

    static class Config {
        ESClient.Config es = new ESClient.Config()

        NewsAPIScraper.Config newsApi = new NewsAPIScraper.Config()
        Archiver.Config archiver = new Archiver.Config()
        boolean clean = false
        String indexPrefix = DEFAULT_ES_INDEX_PREFIX
        String news_index
        String momentum_index
        String sentiment_index

        def setIndexPrefix(p) {
            indexPrefix = p
            news_index = indexPrefix
            momentum_index = indexPrefix + "_" + MOMENTUM_ES_INDEX
            sentiment_index = indexPrefix + "_" + ENTITY_ES_INDEX
        }

        def isValid() {
            def valid = true
            valid &= es.isValid()
            valid &= (indexPrefix != null && !indexPrefix.isEmpty())
            return valid
        }

        @Override
        public String toString() {
            return "Config{" +
                "es=" + es +
                ", newsApi=" + newsApi +
                ", archiver=" + archiver +
                ", clean=" + clean +
                ", indexPrefix=" + indexPrefix +
                '}'
        }
    }

    //TODO: this whole thing is OBE and should either be dropped or rewritten to handle the expanded functionality
    static void main(String[] args) {
        def cli = new CliBuilder(usage: "NewCollector")
        cli.esUrl(args: 1, argName: "URL", "URL for ES [default: $DEFAULT_ES_URL]")
        cli.esIndexPrefix(args: 1, argName: "prefix", "ES index prefix [default: $DEFAULT_ES_INDEX_PREFIX]")
        cli.esUser(args: 1, argName: "user", "username for ES authentication [default: $DEFAULT_ES_USER]")
        cli.esPass(args: 1, argName: "pass", "password for ES authentication [default: $DEFAULT_ES_PASS]")
        cli.newsApiKey(args: 1, argName: "apiKey", "key for newsapi.org")
        cli.propertiesFile(args: 1, argName: "propertiesFile", "properties file location")
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
        log.debug("running with config: [$config]")
        def client = new ESClient(config.es)

        prepESIndex(client, config)

        config.es.index = config.news_index
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
     * @param app config
     */
    private static def prepESIndex(ESClient client, Config config) {
        if (config.clean) {
            client.deleteIndex(config.news_index)
            client.deleteIndex(config.sentiment_index)
            client.deleteIndex(config.momentum_index)
        }

        def newsMapping = new JsonSlurper().parse(this.classLoader.getResource("news_mapping.json")) as Map
        def sentimentMapping = new JsonSlurper().parse(this.classLoader.getResource("sentiment_mapping.json")) as Map
        def momentumMapping = new JsonSlurper().parse(this.classLoader.getResource("momentum_mapping.json")) as Map

        client.createIndex(newsMapping, config.news_index)
        client.createIndex(sentimentMapping, config.sentiment_index)
        client.createIndex(momentumMapping, config.momentum_index)
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
        def newsApiConfig = new NewsAPIScraper.Config()
        def propertyConfig
        if (cliConfig.propertiesFile) {
            propertyConfig = new ConfigSlurper().parse(this.getResource(cliConfig.propertiesFile as String))
        } else {
            propertyConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))
        }

        appConfig.newsApi = newsApiConfig
        appConfig.clean = cliConfig.clean ?: propertyConfig.clean ?: false
        appConfig.indexPrefix = propertyConfig.indexPrefix

        esConfig.with {
            url = cliConfig.esUrl ?: propertyConfig.es.url ?: DEFAULT_ES_URL
            type = DEFAULT_ES_TYPE
            user = cliConfig.esUser ?: propertyConfig.es.user ?: DEFAULT_ES_USER
            pass = cliConfig.esPass ?: propertyConfig.es.pass ?: DEFAULT_ES_PASS
            index = appConfig.news_index //doing this mainly so we an pass validation
        }
        newsApiConfig.with {
            key = propertyConfig.newsApi.key
            sources = propertyConfig.newsApi.sources ?: []
        }
        appConfig.es = esConfig


        assert appConfig.isValid() :  "config is hosed...fix it"

        return appConfig
    }
}