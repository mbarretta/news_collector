package com.elastic.barretta.news_analysis

import groovy.util.logging.Slf4j

@Slf4j
class EntityMomentum {

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "EntityMomentum")
        cli.url(args: 1, argName: "URL", "URL for ES [default: $NewsCollector.DEFAULT_ES_URL]")
        cli.indexPrefix(args: 1, argName: "prefix", "ES index prefix [default: $NewsCollector.DEFAULT_ES_INDEX_PREFIX]")
        cli.user(args: 1, argName: "user", "username for ES authentication [default: $NewsCollector.DEFAULT_ES_USER]")
        cli.pass(args: 1, argName: "pass", "password for ES authentication [default: $NewsCollector.DEFAULT_ES_PASS]")
        cli.propertiesFile(args: 1, argName: "file", "properties file")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        def propertyConfig
        if (options.propertiesFile) {
            propertyConfig = new ConfigSlurper().parse(this.getResource(options.propertiesFile as String))
        } else {
            propertyConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))
        }

        def request = new NewsCollector.Config()

        request.indexPrefix = options.indexPrefix ?: NewsCollector.DEFAULT_ES_INDEX_PREFIX
        request.es.with {
            url = options.url ?: propertyConfig.es.url ?: NewsCollector.DEFAULT_ES_URL
            type = "doc"
            user = options.user ?: propertyConfig.es.user ?: NewsCollector.DEFAULT_ES_USER
            pass = options.pass ?: propertyConfig.es.pass ?: NewsCollector.DEFAULT_ES_PASS
            index = request.momentum_index
        }

        run(new ESClient(request.es))

    }

    static def run(ESClient esClient, date = new Date().format("yyyy-MM-dd")) {
        def insertedRecords = 0
        log.info("scoring momentum for [$date]")

        //prevent duplicates by looking for data from "today"
        if (!esClient.docExists("date", date)) {

            def results = Enricher.calculateMomentum(esClient, Date.parse("yyyy-MM-dd",date))
            if (!results.isEmpty()) {
                def postList = results.inject([]) { l, k, v ->
                    l << [name: k, score: v, date: date]
                }
                insertedRecords = esClient.bulkInsert(postList)
            } else {
                log.info("no records found, so no momentum records generated")
            }
        } else {
            log.info("documents with date found - skipping")
        }
        return insertedRecords
    }
}
