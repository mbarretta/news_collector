package com.elastic.barretta.news_analysis

class EntityMomentum extends NewsCollector {

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "APNewCollector")
        cli.url(args: 1, argName: "URL", "URL for ES [default: $DEFAULT_ES_URL]")
        cli.index(args: 1, argName: "index", "ES index name [default: $DEFAULT_ES_INDEX]")
        cli.user(args: 1, argName: "user", "username for ES authentication [default: $DEFAULT_ES_USER]")
        cli.pass(args: 1, argName: "pass", "password for ES authentication [default: $DEFAULT_ES_PASS]")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        def propertyConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))
        def request = new Config()

        request.es.with {
            url = options.url ?: propertyConfig.es.url ?: DEFAULT_ES_URL
            index = options.index ?: propertyConfig.es.momentum_index ?: DEFAULT_MOMENTUM_ES_INDEX
            type = "doc"
            user = options.user ?: propertyConfig.es.user ?: DEFAULT_ES_USER
            pass = options.pass ?: propertyConfig.es.pass ?: DEFAULT_ES_PASS
        }

        run(new ESClient(request.es))

    }

    static def run(ESClient esClient) {
        def date = new Date().format("yyyy-MM-dd")
        def insertedRecords = 0

        //prevent duplicates by looking for data from "today"
        if (!esClient.docExists("date", date)) {

            log.info("scoring momentum for [$date]")

            def results = Enricher.calculateMomentum(esClient)
            def postList = results.inject([]) { l, k, v ->
                l << [name: k, score: v, date: date]
            }
            insertedRecords = esClient.bulkInsert(postList)
        }
        return insertedRecords
    }
}
