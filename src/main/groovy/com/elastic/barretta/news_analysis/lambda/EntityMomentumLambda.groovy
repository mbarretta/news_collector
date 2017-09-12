package com.elastic.barretta.news_analysis.lambda

import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.Context
import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.Enricher
import com.elastic.barretta.news_analysis.NewsCollector
import groovy.util.logging.Slf4j

@Slf4j
class EntityMomentumLambda implements RequestHandler<NewsCollector.Config, String> {

    public String handleRequest(NewsCollector.Config request, Context context) {
        def propertyConfig = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))

        request.es.with {
            url = url ?: propertyConfig.es.url ?: NewsCollector.DEFAULT_ES_URL
            index = request.momentum_index ?: propertyConfig.es.momentum_index ?: NewsCollector.DEFAULT_MOMENTUM_ES_INDEX
            type = "doc"
            user = user ?: propertyConfig.es.user ?: NewsCollector.DEFAULT_ES_USER
            pass = pass ?: propertyConfig.es.pass ?: NewsCollector.DEFAULT_ES_PASS
        }

        def client = new ESClient(request.es)
        def date = new Date().format("yyyy-MM-dd")
        def insertedRecords = 0

        //prevent duplicates by looking for data from "today"
        if (!client.docExists("date", date)) {

            log.info("scoring momentum for [$date]")

            def results = Enricher.calculateMomentum(client)
            def postList = results.inject([]) { l, k, v ->
                l << [name: k, score: v, date: date]
            }
            insertedRecords = client.bulkInsert(postList)
        }
        return insertedRecords
    }
}
