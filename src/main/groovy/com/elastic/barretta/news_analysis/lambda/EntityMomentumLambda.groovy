package com.elastic.barretta.news_analysis.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.EntityMomentum
import com.elastic.barretta.news_analysis.NewsCollector
import groovy.util.logging.Slf4j

@Slf4j
class EntityMomentumLambda implements RequestHandler<NewsCollector.Config, String> {

    public String handleRequest(NewsCollector.Config request, Context context) {
        def propertyConfig = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))

        request.indexPrefix = request.indexPrefix ?: NewsCollector.DEFAULT_ES_INDEX_PREFIX

        request.es.with {
            url = url ?: propertyConfig.es.url ?: NewsCollector.DEFAULT_ES_URL
            type = "doc"
            user = user ?: propertyConfig.es.user ?: NewsCollector.DEFAULT_ES_USER
            pass = pass ?: propertyConfig.es.pass ?: NewsCollector.DEFAULT_ES_PASS
            index = request.momentum_index
        }

        def client = new ESClient(request.es)

        return EntityMomentum.run(client)
    }
}
