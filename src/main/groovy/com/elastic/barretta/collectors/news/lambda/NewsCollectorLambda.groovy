package com.elastic.barretta.collectors.news.lambda

import com.elastic.barretta.collectors.news.NewsCollector
import com.elastic.barretta.collectors.news.ESClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import groovy.json.JsonOutput

/**
 * class for use by AWS Lambda
 */
class NewsCollectorLambda implements RequestHandler<NewsCollector.Config, String> {

    public String handleRequest(NewsCollector.Config request, Context context) {
        def esConfig = new ESClient.Config()
        def propertyConfig = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))

        esConfig.with {
            url = request.es.url ?: propertyConfig.es.url ?: NewsCollector.Config.DEFAULT_ES_URL
            index = request.es.url ?: propertyConfig.es.index ?: NewsCollector.Config.DEFAULT_ES_INDEX
            type = NewsCollector.Config.DEFAULT_ES_TYPE
            user = request.es.user ?: propertyConfig.es.user ?: NewsCollector.Config.DEFAULT_ES_USER
            pass = request.es.pass ?: propertyConfig.es.pass ?: NewsCollector.Config.DEFAULT_ES_PASS
        }
        request.es = esConfig
        request.newsApi.key = request.newsApi.key ?: propertyConfig.newsApi.key
        request.clean = request.clean ?: propertyConfig.clean ?: false

        return JsonOutput.toJson(NewsCollector.run(request))
    }
}