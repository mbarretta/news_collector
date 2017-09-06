package com.elastic.barretta.collectors.news.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.elastic.barretta.collectors.news.ESClient
import com.elastic.barretta.collectors.news.NewsCollector
import com.elastic.barretta.collectors.news.scrapers.NewsAPIScraper
import groovy.json.JsonOutput

/**
 * class for use by AWS Lambda
 */
class NewsCollectorLambda implements RequestHandler<NewsCollector.Config, String> {

    public String handleRequest(NewsCollector.Config request, Context context) {
        def esConfig = new ESClient.Config()
        def newsApiConfig = new NewsAPIScraper.Config()
        def propertyConfig = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))

        esConfig.with {
            url = request.es.url ?: propertyConfig.es.url ?: NewsCollector.Config.DEFAULT_ES_URL
            index = request.es.url ?: propertyConfig.es.index ?: NewsCollector.Config.DEFAULT_ES_INDEX
            type = NewsCollector.Config.DEFAULT_ES_TYPE
            user = request.es.user ?: propertyConfig.es.user ?: NewsCollector.Config.DEFAULT_ES_USER
            pass = request.es.pass ?: propertyConfig.es.pass ?: NewsCollector.Config.DEFAULT_ES_PASS
        }
        newsApiConfig.with {
            key = propertyConfig.newsApi.key
            sources = propertyConfig.newsApi.sources ?: []
        }

        request.es = esConfig
        request.newsApi = newsApiConfig
        request.clean = request.clean ?: propertyConfig.clean ?: false

        return JsonOutput.toJson(NewsCollector.run(request))
    }
}