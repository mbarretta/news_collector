package com.elastic.barretta.collectors.news.lambda

import com.elastic.barretta.collectors.news.NewsCollector
import com.elastic.barretta.collectors.news.ESClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import groovy.json.JsonOutput

/**
 * class for use by AWS Lambda
 */
class NewsCollectorLambda implements RequestHandler<Request, String> {

    static class Request extends ESClient.Config {
        String clean
        String newsApiKey
    }

    public String handleRequest(Request request, Context context) {
        def esConfig = [
            url  : request.url,
            index: request.index ?: NewsCollector.DEFAULT_ES_INDEX,
            type : NewsCollector.DEFAULT_ES_TYPE,
            user : request.user ?: NewsCollector.DEFAULT_ES_USER,
            pass : request.pass ?: NewsCollector.DEFAULT_ES_PASS
        ] as ESClient.Config

        def appConfig = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))
        appConfig.es = esConfig
        appConfig.clean = request.clean
        appConfig.newsApi.apiKey = request.newsApiKey

        return JsonOutput.toJson(NewsCollector.run(appConfig))
    }
}