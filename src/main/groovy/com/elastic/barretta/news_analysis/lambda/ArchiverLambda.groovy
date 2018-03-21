package com.elastic.barretta.news_analysis.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.elastic.barretta.news_analysis.Archiver
import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.NewsCollector

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * class for use by AWS Lambda
 */
class ArchiverLambda implements RequestHandler<NewsCollector.Config, String> {

    public String handleRequest(NewsCollector.Config request, Context context) {
        def propertyConfig = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))
        def esConfig = new ESClient.Config()

        def lastMonth = LocalDate.now().minusMonths(1)
        def defaultStart = lastMonth.withDayOfMonth(1)
        def defaultEnd = lastMonth.withDayOfMonth(lastMonth.lengthOfMonth())
        def defaultFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        request.indexPrefix = propertyConfig.indexPrefix ?: NewsCollector.DEFAULT_ES_INDEX_PREFIX

        esConfig.with {
            url = propertyConfig.es.url ?: NewsCollector.DEFAULT_ES_URL
            type = "doc"
            user = propertyConfig.es.user ?: NewsCollector.DEFAULT_ES_USER
            pass = propertyConfig.es.pass ?: NewsCollector.DEFAULT_ES_PASS
            index = request.indexPrefix //needed for validation's sake...doh
        }

        request.with {
            archiver.s3.bucket = propertyConfig.s3.bucket
            archiver.s3.prefix = propertyConfig.s3.prefix
            archiver.startDate = propertyConfig.archiver.startDate ?: defaultStart.format(defaultFormater)
            archiver.endDate = propertyConfig.archiver.endDate ?: defaultEnd.format(defaultFormater)
            archiver.outputFileName = null
            es = esConfig
        }

        Archiver.run(request)
        return "done"
    }
}