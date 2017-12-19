package com.elastic.barretta.news_analysis.lambda

import com.elastic.barretta.news_analysis.NewsCollector
import spock.lang.Ignore
import spock.lang.Specification

class EntityMomentumLambdaTest extends Specification {
    @Ignore
    def "HandleRequest"() {
        expect:
        new EntityMomentumLambda().handleRequest(new NewsCollector.Config(), null) != null
    }
}
