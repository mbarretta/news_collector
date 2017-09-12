package com.elastic.barretta.news_analysis

class Utils {
    // todo: this is bad becasue it doesn't use the app config's sentiment_index value - might need to create a static singleton for
    // the config so it can be reached everywhere
    static def writeEntitySentimentsToOwnIndex(String id, Map doc, ESClient client) {
        def date = doc.date_published
        doc.entityObjects?.each {
            if (["PERSON", "ORGANIZATION", "LOCATION"].contains(it.type) && it.sentimentLabel) {
                client.postDoc(
                    [
                        date      : date,
                        name      : it.name,
                        type      : it.type,
                        sentiment : it.sentimentLabel,
                        confidence: it.sentimentConfidence,
                        value     : it.sentimentLabel == "pos" ? 1 : it.sentimentLabel == "neg" ? -1 : 0,
                        articleId : id,
                        source    : it.source
                    ],
                    NewsCollector.DEFAULT_ENTITY_ES_INDEX
                )
            }
        }
    }
}
