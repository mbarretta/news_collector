package com.elastic.barretta.collectors.news

class Utils {
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
                    NewsCollector.Config.DEFAULT_ENTITY_ES_INDEX
                )
            }
        }
    }
}
