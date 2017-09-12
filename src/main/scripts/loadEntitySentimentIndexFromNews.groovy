import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.NewsCollector
import com.elastic.barretta.news_analysis.Utils
import groovy.json.JsonSlurper

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
esClient = new ESClient(config.es as ESClient.Config)

def entitySentimentMapping = new JsonSlurper().parse(this.classLoader.getResource("sentiment_mapping.json")) as Map

esClient.deleteIndex(NewsCollector.DEFAULT_ENTITY_ES_INDEX)
esClient.createIndex(entitySentimentMapping, NewsCollector.DEFAULT_ENTITY_ES_INDEX)

query = [
   match_all: [boost: 1]
]

esClient.scrollQuery(query, 500, "1m") {
    if (!it._source.text.isEmpty() && it._source.entityObjects && !it._source.entityObjects.isEmpty()) {
        Utils.writeEntitySentimentsToOwnIndex(it._id, it._source, esClient)
    }
}
