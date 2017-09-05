import com.elastic.barretta.collectors.news.ESClient
import com.elastic.barretta.collectors.news.Utils

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
config.es.index="news2"
esClient = new ESClient(config.es as ESClient.Config)

query = [
   match_all: [boost: 1]
]

response = esClient.post(path: "/${config.es.index}/_search") {
    json query
}

esClient.scrollQuery(query, 500, "1m") {
    Utils.writeEntitySentimentsToOwnIndex(it._source, client)
}