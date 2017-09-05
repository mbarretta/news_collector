import com.elastic.barretta.collectors.news.ESClient
import com.elastic.barretta.collectors.news.Enricher

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
esClient = new ESClient(config.es as ESClient.Config)
enricher = new Enricher()

query = [
    bool: [
        must_not: [
            exists: [
                field: "entityObjects"
            ]
        ],
        must: [
            exists: [
                field: "text"
            ]
        ]
    ]
]

esClient.scrollQuery(query, 500, "1m") {
    def doc = enricher.addEntities(it._source)
    esClient.updateDoc(it._id, doc)
}
