import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.Enricher

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
esClient = new ESClient(config.es as ESClient.Config)
enricher = new Enricher()

query = [
    bool: [
        must_not: [
            exists: [
                field: "entityNames"
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
    def doc = enricher.enrich(it._source)
    if (doc.entityNames) {
        esClient.updateDoc(it._id, doc)
    }
}
