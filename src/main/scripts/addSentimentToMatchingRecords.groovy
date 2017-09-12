import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.Enricher

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
esClient = new ESClient(config.es as ESClient.Config)
enricher = new Enricher()

query = [
    bool: [
        must_not: [
            exists: [
                field: "sentimentLabel"
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
    if (!it._source.text.trim().isEmpty()) {
        def doc = enricher.addSentiment(it._source)
        if (doc.entityObjects) {
            esClient.updateDoc(it._id, doc)
        }
    }
}
