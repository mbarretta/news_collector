import com.elastic.barretta.news_analysis.ESClient

import java.util.concurrent.ConcurrentLinkedQueue

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
esClient = new ESClient([url: config.es.url, index: config.es.index, user: config.es.user, pass: config.es.pass] as ESClient.Config)
localClient = new ESClient([url: "http://localhost:9200"] as ESClient.Config)

esClient.createIndex([
    "settings": [
        "index": [
            "number_of_shards"  : 1,
            "number_of_replicas": 1
        ]
    ]
], "news_text_terms")

query = [
    bool: [
        must_not: [
            term: [
                "text": ""
            ]
        ]
    ]
]
records = [] as ConcurrentLinkedQueue
esClient.scrollQuery(query, 200, "1m") { record ->
    if (record._source.text != "") {
        def response = localClient.post(path: "/_analyze") {
            json analyzer: "stop",
                filter: ["lowercase"],
                text: record._source.text
        }
        def tokens = response.json.tokens.collect { it.token }
        if (tokens.size() > 0) {
            records.add([terms: tokens])
        }
//        esClient.postDoc([terms: tokens], "news_text_terms")
        synchronized (records) {
            if (records.size() > 200) {
                esClient.bulkInsert(Arrays.asList(records.toArray()), "news_text_terms", "doc")
                records.clear()
            }
        }
    }
}
esClient.bulkInsert(Arrays.asList(records.toArray()), "news_text_terms", "doc")
