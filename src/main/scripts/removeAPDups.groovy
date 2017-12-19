import com.elastic.barretta.news_analysis.ESClient

config = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
esClient = new ESClient(config.es as ESClient.Config)

query = [
    size: 0,
    aggs: [
        dups: [
            terms: [
                field: "shortId",
                size: 1000,
                min_doc_count: 2
            ],
            aggs: [
                ids: [
                  top_hits: [
                      size: 100,
                      _source: [
                          include: "shortId"
                      ],
                      sort: [[
                          date: [
                              order: "desc"
                          ]
                      ]]
                  ]
                ]
            ]
        ]
    ]
]

response = esClient.post(path: "/${config.es.index}/_search") {
    json query
}

response.json.aggregations.dups.buckets.each {
    println "[$it.key] - [${it.ids.hits.total}]"
    it.ids.hits.hits.collect().drop(1).each {
        esClient.delete(path: "/${config.es.index}/doc/${it._id}")
    }
}