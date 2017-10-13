import com.elastic.barretta.news_analysis.Enricher
import com.elastic.barretta.news_analysis.NewsCollector
import wslite.http.auth.HTTPBasicAuthorization
import wslite.rest.RESTClient


//todo: put in sync with actual momentum code


client = new RESTClient("https://07b49e0aa73d320250c94080361f76d5.us-east-1.aws.found.io:9243")
client.authorization = new HTTPBasicAuthorization("elastic", "a08KbLCCWhr0J9IBh97ULfKX")

date = Calendar.instance
date.set(2017, 6, 1)
while (date.before(Calendar.instance)) {

//    Enricher.calculateMomentum(client, date.getTime())

    dateString = date.format("yyyy-MM-dd")
    println "$dateString ..."

    body = [
        query: [
            range: [
                date_published: [
                    gte: "$dateString 11:11:11||-2d/d",
                    lte: "$dateString 11:11:11||/d"
                ]
            ]
        ],
        aggs: [
            daily: [
                date_histogram: [
                    field   : "date_published",
                    interval: "day"
                ],
                aggs: [
                    entities: [
                        terms: [
                            field: "entityPeople.keyword",
                            size : 10000

                        ],
                        aggs: [
                            sources: [
                                terms: [
                                    field: "source",
                                    size : 50
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]
    response = client.post(path: 'news/_search?size=0') {
        json body
    }

    data = [:].withDefault { 0 as double }
    debug = [:].withDefault { [] }
    buckets = response.json.aggregations.daily.buckets

    //trying to do [1]/[0] + [2]/[1]
    def sourceCount = buckets.entities.buckets.sources.buckets.collect { it.key }.flatten().unique().size()
    buckets.eachWithIndex { bucket, i ->
        bucket.entities.buckets.each { entity ->
            if (i < 2) {
                def match = buckets[i + 1].entities.buckets.find { it.key == entity.key }
                diff = (match) ? match.doc_count / entity.doc_count : entity.doc_count
                data[entity.key] += (diff * Math.min(1/2 * sourceCount, 2 as double))
            }

            //can't reach ahead to the next bucket anymore...
            else {

                //if we're in the 3rd bucket and haven't seen this guy yet, throw him in, otherwise he's already been considered during last loop
                if (!data.containsKey(entity.key)) {
                    data[entity.key] += entity.doc_count
                }
            }
            debug[entity.key] << entity.doc_count
        }
    }

    post = new StringBuilder()
    data.each { k, v ->
        post.append('{"index":{}}').append("\n").append('{"name": "' + k + '", "score": "' + v + '", "date":"' + dateString + '"}').append("\n")
    }

    client.put(path: "/${NewsCollector.DEFAULT_MOMENTUM_ES_INDEX}/doc/_bulk") {
        type "application/x-ndjson"
        text post.toString()
    }
    println "...done"
    date.add(Calendar.DAY_OF_MONTH, 1)
}
