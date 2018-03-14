package com.elastic.barretta.news_analysis

import com.elastic.barretta.analytics.rosette.RosetteApiClient
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class Enricher {
    RosetteApiClient rosette

    Enricher() {
        def config = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))

        if (config.enrichment) {
            if (config.enrichment.rosetteApi) {
                try {
                    rosette = new RosetteApiClient(config.enrichment.rosetteApi as RosetteApiClient.Config)
                } catch (e) {
                    rosette = null
                    log.warn("unable to establish connection to Rosette API [${config.enrichment.rosetteApi}]")
                }
            } else {
                log.info("Rosette API config is not present - skipping init")
            }
        } else {
            log.warn("missing enrichment{} configuration - skipping enrichment")
        }
    }

    def enrich(Map doc) {
        if (rosette) {
            def entities = rosette.getEntities(doc.text)
            doc = addEntities(doc, entities)
            doc = addLocations(doc, entities)
            doc = addSentiment(doc)
        }
        return doc
    }

    def addEntities(Map doc, List entities = null) {
        if (rosette) {
            if (!entities) {
                entities = rosette.getEntities(doc.text)
            }

            doc.entityNames = []
            doc.entityPeople = []
            doc.entityResolvedPeople = []
            doc.entityOrgs = []
            doc.entityResolvedOrgs = []
            doc.entityObjects = []
            entities.each {
                doc.entityNames << it.normalized
                doc.entityObjects << [id: it.entityId, name: it.normalized, type: it.type, count: it.count]
                if (it.type == "PERSON") {
                    doc.entityPeople << it.normalized
                    if (it.entityId.startsWith("Q")) {
                        doc.entityResolvedPeople << it.normalized
                    }
                } else if (it.type == "ORGANIZATION") {
                    doc.entityOrgs << it.normalized
                    if (it.entityId.startsWith("Q")) {
                        doc.entityResolvedOrgs << it.normalized
                    }
                }

            }
        }
        return doc
    }

    def addLocations(Map doc, List entities = null) {
        if (rosette) {
            if (!entities) {
                entities = rosette.getEntities(doc.text)
            }

            doc.locations = []
            doc.entityLocations = []
            doc.entityResolvedLocations = []
            doc.locationObjects = []
            entities.findAll { it.type == "LOCATION" }.each {
                doc.entityLocations << it.normalized
                def location = new JsonSlurper().parse("https://www.wikidata.org/w/api.php?format=json&action=wbgetentities&ids=${it.entityId}".toURL())
                if (location) {
                    def geo = location?.entities?."${it.entityId}"?.claims?.find {
                        it.value[0].mainsnak.datatype == "globe-coordinate" && it.value[0].mainsnak.datavalue.value.latitude != null
                    }
                    if (geo) {
                        geo = geo.value[0].mainsnak.datavalue.value
                        doc.locations << [lat: geo.latitude, lon: geo.longitude]
                        doc.entityResolvedLocations << it.normalized
                        doc.locationObjects << [name: it.normalized, geo: [lat: String.valueOf(geo.latitude), lon: String.valueOf(geo.longitude)]]
                    }
                }
            }
        }
        return doc
    }

    def addSentiment(Map doc) {
        if (rosette) {
            def sentimentMap = rosette.getSentiment(doc.text)
            if (sentimentMap) {
                doc.sentimentLabel = sentimentMap.document.label
                doc.sentimentConfidence = sentimentMap.document.confidence

                if (doc.entityObjects) {
                    doc.entityObjects.collect { target ->
                        def source = sentimentMap.entities.find { target.id == it.entityId }
                        target.sentimentLabel = source?.sentiment?.label
                        target.sentimentConfidence = source?.sentiment?.confidence
                    }
                }
            }
        }
        return doc
    }

    static def calculateMomentum(ESClient client, Date date = new Date()) {
        def dateString = date.format("yyyy-MM-dd")

        def body = [
            query: [
                range: [
                    date: [
                        gte: "$dateString 11:11:11||-3d/d",
                        lte: "$dateString 11:11:11||-1d/d"
                    ]
                ]
            ],
            aggs : [
                daily: [
                    date_histogram: [
                        field   : "date",
                        interval: "day"
                    ],
                    aggs          : [
                        entities: [
                            terms: [
                                field: "entityPeople.keyword",
                                size : 10000

                            ],
                            aggs : [
                                sources: [
                                    terms: [
                                        field        : "source",
                                        size         : 50,
                                        min_doc_count: 1
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
        def response = client.post(path: "${NewsCollector.DEFAULT_ES_INDEX_PREFIX}/_search?size=0") {
            json body
        }

        def data = [:].withDefault { 0 as double }
        def buckets = response.json.aggregations.daily.buckets

        // trying to do log(avg())*[1]/[0] + [2]/[1]) * min( (1/5) * numSources, 2)
        // intuition is to look at the "average" change in the mentions for this entity over the past three days,
        // adjust it on a log scale to boost score of those with a lot of mentions, and then increase that score to
        // reward those (up to 2x) who show up in more than one source - reward maxes out after 10 sources
        buckets.eachWithIndex { bucket, i ->

            //look at all three buckets and do the things
            bucket.entities.buckets.each { entity ->

                if (i < 2) {
                    def match = buckets[i + 1].entities.buckets.find { it.key == entity.key }
                    def diff = (match) ? match.doc_count / entity.doc_count : entity.doc_count
                    data[entity.key] += diff
                }

                // can't reach ahead to the next bucket anymore...
                else {

                    // if we're in the 3rd bucket and haven't seen this guy yet, throw him in
                    // otherwise he's already been considered during last loop
                    if (!data.containsKey(entity.key)) {
                        data[entity.key] += entity.doc_count
                    }

                    //finish our scoring considering data from all three buckets
                    def parent = buckets.entities.buckets.flatten().findAll { it.key == entity.key }
                    def sourceWeight = Math.min(parent.sources.buckets.flatten().size() / 5, 2 as double)
                    def mentionWeight = Math.log(parent.collect { it.doc_count }.sum() / 3)
                    data[entity.key] =  sourceWeight * mentionWeight * data[entity.key] as double
                }
            }
        }
        return data
    }
}