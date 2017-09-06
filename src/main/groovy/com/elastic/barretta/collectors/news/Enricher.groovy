package com.elastic.barretta.collectors.news

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
                    log.warn("unable to establish connecti on to Rosette API [${config.enrichment.rosetteApi}]")
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
}