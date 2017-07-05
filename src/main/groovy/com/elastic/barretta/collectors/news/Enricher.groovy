package com.elastic.barretta.collectors.news

import com.elastic.barretta.analytics.rosette.RosetteApiClient
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class Enricher {
    RosetteApiClient rosette

    Enricher() {
        def config = new ConfigSlurper().parse(this.class.classLoader.getResource("properties.groovy"))

        try {
            rosette = new RosetteApiClient(config.rosetteApi as RosetteApiClient.Config)
            rosette.test()
        } catch (e) {
            log.warn("unable to establish connection to Rosette API")
        }
    }

    def enrich(Map doc) {
        if (rosette) {
            def entities = rosette.getEntities(doc.text)
            doc = addEntities(doc, entities)
            doc = addLocations(doc, entities)
        }
        return doc
    }

    def addEntities(Map doc, entities = null) {
        if (rosette) {
            if (!entities) {
                entities = rosette.getEntities(doc.text)
            }

            doc.entityNames = []
            doc.entityObjects = []
            entities.each {
                doc.entityNames << it.normalized
                doc.entityObjects << [id: it.entityId, name: it.normalized, type: it.type, count: it.count]
            }
        }
        return doc
    }

    def addLocations(Map doc, entities = null) {
        if (rosette) {
            if (!entities) {
                entities = rosette.getEntities(doc.text)
            }

            doc.locations = []
            entities.findAll { it.type == "LOCATION" }.each {
                def location = new JsonSlurper().parse("https://www.wikidata.org/w/api.php?format=json&action=wbgetentities&ids=${it.entityId}".toURL())
                if (location) {
                    def geo = location?.entities?."${it.entityId}"?.claims?.find {
                        it.value[0].mainsnak.datatype == "globe-coordinate" && it.value[0].mainsnak.datavalue.value.latitude != null
                    }
                    if (geo) {
                        geo = geo.value[0].mainsnak.datavalue.value
                        doc.locations << [lat: geo.latitude, lon: geo.longitude]
                    }
                }
            }
        }
        return doc
    }
}