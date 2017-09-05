package com.elastic.barretta.collectors.news

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import groovyx.gpars.GParsPool
import wslite.http.auth.HTTPBasicAuthorization
import wslite.rest.RESTClient
import wslite.rest.RESTClientException

/**
 * lightweight ES client
 */
@Slf4j
class ESClient {

    @Delegate
    private RESTClient client
    private Config config

    static class Config {
        String url
        String index
        String type = "doc"
        String user
        String pass

        @Override
        public String toString() {
            return "Config{" +
                "\nurl [" + url + ']' +
                "\nindex [" + index + ']' +
                "\ntype [" + type + ']' +
                "\nuser [" + user + ']' +
                "\npass [hidden] " +
                '\n}'
        }
    }

    ESClient(Config config) {
        this.client = new RESTClient(config.url)
        this.config = config
        if (config.user) {
            client.authorization = new HTTPBasicAuthorization(config.user, config.pass)
        }
        testClient()
    }

    def deleteIndex(String index = config.index) {
        log.info("deleting index [$config.url/$index]")
        try {
            client.delete(path: "/$index")
        } catch (RESTClientException e) {
            if (e.response.statusCode == 404) {
                log.warn("index does not exist")
            }
        }
    }

    def createIndex(Map payload = null, String index = config.index) {
        if (!indexExists(index)) {
            log.info("creating index [$config.url/$index]")
            client.put(path: "/$index") {
                if (payload) {
                    json payload
                }
            }
        } else {
            log.info("index [$index] already exists")
        }
    }

    def indexExists(index = config.index) {
        def exists = true
        try {
            client.head(path: "/$index")
        } catch (RESTClientException e) {
            if (e.response.statusCode == 404) {
                exists = false
            }
        }
        return exists
    }

    def postDoc(Map content, index = config.index, type = config.type) {
        try {
            def response = client.post(path: "/$index/$type") {
                json content
            }
            return response.json._id
        } catch (RESTClientException e) {
            log.error("error posting doc [$e.message] // path [/$index/$type]")
            if (e.response.statusCode == 400) {
                log.error("detail [\n${JsonOutput.prettyPrint(new String(e.response.data))}\n]")
            }
        }
    }

    def updateDoc(id, Map content, index = config.index, type = config.type) {
        try {
            client.put(path: "/$index/$type/$id") {
                json content
            }
        } catch (RESTClientException e) {
            log.error("error updating doc [$e.message] // path [/$index/$type/$id] content [\n$content\n]")
            if (e.response.statusCode == 400) {
                log.error("detail [\n${JsonOutput.prettyPrint(new String(e.response.data))}\n]")
            }
        }
    }

    def docExists(String field, String value, String index = config.index, String type = config.type) {
        def returnVal = true
        try {
            if (value) {
                def response = client.post(path: "/$index/$type/_search") {
                    json size: 0, query: [match: [(field): value]]
                }
                returnVal = response.json.hits.total > 0
            }
        } catch (RESTClientException e) {
            log.error("error determining doc existence [$e.cause] // path [/$index/$type/_search] field [$field] value [$value]")
            if (e.response.statusCode == 400) {
                log.error("detail [\n${JsonOutput.prettyPrint(new String(e.response.data))}\n]")
            }
        }
        return returnVal
    }

    def getDocByUniqueField(String uniqueField, String value, String index = config.index, String type = config.type) {
        def returnObj = [:]
        try {
            def response = client.post(path: "/$index/$type/_search") {
                json size: 1, query: [match: [(uniqueField): value]]
            }

            returnObj = response.json.hits.hits[0]
        } catch (RESTClientException e) {
            log.error("error fetching doc by unique field [$e.cause] // path [/$index/$type/_search] field [$uniqueField] value [$value]")
            if (e.response.statusCode == 400) {
                log.error("detail [\n${JsonOutput.prettyPrint(new String(e.response.data))}\n]")
            }
        }
        return returnObj
    }

    def scrollQuery(Map body, int batchSize = 100, String keepAlive = "1m", Closure mapFunction) {
        try {
            def response = client.post(path: "/$config.index/$config.type/_search?scroll=$keepAlive") {
                json size: batchSize, query: body
            }

            log.info("found [${response.json.hits.total}] records...")
            def scrollId = response.json._scroll_id

            GParsPool.withPool {
                def asyncMapFunction = mapFunction.async()

                //do first batch
                response.json.hits.hits.collect().each {
                    asyncMapFunction(it as Map)
                }

                //do other batches if we need to
                while (response.json.hits.hits.size() >= batchSize) {
                    response = client.post(path: "/_search/scroll") {
                        json scroll: keepAlive, scroll_id: scrollId
                    }
                    log.info("...queuing batch w/ [${response.json.hits.hits.size()}] results")
                    response.json.hits.hits.collect().each {
                        asyncMapFunction(it as Map)
                    }
                    scrollId = response.json._scroll_id
                }
            }
        } catch (RESTClientException e) {
            log.error("error running scroll query [$e.cause] ")
            if (e.response.statusCode == 400) {
                log.error("detail [\n${JsonOutput.prettyPrint(new String(e.response.data))}\n]")
            }
        }
    }

    private testClient() {
        try {
            client.httpClient.connectTimeout = 5000
            client.get()
            log.info("able to connect to ES [$config.url]")
        } catch (RESTClientException e) {
            log.error("unable to connect to ES [${e.message}]\n$config")
            System.exit(1)
        }
    }
}