package com.elastic.barretta.collectors.news.scrapers

import com.elastic.barretta.collectors.news.ESClient
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

/**
 * Pull docs from AP and push into ES using supplied client
 */
@Slf4j
class APScraper {

    static def scrape(ESClient client) {

        def results = [:]
        final def AP_URL = "https://afs-prod.appspot.com/api/v2/feed/tag?tags="

        //fetch most recent article "cards" from each section
        [
            "apf-topnews",
            "apf-usnews",
            "apf-intlnews",
            "apf-business",
            "apf-politics",
            "apf-technology",
            "apf-Health",
            "apf-science"
        ].each { tag ->
            log.info("fetching for [$tag]")

            //get all the urls from each card
            def articleUrls = new JsonSlurper().parse((AP_URL + tag).toURL()).cards.contents*.gcsUrl.flatten()
            log.trace("...found [${articleUrls.size()}] articles")

            //fetch article for each url found
            def posted = 0
            articleUrls.each { url ->
                def article = new JsonSlurper().parse(url.toURL())

                //quality-check - not all "articles" are actually articles
                if (article && article.storyHTML && !article.shortId.contains(":")) {

                    //filter duplicates
                    if (!client.docExists("shortId", article.shortId)) {
                        client.postDoc([
                            title         : article.title,
                            shortId       : article.shortId,
                            url           : article.localLinkUrl,
                            byline        : article.bylines,
                            date_published: article.published,
                            source        : "associated-press",
                            section       : tag,
                            text          : article.storyHTML.replaceAll(/<.*?>/, "").replaceAll(/\s{2,}/, " ")
                        ])
                        posted++

                    } else {
                        log.trace("doc [$article.shortId] already exists in index")
                    }
                } else {
                    log.trace("empty article found [$url]")
                }
            }
            log.trace("...posted [$posted]")
            results << [(tag):posted]
        }

        log.info("results:\n$results")
        return results
    }
}