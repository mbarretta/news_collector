package com.elastic.barretta.collectors.news.scrapers

import com.elastic.barretta.collectors.news.ESClient
import de.l3s.boilerpipe.extractors.ArticleExtractor
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class NewsAPIScraper {

    static class Config {
        String key
        String url = "https://newsapi.org/v1/articles"
    }

    static def scrape(Config config, ESClient client) {
        if (!config.key) {
            log.error("no API key set for NewsAPI - skipping")
            return
        }

        def results = [:]
        [
            "al-jazeera-english",
            "ars-technica",
            "associated-press",
            "bbc-news",
            "bbc-sport",
            "bloomberg",
            "business-insider",
            "cnbc",
            "cnn",
            "financial-times",
            "google-news",
            "hacker-news",
            "newsweek",
            "new-york-magazine",
            "reuters",
            "techcrunch",
            "the-economist",
            "the-guardian-uk",
            "the-new-york-times",
            "the-times-of-india",
            "the-wall-street-journal",
            "the-washington-post",
            "time",
            "usa-today"
        ].each {
            log.info("fetching source [$it]")

            def posted = 0
            def url = new URL(config.url + "?apiKey=${config.key}&source=$it")

            try {

                //loop through each article we found...
                new JsonSlurper().parse(url).articles.each { article ->
                    def doc = [
                        title         : article.title,
                        url           : article.url,
                        byline        : article.author,
                        date_published: article.publishedAt,
                        source        : it,
                        text          : ArticleExtractor.INSTANCE.getText(article.url.toURL())
                    ]

                    //if it's new, write it
                    if (!client.docExists("url.keyword", article.url)) {
                        client.putDoc(doc)
                        posted++
                    }

                    //else, decide if we should update it or ignore it
                    else {
                        log.trace("doc [$article.url] already exists in index")
                        def existingDoc = client.getDocByUniqueField("url.keyword", article.url)

                        //if the doc has a new published date, we'll assume content was changed or added: we'll be doing an update
                        if (existingDoc._source.date_published != doc.date_published) {
                            log.trace("...updating due to newer timestamp [$doc.date_published] vs [$existingDoc._source.date_published]")
                            client.updateDoc(existingDoc._id, doc)
                        }
                    }
                }
                log.trace("...posted [$posted]")
                results << [(it): posted]
            } catch (e) {
                log.error("error fetching or posting article [${e.getCause()}]")
            }
        }
        log.info("results:\n$results")
        return results
    }
}