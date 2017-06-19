package com.elastic.barretta.collectors.news.scrapers

import com.elastic.barretta.collectors.news.ESClient
import de.l3s.boilerpipe.extractors.ArticleExtractor
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class NewsAPIScraper {

    static def scrape(ConfigObject config, ESClient client) {
        final def URL = "https://newsapi.org/v1/articles"

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
            def url = new URL(URL + "?sortyBy=latest&apiKey=${config.newsApi.apiKey as String}&source=$it")

            new JsonSlurper().parse(url).articles.each { article ->
                if (!client.docExists("url.keyword", article.url)) {
                    try {
                        client.postDoc([
                            title         : article.title,
                            url           : article.url,
                            byline        : article.author,
                            date_published: article.publishedAt,
                            source        : it,
                            text          : ArticleExtractor.INSTANCE.getText(article.url.toURL())
                        ])
                        posted++
                    } catch (e) {
                        log.error("error fetching or posting article [${e.getCause()}]")
                    }
                } else {
                    log.trace("doc [$article.url] already exists in index")
                }
            }
            log.trace("...posted [$posted]")
            results << [(it): posted]
        }
        log.info("results:\n$results")
        return results
    }
}