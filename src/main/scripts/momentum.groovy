import com.elastic.barretta.news_analysis.ESClient
import com.elastic.barretta.news_analysis.EntityMomentum
import com.elastic.barretta.news_analysis.NewsCollector

propertyConfig = new ConfigSlurper().parse(this.class.getResource("properties.groovy"))
request = new NewsCollector.Config()

request.indexPrefix = NewsCollector.DEFAULT_ES_INDEX_PREFIX
request.es.with {
    url = propertyConfig.es.url
    user = propertyConfig.es.user
    pass = propertyConfig.es.pass
    index = request.momentum_index
}
client = new ESClient(request.es)

date = Calendar.instance
date.set(2017, 11, 18)
while (date.before(Calendar.instance)) {
    EntityMomentum.run(client, date.format("yyyy-MM-dd"))
    date.add(Calendar.DAY_OF_MONTH, 1)
}
