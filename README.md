# News Collector
Fetch top news from AP and [NewsAPI](https://newsapi.org) into Elasticsearch. Optionally, you can enrich the data with entity and location information extracted via the [Rosette API](https://developer.rosette.com/)

There is also code to create an entity "momentum" metric which tries to assign a score to entities everyday based on their change in mentions over the past few days:
```
log(avg(M_t)) * min( S / 5, 2) * ( (M_1 / M_0) + (M_2 / M_1) )

where:
- S = number of sources mentioning the entity during the period
- M = mentions
- M_t = total mentions during period
- M_x = mention for a given day in the period
```

The intuition is to look at the change in mentions from the start of the period to the end and boost the score in a diminishing way for entities with a lot of mentions during that period, and also boost if those mentions occured across a lot of different sources (10 sources delivers the max 2x boost)

## Config
You have a two main options: CLI args and/or a `properties.groovy` (create your own via the supplied example in `src/main/resources`). There are also defaults defined in `NewsCollector.groovy` ...probably should just remove those and use `properties.groovy` as the default property repo.

If you're using AWS Lambda, you can also use values in `properties.groovy`, but they'll end-up packaged with the uberjar. Alternatively, you can pass in JSON (see details below).

### Authentication

If you set an Elasticsearch user using one of the various config methods, then you'll need to also send a password. If you don't configure a user, it'll figure you don't have authentication setup! And shame on you if you don't. [X-Pack](https://www.elastic.co/products/x-pack) is loaded with features (to include security) that make Elasticsearch into a production-level analytic powerhouse.

### CLI
```
usage: APNewCollector
 -clean                 drop and build the ES index
 -esIndex <index>       ES index name [default: news]
 -esPass <pass>         password for ES authentication [default: changeme]
 -esUrl <URL>           URL for ES [default: http://localhost:9200]
 -esUser <user>         username for ES authentication [default: elastic]
 -help                  print this message
 -newsApiKey <apiKey>   key for newsapi.org
```

### AWS Lambda

Support for AWS Lambda is available by building an uber-jar (via [shadowJar](https://github.com/johnrengelman/shadow)) and pointing towards `com.elastic.barretta.news_analysis.lambda.NewsCollectorLambda::handleRequest`.

The entity momentum code can be triggered via Lambda from `com.elastic.barretta.news_analysis.lambda.EntityMomentumLambda::handleRequest`.

 As mentioned above, you can configure it via `properties.groovy` on the classpath or by passing in JSON in the form:
```
 {
    "es" {
        "url": "http://myhost.com:9200",
        "index": "news",
        "user": "elastic",
        "pass": "changeme"
    },
    "newsApi": {
        "key": "mykey"
    },
    "enrichment": {
        "rosetteApi": {
            "url": "http://localhost:8181/rest/v1",
            "key": "mykey"
        }
    }
    "clean": "false" //or true!
 }
```

#### News API
If you don't want to get a NewsAPI key, no worries! Just leave that config blank and it'll be skipped
