# News Collector
Fetch top news from AP and [NewsAPI](https://newsapi.org) into Elasticsearch

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

Support for AWS Lambda is available by building an uber-jar (via [shadowJar](https://github.com/johnrengelman/shadow)) and pointing towards `com.elastic.barretta.collectors.news.lambda.NewsCollectorLambda::handleRequest`.

...yes, that package name is obnoxious, and I probably should change it or remove the vast majority of it

 As mentioned above, you can configure it via `properties.groovy` on the classpath or by passing in JSON in the form:
```
 {
    "es" {
        "url": "http://myhost.com:9200",
        "index": "news",
        "user": "elastic",
        "pass": "changeme"
    }
    "newsApi": {
        "key": "mykey"
    }
    "clean": "false" //or true!
 }
```

#### News API
If you don't want to get a NewsAPI key, no worries! Just leave that config blank and it'll be skipped
