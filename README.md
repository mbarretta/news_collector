# AP News Collector
Fetch top news from AP and [NewsAPI](https://newsapi.org) into Elasticsearch

## Config
You have a two main options: CLI args and/or a `properties.groovy`. There are also defaults defined in `NewsCollector.groovy` ...probably should just remove those and use `properties.groovy` as the default property repo.

If you're using Lambda, you can also put values in `properties.groovy`, but they'll end-up packaged with the uberjar. Alternatively, you can pass in JSON. I use CloudWatch Events to send the necessary JSON every 5 hours.

### Authentication

If you set an Elasticsearch user using one of the various config methods, then you'll need to also send a password. If you don't configure a user, it'll figure you don't have authentication setup! And shame on your if you don't. [X-Pack](https://www.elastic.co/products/x-pack) is loaded with features (to include security) that make Elasticsearch into a product-level analytic powerhouse.

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
 
Expected input is JSON in the form:
```
 {
    "url": "http://myhost.com:9200",
    "index": "ap_news",
    "user": "elastic",
    "password: "changeme",
    "newsApiKey": "mykey"
    "clean": "false"
 }
```

**NOTE:** For Lambda, only `url` is required, though if you don't have `newsApiKey` set in `properties.groovy` and you want to fetch stuff from there, you'll need to have that passed in as well.


#### News API
If you don't want to get a NewsAPI key, no worries! Just leave that config blank and it'll be skipped

See below for more.