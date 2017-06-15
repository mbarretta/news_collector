# AP News Collector
Fetch top news from AP into Elasticsearch

## CLI
```
usage: APNewCollector
 -clean             drop and build the ES index
 -esIndex <index>   ES index name [default: news]
 -esPass <pass>     password for ES authentication [default: changeme]
 -esUrl <URL>       URL for ES [default: http://localhost:9200]
 -esUser <user>     username for ES authentication [default: elastic]
 -help              print this message
 ```
 
 ## AWS Lambda
 Support for AWS Lambda is available by building an uber-jar (via [shadowJar](https://github.com/johnrengelman/shadow)) and pointing towards `APNewsCollectorLambda::handleRequest`.
 
 Expected input is JSON in the form:
 ```
 {
    "url": "http://myhost.com:9200",
    "index": "ap_news",
    "user": "elastic",
    "password: "changeme"
 }
 ```
 **NOTE:** For Lambda, only `url` is required
