# News Collector
Fetch top news from AP and [NewsAPI](https://newsapi.org) into Elasticsearch. Optionally, you can enrich the data with entity and location information extracted via the [Rosette API](https://developer.rosette.com/). And, you can also archive the resulting ES-indexed data into S3.

The three main classes are:
- NewCollector: does the news collection
- EntityMomentum: does the entity momentum calculation described below
- Archiver: does the archiving

Each of the main classes has a `main()`, a Lambda `RequestHandler`, and a Dockerfile that gets built by Gradle.

The entity "momentum" metric tries to assign a score to entities each day based on their change in mentions over the past few days:
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

#### News API
If you don't want to get a NewsAPI key, no worries! Just leave that config blank and it'll be skipped

# EntityMomentum
```
usage: EntityMomentum
 -help                    print this message
 -indexPrefix <prefix>    ES index prefix [default: news]
 -pass <pass>             password for ES authentication [default: changeme]
 -propertiesFile <file>   properties file
 -url <URL>               URL for ES [default: http://localhost:9200]
 -user <user>             username for ES authentication [default: elastic]
```

# Archiver
The [Archiver](https://github.com/mbarretta/news_collector/blob/master/src/main/groovy/com/elastic/barretta/news_analysis/Archiver.groovy) is designed to build a tar.gz of data from all the indicies created by the news collector and save it to an S3 bucket. By default, it will archive all the data from the first day of "this" month until "yesterday". There is a `main()` method that will accept ES and S3 configuration as well as a start and end date
```
usage: Archiver
 -bucket <bucket>             S3 bucket name for Archiver
 -endDate <bucket>            end date for Archiver 'yyyy-MM-dd'
 -help                        print this message
 -indexPrefix <prefix>        ES index prefix [default: news]
 -outputFileName <filename>   The filename to be used when writing to s3
                              [default: all_data-<startDate yyyyMMdd>.tar.gz]
 -pass <pass>                 password for ES authentication [default: changeme]
 -prefix <prefix>             S3 file prefix for Archiver
 -startDate <bucket>          start date for Archiver in 'yyyy-MM-dd
 -url <URL>                   URL for ES [default: http://localhost:9200]
 -user <user>                 username for ES authentication [default: elastic]
```                              
