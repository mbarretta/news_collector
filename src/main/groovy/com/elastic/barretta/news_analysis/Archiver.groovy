package com.elastic.barretta.news_analysis

import com.amazonaws.services.s3.model.ObjectMetadata
import com.elastic.barretta.clients.S3Client
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j

import java.nio.file.Files
import java.text.SimpleDateFormat

@Slf4j
class Archiver {

    static class Config {
        String startDate
        String endDate

//lame validation
        def isValid() {
            def valid = [startDate, endDate].inject(true) { b, k ->
                b &= (k != null && !k.isEmpty()); b
            }
            return valid
        }

        @Override
        public String toString() {
            return "Config{" +
                "startDate=" + startDate +
                ", endDate=" + endDate +
                '}'
        }
    }

    static NewsCollector.Config config = new NewsCollector.Config()

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "Archiver")
        cli.url(args: 1, argName: "URL", "URL for ES [default: $NewsCollector.DEFAULT_ES_URL]")
        cli.index(args: 1, argName: "index", "ES index name [default: $NewsCollector.DEFAULT_ES_INDEX]")
        cli.user(args: 1, argName: "user", "username for ES authentication [default: $NewsCollector.DEFAULT_ES_USER]")
        cli.pass(args: 1, argName: "pass", "password for ES authentication [default: $NewsCollector.DEFAULT_ES_PASS]")
        cli.bucket(args: 1, argName: "bucket", "S3 bucket name for Archiver")
        cli.prefix(args: 1, argName: "prefix", "S3 file prefix for Archiver")
        cli.startDate(args: 1, argName: "bucket", "S3 file start date for Archiver")
        cli.endDate(args: 1, argName: "bucket", "S3 file end date for Archiver")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        doConfig(options)
        run()
    }

    static def run() {
        log.info("running with config [$config]")

        doIndex(config.news_index)
        doIndex(config.momentum_index)
        doIndex(config.sentiment_index)
    }

    static def doIndex(index) {
        log.info("archiving index [$index] [${config.archiver.startDate} - ${config.archiver.endDate}]")
        config.es.index = index
        def esClient = new ESClient(config.es)

        def query = [
            constant_score: [
                filter: [
                    range: [
                        date: [
                            lte: config.archiver.endDate,
                            gte: config.archiver.startDate
                        ]
                    ]
                ]
            ]
        ]

        def tmpDir = Files.createTempDirectory("archiver").toFile()
        log.debug("running ES query and saving results to tmp dir [$tmpDir.absoluteFile]")
        esClient.scrollQuery(query, 1000) {
            new File(tmpDir, it._id).withWriter { writer ->
                writer << JsonOutput.toJson(it._source)
            }
        }

        if (tmpDir.list().length > 0) {
            log.debug("building zip")
            def zipStream = Utils.zipFile(tmpDir)
            tmpDir.delete()

            def s3Client = new S3Client(config.s3)

            //haaaack
            def fileNameDate = config.archiver.startDate
            if (config.archiver.startDate.contains("now") || config.archiver.endDate.contains("now")) {
                fileNameDate = new Date().format("yyyy-MM-dd HH:mm:ss")
            }

            //write the zip to S3
            def key = Utils.generateS3KeyName(
                config.s3.prefix,
                config.es.index, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(fileNameDate),
                "zip",
                false
            )
            log.debug("writing zip to S3 [$config.s3.bucket/$key]")
            def zipBytes = zipStream.toByteArray()
            def metadata = new ObjectMetadata()
            metadata.setContentType("application/zip")
            metadata.setContentLength(Integer.toUnsignedLong(zipBytes.length))
            s3Client.putObject(
                config.s3.bucket,
                key,
                new ByteArrayInputStream(zipStream.toByteArray()),
                metadata
            )
        }

        log.info("done")
    }

    //break down date range into series of days
    static def doPastDays(startDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-06-01 00:00:00"),
                          endDate = new Date())
    {
        while (startDate.before(endDate)) {
            log.info("archiving [$startDate]")
            config.archiver.startDate = startDate.format("yyyy-MM-dd HH:mm:ss")
            config.archiver.endDate = (startDate + 1).format("yyyy-MM-dd HH:mm:ss")
            run()
            startDate += 1
        }
    }

    private static def doConfig(cliConfig) {

        def propertyConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))
        def esConfig = new ESClient.Config()

        esConfig.with {
            url = cliConfig.url ?: propertyConfig.es.url ?: NewsCollector.DEFAULT_ES_URL
            index = cliConfig.index ?: propertyConfig.es.index ?: NewsCollector.DEFAULT_ES_INDEX
            type = "doc"
            user = cliConfig.user ?: propertyConfig.es.user ?: NewsCollector.DEFAULT_ES_USER
            pass = cliConfig.pass ?: propertyConfig.es.pass ?: NewsCollector.DEFAULT_ES_PASS
        }

        config.with {
            s3.bucket = cliConfig.bucket ?: propertyConfig.s3.bucket
            s3.prefix = cliConfig.prefix ?: propertyConfig.s3.prefix
            archiver.startDate = cliConfig.startDate ?: propertyConfig.archiver.startDate ?: "now-1d/M"
            archiver.endDate = cliConfig.endDate ?: propertyConfig.archiver.endDate ?: "now-1d/M"
            es = esConfig
        }

        assert config.isValid(): "dude, where's my valid config?"
    }
}
