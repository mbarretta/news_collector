package com.elastic.barretta.news_analysis

import com.amazonaws.services.s3.model.ObjectMetadata
import com.elastic.barretta.clients.S3Client
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import groovyx.gpars.GParsExecutorsPool

import java.nio.file.Files
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

@Slf4j
class Archiver {

    static class Config {
        String startDate
        String endDate
        String outputFileName
        S3Client.Config s3 = new S3Client.Config()

//lame validation
        def isValid() {
            def valid = [startDate, endDate].inject(true) { b, k ->
                b &= (k != null && !k.isEmpty()); b
            }
            valid &= s3.isValid()
            return valid
        }

        @Override
        public String toString() {
            return "Config{" +
                "startDate=" + startDate +
                ", endDate=" + endDate +
                ", s3=" + s3 +
                '}'
        }
    }

    static NewsCollector.Config config = new NewsCollector.Config()

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "Archiver")
        cli.url(args: 1, argName: "URL", "URL for ES [default: $NewsCollector.DEFAULT_ES_URL]")
        cli.indexPrefix(args: 1, argName: "prefix", "ES index prefix [default: $NewsCollector.DEFAULT_ES_INDEX_PREFIX]")
        cli.user(args: 1, argName: "user", "username for ES authentication [default: $NewsCollector.DEFAULT_ES_USER]")
        cli.pass(args: 1, argName: "pass", "password for ES authentication [default: $NewsCollector.DEFAULT_ES_PASS]")
        cli.bucket(args: 1, argName: "bucket", "S3 bucket name for Archiver")
        cli.prefix(args: 1, argName: "prefix", "S3 file prefix for Archiver")
        cli.startDate(args: 1, argName: "bucket", "start date for Archiver in 'yyyy-MM-dd")
        cli.endDate(args: 1, argName: "bucket", "end date for Archiver 'yyyy-MM-dd'")
        cli.outputFileName(args: 1, argName: "filename", "The filename to be used when writing to s3 [default: all_data-<startDate yyyyMMdd>.tar.gz]")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        doConfig(options)
        run(config)
    }

    static def run(NewsCollector.Config config) {
        log.info("running with config [$config]")

        def dataDir = Files.createTempDirectory("archiver").toFile()
        dataDir.deleteOnExit()

        GParsExecutorsPool.withPool {
            it.execute({ doIndex(config, config.news_index, dataDir) } as Runnable)
            it.execute({ doIndex(config, config.momentum_index, dataDir) } as Runnable)
            it.execute({ doIndex(config, config.sentiment_index, dataDir) } as Runnable)
        }

        zipAndPushToS3(dataDir)
    }

    static def doIndex(final NewsCollector.Config config, final String index, final baseDir) {
        log.info("archiving index [$index] [${config.archiver.startDate} - ${config.archiver.endDate}]")
        def esClient = new ESClient(new ESClient.Config(url: config.es.url, index: index, user: config.es.user, pass: config.es.pass))

        def query = [
            constant_score: [
                filter: [
                    range: [
                        date: [
                            gte: config.archiver.startDate + " 00:00:00",
                            lte: config.archiver.endDate + " 23:59:59"
                        ]
                    ]
                ]
            ]
        ]

        def dataDir = new File("$baseDir/$index/doc")
        dataDir.mkdirs()
        log.debug("running ES query and saving results to tmp dir [$dataDir.absolutePath]")
        esClient.scrollQuery(query, 1000) {
            new File(dataDir, "id-"+it._id + ".json").withWriter { writer ->
                writer << JsonOutput.toJson(it._source)
            }
        }

        log.info("done with [$index]")
    }

    private static def zipAndPushToS3(dataDir)
    {
        if (dataDir.list().length > 0) {
            log.debug("building zip")
            def zipStream = Utils.createTarball(dataDir)
            def s3Client = new S3Client(config.archiver.s3)

            //write the zip to S3
            def key
            if (!config.archiver.outputFileName) {
                key = Utils.generateS3KeyName(
                    config.archiver.s3.prefix,
                    "all_news", new SimpleDateFormat("yyyy-MM-dd").parse(config.archiver.startDate),
                    "tar.gz",
                    false
                )
            } else {
                key = config.archiver.outputFileName
            }
            log.debug("writing zip to S3 [$config.archiver.s3.bucket/$key]")
            def zipBytes = zipStream.toByteArray()
            def metadata = new ObjectMetadata()
            metadata.setContentType("application/gzip")
            metadata.setContentLength(Integer.toUnsignedLong(zipBytes.length))
            s3Client.putObject(
                config.archiver.s3.bucket,
                key,
                new ByteArrayInputStream(zipStream.toByteArray()),
                metadata
            )
        }
    }

    private static def doConfig(cliConfig) {

        def propertyConfig = new ConfigSlurper().parse(this.classLoader.getResource("properties.groovy"))
        def esConfig = new ESClient.Config()

        //set default start and end date to pull everything from the beginning of the month until "yesterday"
        //the intent is to have this run once per day and build an archive that's current to end of "yesterday"
        def defaultStart = LocalDate.now().withDayOfMonth(1)
        def defaultEnd = LocalDate.now() .minusDays(1)

        def defaultFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        config.indexPrefix = cliConfig.indexPrefix ?: propertyConfig.indexPrefix ?: NewsCollector.DEFAULT_ES_INDEX_PREFIX

        esConfig.with {
            url = cliConfig.url ?: propertyConfig.es.url ?: NewsCollector.DEFAULT_ES_URL
            type = "doc"
            user = cliConfig.user ?: propertyConfig.es.user ?: NewsCollector.DEFAULT_ES_USER
            pass = cliConfig.pass ?: propertyConfig.es.pass ?: NewsCollector.DEFAULT_ES_PASS
            index = config.indexPrefix //needed for validation's sake...doh
        }

        config.with {
            archiver.s3.bucket = cliConfig.bucket ?: propertyConfig.s3.bucket
            archiver.s3.prefix = cliConfig.prefix ?: propertyConfig.s3.prefix
            archiver.startDate = cliConfig.startDate ?: propertyConfig.archiver.startDate ?: defaultStart.format(defaultFormater)
            archiver.endDate = cliConfig.endDate ?: propertyConfig.archiver.endDate ?: defaultEnd.format(defaultFormater)
            archiver.outputFileName = cliConfig.outputFileName ?: null
            es = esConfig
        }

        assert config.archiver.isValid(): "dude, where's my valid config?: $config.archiver"
        assert config.es.isValid(): "es config could use some work: $esConfig"
    }
}
