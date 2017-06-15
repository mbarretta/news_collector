/**
 * Main class
 */
class APNewsCollector {

    final static DEFAULT_ES_URL = "http://localhost:9200"
    final static DEFAULT_ES_INDEX = "news"
    final static DEFAULT_ES_USER = "elastic"
    final static DEFAULT_ES_PASS = "changeme"
    final static DEFAULT_ES_TYPE = "doc"

    final static ES_MAPPING = [
        (DEFAULT_ES_TYPE): [
            properties: [
                date_published: [type: "date", format: "yyyy-MM-dd HH:mm:ss"],
                title         : [type: "text"],
                shortId       : [type: "keyword"],
                url           : [type: "keyword"],
                byline        : [type: "text"],
                test          : [type: "text"],
                entityObjects : [type: "nested"],
                entityNames   : [type: "text"]
            ]
        ]
    ]

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "APNewCollector")
        cli.esUrl(args: 1, argName: "URL", "URL for ES [default: $DEFAULT_ES_URL]")
        cli.esIndex(args: 1, argName: "index", "ES index name [default: $DEFAULT_ES_INDEX]")
        cli.esUser(args: 1, argName: "user", "username for ES authentication [default: $DEFAULT_ES_USER]")
        cli.esPass(args: 1, argName: "pass", "password for ES authentication [default: $DEFAULT_ES_PASS]")
        cli.clean("drop and build the ES index")
        cli.help("print this message")
        def options = cli.parse(args)

        if (options.help) {
            cli.usage()
            System.exit(0)
        }

        //setup ES config & client
        def esConfig = new ESClient.Config()
        esConfig.with {
            url = options.esUrl ?: DEFAULT_ES_URL
            index = options.esIndex ?: DEFAULT_ES_INDEX
            type = DEFAULT_ES_TYPE
            user = options.esUser ?: DEFAULT_ES_USER
            pass = options.esPass ?: DEFAULT_ES_PASS
        }

        run(esConfig, options.clean)
    }

    static def run(ESClient.Config config, boolean clean = false) {
        def client = new ESClient(config)

        prepESIndex(client, clean)
        return Scraper.scrape(client)
    }

    private static def prepESIndex(client, clean) {
        if (clean) {
            client.deleteIndex()
        } else {
            client.createIndex(ES_MAPPING)
        }
    }
}