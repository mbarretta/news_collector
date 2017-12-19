import com.elastic.barretta.news_analysis.Archiver

import java.text.SimpleDateFormat

startDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-06-01 00:00:00")
eendDate = new Date()
archiver = new Archiver()
archiver.config.with {
    es.with {
        index = "news"
        url = "https://07b49e0aa73d320250c94080361f76d5.us-east-1.aws.found.io:9243"
        user = "elastic"
        pass = "a08KbLCCWhr0J9IBh97ULfKX"
    }
    s3.with {
        bucket = "sa.temp"
        prefix = "demos/news/data"
    }
    endDate = eendDate.format("yyyy-MM-dd HH:mm:ss")
}

while (startDate.before(eendDate)) {
    archiver.config.startDate = startDate.format("yyyy-MM-dd HH:mm:ss")
    archiver.config.endDate = (startDate + 1).format("yyyy-MM-dd HH:mm:ss")
    archiver.run()
    startDate += 1
}