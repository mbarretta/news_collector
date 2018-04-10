package com.elastic.barretta.news_analysis

import groovy.io.FileType
import groovy.util.logging.Slf4j
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream

import java.util.zip.GZIPOutputStream

@Slf4j
class Utils {
    // todo: this is bad because it doesn't use the app config's sentiment_index value - might need to create a static singleton for
    // the config so it can be reached everywhere
    static def writeEntitySentimentsToOwnIndex(String id, Map doc, ESClient client) {
        def date = doc.date
        doc.entityObjects?.each {
            if (["PERSON", "ORGANIZATION", "LOCATION"].contains(it.type) && it.sentimentLabel) {
                client.postDoc(
                    [
                        date      : date,
                        name      : it.name,
                        type      : it.type,
                        sentiment : it.sentimentLabel,
                        confidence: it.sentimentConfidence,
                        value     : it.sentimentLabel == "pos" ? 1 : it.sentimentLabel == "neg" ? -1 : 0,
                        articleId : id,
                        source    : it.source
                    ],
                    "news_entity_sentiment"
                )
            }
        }
    }

    static def createTarball(File input, ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        TarArchiveOutputStream out = null
        def baseDir = input.absolutePath
        def addIt = { File file ->
            def entry = new TarArchiveEntry(file.path - baseDir)
            entry.size = file.bytes.length

            out.putArchiveEntry(entry)
            out.write(file.bytes)
            out.closeArchiveEntry()
        }
        try {
            out = new TarArchiveOutputStream( new GZIPOutputStream( new BufferedOutputStream(baos)))
            if (input.isDirectory()) {
                input.eachFileRecurse(FileType.FILES) {
                    addIt(it)
                }
            } else {
                addIt(input)
            }
        } catch (IOException ioe) {
            log.error("error while tar-ing file [$input.name] [$ioe.cause]")
        } finally {
            out.close()
            baos.close()
        }
        return baos
    }

    static def generateS3KeyName(String prefix, String name, Date date = new Date(), String extension = "json", includeUUID = true) {
        return "$prefix/$name-${date.format("yyyyMMdd")}${includeUUID ? "-" + UUID.randomUUID() : ""}.$extension"
    }
}
