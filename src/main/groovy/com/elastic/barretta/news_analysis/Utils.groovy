package com.elastic.barretta.news_analysis

import groovy.util.logging.Slf4j

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

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
                    NewsCollector.DEFAULT_ENTITY_ES_INDEX
                )
            }
        }
    }

    static def zipFile(File input, ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        ZipOutputStream zipStream = new ZipOutputStream(baos)
        def zipper = { file ->
            ZipEntry entry = new ZipEntry(file.name)
            zipStream.putNextEntry(entry)
            zipStream.write(file.bytes)
            zipStream.closeEntry()
        }
        try {
            if (input.isDirectory()) {
                input.eachFile {
                    zipper(it)
                }
            } else {
                zipper(input)
            }

        } catch (IOException ioe) {
            log.error("error while zipping file [$input.name] [$ioe.cause]")
        } finally {
            zipStream.close()
            baos.close()
        }
        return baos
    }

    static def generateS3KeyName(String prefix, String name, Date date = new Date(), String extension = "json", includeUUID = true) {
        return "$prefix/$name-${date.format("yyyyMMdd")}${includeUUID ? "-" + UUID.randomUUID() : ""}.$extension"
    }
}
