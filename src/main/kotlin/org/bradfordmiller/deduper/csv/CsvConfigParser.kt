package org.bradfordmiller.deduper.csv

import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.utils.Right

class CsvConfigParser(config: Map<String, String>) {
    val extension = config["ext"] ?: error("File extension (ext) setting is missing")
    val delimiter = config["delimiter"] ?: error("File delimiter (delimiter) setting is missing")
    val targetName = config["targetName"] ?: error("Target csv file (targetName) setting is missing")

    companion object {
        fun getCsvMap(context: String, jndi: String): Map<String, String> {
            val ds = JNDIUtils.getDataSource(jndi, context) as Right
            return ds.right as Map<String, String>
        }
    }
}