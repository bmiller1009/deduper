package org.bradfordmiller.deduper.csv

import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.utils.Right

class CsvConfigParser(config: Map<String, String>) {
    val extension = config["ext"]!!
    val delimiter = config["delimiter"]!!
    val targetName = config["targetName"]!!

    companion object {
        fun getCsvMap(context: String, jndi: String): Map<String, String> {
            val ds = JNDIUtils.getDataSource(jndi, context) as Right
            val map = ds.right as Map<String, String>
            return map
        }
    }
}