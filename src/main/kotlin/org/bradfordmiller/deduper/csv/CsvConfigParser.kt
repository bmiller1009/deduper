package org.bradfordmiller.deduper.csv

import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.utils.Right
import org.slf4j.LoggerFactory

class CsvConfigParser(config: Map<String, String>) {

    val extension = setDefault(config, "ext", "txt")
    val delimiter = setDefault(config, "delimiter", ",")
    val targetName = config["targetName"] ?: error("Target csv file (targetName) setting is missing")

    companion object {

        val logger = LoggerFactory.getLogger(CsvConfigParser::class.java)

        fun getCsvMap(context: String, jndi: String): Map<String, String> {
            val ds = JNDIUtils.getDataSource(jndi, context) as Right
            return ds.right as Map<String, String>
        }

        fun setDefault(config: Map<String, String>, key: String, default: String): String {
            if(!config.containsKey(key) || config[key] == "") {
                logger.info("'$key' key is blank.  Using '$default' as extension for default")
                return default
            } else {
                return config[key]!!
            }
        }
    }
}