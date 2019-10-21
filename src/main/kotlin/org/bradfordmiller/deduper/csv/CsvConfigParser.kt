package org.bradfordmiller.deduper.csv

import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.utils.Right
import org.slf4j.LoggerFactory

/**
 * Parses [config] settings which define the format for outputting csv data.  Note that the default file extension for
 * a csv output is 'txt' and the default delimiter is a comma
 */
class CsvConfigParser(private val config: Map<String, String>) {

    val extension = setDefault(config, "ext", "txt")
    val delimiter = setDefault(config, "delimiter", ",")
    val targetName = config["targetName"] ?: error("Target csv file (targetName) setting is missing")

    companion object {

        val logger = LoggerFactory.getLogger(CsvConfigParser::class.java)
        /**
         * returns a configuration map of csv formatting values based on the lookup of the [jndi] in the jndi [context]
         */
        fun getCsvMap(context: String, jndi: String): Map<String, String> {
            val ds = JNDIUtils.getDataSource(jndi, context) as Right
            return ds.right as Map<String, String>
        }
        /**
         * sets default values in the csv configuration map if parameters are not provided
         */
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