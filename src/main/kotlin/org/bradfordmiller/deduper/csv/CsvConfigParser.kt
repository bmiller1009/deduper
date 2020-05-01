package org.bradfordmiller.deduper.csv

import io.vavr.control.Either
import org.bradfordmiller.simplejndiutils.JNDIUtils
import org.slf4j.LoggerFactory

/**
 * Parses [config] settings which define the format for outputting csv data.  Note that the default file extension for
 * a csv output is 'txt' and the default delimiter is a comma
 */
class CsvConfigParser(private val config: Map<String, String>) {

    val extension = setDefault(config, "org/bradfordmiller/deduper/ext", "txt")
    val delimiter = setDefault(config, "delimiter", ",")
    val targetName = config["targetName"] ?: error("Target csv file (targetName) setting is missing")

    companion object {

        val logger = LoggerFactory.getLogger(CsvConfigParser::class.java)
        /**
         * returns a configuration map of csv formatting values based on the lookup of the [jndi] in the jndi [context]
         */
        fun getCsvMap(context: String, jndi: String): Map<String, String> {
            val right = JNDIUtils.getDataSource(jndi, context) as Either.Right
            val ds = right.get().toMap()
            return ds
        }
        /**
         * sets default values in the csv [config] map if parameters are not provided
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