package org.bradfordmiller.deduper.config

import org.apache.commons.lang.NullArgumentException
import org.slf4j.LoggerFactory

enum class ConfigType {Sql, Csv}

class Config private constructor(
    val srcJndi: String,
    val srcName: String,
    val context: String,
    val hashColumns: Set<String>,
    val tgtJndi: String?,
    val dupesJndi: String?,
    val tgtTable: String?,
    val deleteTargetIfExists: Boolean,
    val deleteDupeIfExist: Boolean
) {

    companion object {
        private val logger = LoggerFactory.getLogger(Config::class.java)
    }

    data class ConfigBuilder(
        private var srcJndi: String? = null,
        private var srcName: String? = null,
        private var context: String? = null,
        private var keyOn: Set<String>? = null,
        private var tgtJndi: String? = null,
        private var dupesJndi: String? = null,
        private var tgtTable: String? = null,
        private var deleteTargetIfExist: Boolean = false,
        private var deleteDupeIfExist: Boolean = false
    ) {
        companion object {
            private val logger = LoggerFactory.getLogger(ConfigBuilder::class.java)
        }

        fun sourceJndi(srcJndi: String) = apply { this.srcJndi = srcJndi }
        fun sourceName(srcName: String) = apply { this.srcName = srcName }
        fun jndiContext(context: String) = apply { this.context = context }
        fun hashColumns(hashColumns: Set<String>) = apply { this.keyOn = hashColumns }
        fun targetJndi(tgtJndi: String) = apply { this.tgtJndi = tgtJndi }
        fun dupesJndi(dupesJndi: String) = apply { this.dupesJndi = dupesJndi }
        fun targetTable(targetTable: String) = apply { this.tgtTable = targetTable }
        fun deleteTargetIfExists(deleteTargetIfExist: Boolean) = apply {this.deleteTargetIfExist = deleteTargetIfExist}
        fun deleteDupeIfExists(deleteDupeIfExist: Boolean) = apply {this.deleteDupeIfExist = deleteDupeIfExist}
        fun build(): Config {
            val sourceJndi = srcJndi ?: throw NullArgumentException("Source JNDI must be set")
            val sourceName = srcName ?: throw NullArgumentException("Source JNDI name must be set")
            val finalContext = context ?: throw NullArgumentException("JNDI context must be set")
            val targetJndi = tgtJndi
            val finalDupesJndi = dupesJndi
            val finalDeleteTargetIfExists = deleteTargetIfExist
            val finalDeleteDupeIfExists = deleteDupeIfExist
            val config = Config(sourceJndi, sourceName, finalContext, keyOn.orEmpty(), targetJndi, finalDupesJndi, tgtTable, finalDeleteTargetIfExists, finalDeleteDupeIfExists)
            logger.trace("Built config object $config")
            return config
        }
    }
}