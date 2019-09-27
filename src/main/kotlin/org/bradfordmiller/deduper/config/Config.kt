package org.bradfordmiller.deduper.config

import org.apache.commons.lang.NullArgumentException
import org.bradfordmiller.deduper.jndi.JNDITargetType
import org.slf4j.LoggerFactory

class Config private constructor(
    val srcJndi: String,
    val srcName: String,
    val context: String,
    val hashColumns: Set<String>,
    val targetJndi: JNDITargetType?,
    val dupesJndi: JNDITargetType?,
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
        private var targetJndi: JNDITargetType? = null,
        private var dupesJndi: JNDITargetType? = null,
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
        fun targetJndi(jndiTargetType: JNDITargetType) = apply {this.targetJndi = jndiTargetType}
        fun dupesJndi(jndiTargetType: JNDITargetType) = apply {this.dupesJndi = jndiTargetType}
        fun deleteTargetIfExists(deleteTargetIfExist: Boolean) = apply {this.deleteTargetIfExist = deleteTargetIfExist}
        fun deleteDupeIfExists(deleteDupeIfExist: Boolean) = apply {this.deleteDupeIfExist = deleteDupeIfExist}
        fun build(): Config {
            val sourceJndi = srcJndi ?: throw NullArgumentException("Source JNDI must be set")
            val sourceName = srcName ?: throw NullArgumentException("Source JNDI name must be set")
            val finalContext = context ?: throw NullArgumentException("JNDI context must be set")
            val finalTargetJndi = targetJndi
            val finalDupesJndi = dupesJndi
            val finalDeleteTargetIfExists = deleteTargetIfExist
            val finalDeleteDupeIfExists = deleteDupeIfExist
            val config = Config(sourceJndi, sourceName, finalContext, keyOn.orEmpty(), finalTargetJndi, finalDupesJndi, finalDeleteTargetIfExists, finalDeleteDupeIfExists)
            logger.trace("Built config object $config")
            return config
        }
    }
}