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
    val hashJndi: JNDITargetType?
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
        private var hashJndi: JNDITargetType? = null
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
        fun hashJndi(jndiTargetType: JNDITargetType) = apply {this.hashJndi = jndiTargetType}
        fun build(): Config {
            val sourceJndi = srcJndi ?: throw NullArgumentException("Source JNDI must be set")
            val sourceName = srcName ?: throw NullArgumentException("Source JNDI name must be set")
            val finalContext = context ?: throw NullArgumentException("JNDI context must be set")
            val finalTargetJndi = targetJndi
            val finalDupesJndi = dupesJndi
            val finalHashJndi = hashJndi
            val config = Config(sourceJndi, sourceName, finalContext, keyOn.orEmpty(), finalTargetJndi, finalDupesJndi, finalHashJndi)
            logger.trace("Built config object $config")
            return config
        }
    }
}