package org.bradfordmiller.deduper.config

import org.apache.commons.lang.NullArgumentException
import org.bradfordmiller.deduper.jndi.JNDITargetType
import org.slf4j.LoggerFactory

data class SourceJndi(val jndiName: String, val context: String, val tableQuery: String, val hashKeys: MutableSet<String> = mutableSetOf())
data class HashSourceJndi(val jndiName: String, val context: String, val hashTableName: String, val hashColumnName: String)

class Config private constructor(
    val sourceJndi: SourceJndi,
    val seenHashesJndi: HashSourceJndi?,
    val targetJndi: JNDITargetType?,
    val dupesJndi: JNDITargetType?,
    val hashJndi: JNDITargetType?
) {
    data class ConfigBuilder(
        private var sourceJndi: SourceJndi? = null,
        private var seenHashesJndi: HashSourceJndi? = null,
        private var targetJndi: JNDITargetType? = null,
        private var dupesJndi: JNDITargetType? = null,
        private var hashJndi: JNDITargetType? = null
    ) {
        companion object {
            private val logger = LoggerFactory.getLogger(ConfigBuilder::class.java)
        }

        fun sourceJndi(sourceJndi: SourceJndi) = apply {this.sourceJndi = sourceJndi}
        fun seenHashesJndi(seenHashesJndi: HashSourceJndi) = apply {this.seenHashesJndi = seenHashesJndi}
        fun targetJndi(jndiTargetType: JNDITargetType) = apply {this.targetJndi = jndiTargetType}
        fun dupesJndi(jndiTargetType: JNDITargetType) = apply {this.dupesJndi = jndiTargetType}
        fun hashJndi(jndiTargetType: JNDITargetType) = apply {this.hashJndi = jndiTargetType}
        fun build(): Config {
            val sourceJndi = sourceJndi ?: throw NullArgumentException("Source JNDI must be set")
            val seenHashesJndi = seenHashesJndi
            val finalTargetJndi = targetJndi
            val finalDupesJndi = dupesJndi
            val finalHashJndi = hashJndi
            val config =
              Config(sourceJndi, seenHashesJndi, finalTargetJndi, finalDupesJndi, finalHashJndi)
            logger.trace("Built config object $config")
            return config
        }
    }
}