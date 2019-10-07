package org.bradfordmiller.deduper.config

import org.apache.commons.lang.NullArgumentException
import org.bradfordmiller.deduper.jndi.JNDITargetType
import org.slf4j.LoggerFactory

data class SourceJndi(val jndiName: String, val tableQuery: String)

class Config private constructor(
    val sourceJndi: SourceJndi,
    val seenHashesJndi: SourceJndi?,
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
        private var sourceJndi: SourceJndi? = null,
        private var seenHashesJndi: SourceJndi? = null,
        private var context: String? = null,
        private var keyOn: Set<String>? = null,
        private var targetJndi: JNDITargetType? = null,
        private var dupesJndi: JNDITargetType? = null,
        private var hashJndi: JNDITargetType? = null
    ) {
        companion object {
            private val logger = LoggerFactory.getLogger(ConfigBuilder::class.java)
        }

        fun sourceJndi(sourceJndi: SourceJndi) = apply {this.sourceJndi = sourceJndi}
        fun seenHashesJndi(seenHashesJndi: SourceJndi) = apply {this.seenHashesJndi = seenHashesJndi}
        fun jndiContext(context: String) = apply { this.context = context }
        fun hashColumns(hashColumns: Set<String>) = apply { this.keyOn = hashColumns }
        fun targetJndi(jndiTargetType: JNDITargetType) = apply {this.targetJndi = jndiTargetType}
        fun dupesJndi(jndiTargetType: JNDITargetType) = apply {this.dupesJndi = jndiTargetType}
        fun hashJndi(jndiTargetType: JNDITargetType) = apply {this.hashJndi = jndiTargetType}
        fun build(): Config {
            val sourceJndi = sourceJndi ?: throw NullArgumentException("Source JNDI must be set")
            val seenHashesJndi = seenHashesJndi
            val finalContext = context ?: throw NullArgumentException("JNDI context must be set")
            val finalTargetJndi = targetJndi
            val finalDupesJndi = dupesJndi
            val finalHashJndi = hashJndi
            val config =
              Config(sourceJndi, seenHashesJndi, finalContext, keyOn.orEmpty(), finalTargetJndi, finalDupesJndi, finalHashJndi)
            logger.trace("Built config object $config")
            return config
        }
    }
}