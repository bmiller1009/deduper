package org.bradfordmiller.deduper.config

import org.apache.commons.lang.NullArgumentException
import org.bradfordmiller.deduper.jndi.JNDITargetType
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * A source jndi entity
 *
 * @property jndiName the jndi name defined in the simple-jndi properties file
 * @property context the context name for the jndi name, which basically maps to a properties file of the same name
 * IE if context = "test" then there should be a corresponding test.properties file present in the org.osjava.sj.root
 * defined directory in jndi.properties.  In the above example, if the context = "test" and org.osjava.sj.root =
 * src/main/resources/jndi then the jndi name will be searched for in src/main/resources/jndi/test.properties
 * @property tableQuery can either be a table (which 'SELECT *' will be run against) or a specific 'SELECT' SQL query
 * @property hashKeys a list of column names which will be used to hash the values returned by [tableQuery]
 */
data class SourceJndi(
    val jndiName: String,
    val context: String,
    val tableQuery: String,
    val hashKeys: MutableSet<String> = mutableSetOf()
)

/**
 * A hash source jndi entity. This is used when configuring a specific set of existing hashes to "dedupe" against
 *
 * @property jndiName the jndi name defined in the simple-jndi properties file
 * @property context the context name for the jndi name, which basically maps to a properties file of the same name
 * IE if context = "test" then there should be a corresponding test.properties file present in the org.osjava.sj.root
 * defined directory in jndi.properties.  In the above example, if the context = "test" and org.osjava.sj.root =
 * src/main/resources/jndi then the jndi name will be searched for in src/main/resources/jndi/test.properties
 * @property hashTableName the name of the table found in the [jndiName] which contains the existing hashes
 * @property hashColumnName the name of the column in [hashTableName] which contains the hashes
 */
data class HashSourceJndi(
    val jndiName: String,
    val context: String,
    val hashTableName: String,
    val hashColumnName: String
)

/**
 * Settings for the Execution Service timeout. Once a deduper has published all data to the blocking queues of all
 * consumers, consumers will have a dynamic timeout set (defaults to 60 seconds) for all consumers to finish persisting
 * data
 *
 * @property interval the numeric value of time (IE, 60)
 * @property timeUnit the unit of time represented by the [interval]
 */
data class ExecutionServiceTimeout(
    val interval: Long,
    val timeUnit: TimeUnit
)

/**
 * Configuration for a deduper process
 *
 * @constructor creates a new instance of the Config class
 * @property sourceJndi a [SourceJndi] object
 * @property executionServiceTimeout a [ExecutionServiceTimeout] object
 * @property seenHashesJndi a [HashSourceJndi] object
 * @property targetJndi a nullable [JNDITargetType] for writing out deduped data
 * @property dupesJndi a nullable [JNDITargetType] for writing out duplicate data
 * @property hashJndi a nullable [JNDITargetType] for writing out hashes from a deduper process
 */
class Config private constructor(
    val sourceJndi: SourceJndi,
    val executionServiceTimeout: ExecutionServiceTimeout,
    val seenHashesJndi: HashSourceJndi?,
    val targetJndi: JNDITargetType?,
    val dupesJndi: JNDITargetType?,
    val hashJndi: JNDITargetType?
) {
    /**
     * Builder object for the [Config] class
     *
     * @property sourceJndi a [SourceJndi] object
     * @property seenHashesJndi a [HashSourceJndi] object
     * @property targetJndi a nullable [JNDITargetType] for writing out deduped data
     * @property dupesJndi a nullable [JNDITargetType] for writing out duplicate data
     * @property hashJndi a nullable [JNDITargetType] for writing out hashes from a deduper process
     * @property executionServiceTimeout a [ExecutionServiceTimeout] object which defaults to 60 seconds
     */
    data class ConfigBuilder(
        private var sourceJndi: SourceJndi? = null,
        private var seenHashesJndi: HashSourceJndi? = null,
        private var targetJndi: JNDITargetType? = null,
        private var dupesJndi: JNDITargetType? = null,
        private var hashJndi: JNDITargetType? = null,
        private var executionServiceTimeout: ExecutionServiceTimeout = ExecutionServiceTimeout(60, TimeUnit.SECONDS)
    ) {
        companion object {
            private val logger = LoggerFactory.getLogger(ConfigBuilder::class.java)
        }
        /**
         * sets the [sourceJndi] for the builder object
         */
        fun sourceJndi(sourceJndi: SourceJndi) = apply {this.sourceJndi = sourceJndi}
        /**
         * sets the [seenHashesJndi] for the builder object
         */
        fun seenHashesJndi(seenHashesJndi: HashSourceJndi) = apply {this.seenHashesJndi = seenHashesJndi}
        /**
         * sets the [targetJndi] for the builder object
         */
        fun targetJndi(jndiTargetType: JNDITargetType) = apply {this.targetJndi = jndiTargetType}
        /**
         * sets the [dupesJndi] for the builder object
         */
        fun dupesJndi(jndiTargetType: JNDITargetType) = apply {this.dupesJndi = jndiTargetType}
        /**
         * sets the [hashJndi] for the builder object
         */
        fun hashJndi(jndiTargetType: JNDITargetType) = apply {this.hashJndi = jndiTargetType}
        /**
         * Sets the [executionServiceTimeout] for the builder object
         */
        fun executionServiceTimeout(executionServiceTimeout: ExecutionServiceTimeout) = apply {this.executionServiceTimeout = executionServiceTimeout}
        /**
         * returns [Config] object with builder options set
         */
        fun build(): Config {
            val sourceJndi = sourceJndi ?: throw NullArgumentException("Source JNDI must be set")
            val executionServiceTimeout = executionServiceTimeout
            val seenHashesJndi = seenHashesJndi
            val finalTargetJndi = targetJndi
            val finalDupesJndi = dupesJndi
            val finalHashJndi = hashJndi
            val config =
              Config(sourceJndi, executionServiceTimeout, seenHashesJndi, finalTargetJndi, finalDupesJndi, finalHashJndi)
            logger.trace("Built config object $config")
            return config
        }
    }
}