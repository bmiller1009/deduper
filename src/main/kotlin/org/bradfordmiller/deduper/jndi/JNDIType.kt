package org.bradfordmiller.deduper.jndi

/**
 * Defines output information for target data based on the [jndi] name and jndi [context]
 *
 * @property deleteIfExists specifies whether to delete the existing target data before spooling out new data
 */
abstract class JNDITargetType(val jndi: String, val context: String, val deleteIfExists: Boolean)
/**
 * Defines output information for csv target data based on the [jndi] name and jndi [context]
 *
 * @parameter deleteIfExists specifies whether to delete the existing target data before spooling out new data
 */
class CsvJNDITargetType(
    jndi: String,
    context: String,
    deleteIfExists: Boolean
): JNDITargetType(jndi, context, deleteIfExists)
/**
 * Defines output information for sql target data based on the [jndi] name and jndi [context]
 *
 * @parameter deleteIfExists specifies whether to delete the existing target data before spooling out new data
 * @property targetTable name of the target table to be created
 * @property varcharPadding adds additonal characters to varchar fields in instances where the varchar size may
 * be too small with the defaults.
 */
class SqlJNDITargetType(
    jndi: String, context: String,
    deleteIfExists: Boolean,
    val targetTable: String,
    val varcharPadding: Int = 0
): JNDITargetType(jndi, context, deleteIfExists)
/**
 * Defines output information for sql duplicate data based on the [jndi] name and jndi [context]
 *
 * @parameter deleteIfExists specifies whether to delete the existing target data before spooling out new data
 */
class SqlJNDIDupeType(
    jndi: String,
    context: String,
    deleteIfExists: Boolean
): JNDITargetType(jndi, context, deleteIfExists)
/**
 * Defines output information for sql hash data based on the [jndi] name and jndi [context]
 *
 * @parameter deleteIfExists specifies whether to delete the existing target data before spooling out new data
 * @property includeJson specifies whether each output hash value will include a corresponding json representation of
 * the data
 */
class SqlJNDIHashType(
    jndi: String,
    context: String,
    val includeJson: Boolean,
    deleteIfExists: Boolean
): JNDITargetType(jndi, context, deleteIfExists)
