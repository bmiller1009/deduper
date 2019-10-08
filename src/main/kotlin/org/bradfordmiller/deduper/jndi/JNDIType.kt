package org.bradfordmiller.deduper.jndi

abstract class JNDITargetType(val jndi: String, val context: String, val deleteIfExists: Boolean)
class CsvJNDITargetType(jndi: String, context: String, deleteIfExists: Boolean): JNDITargetType(jndi, context, deleteIfExists)
class SqlJNDITargetType(jndi: String, context: String, deleteIfExists: Boolean, val targetTable: String, val varcharPadding: Int = 0): JNDITargetType(jndi, context, deleteIfExists)
class SqlJNDIDupeType(jndi: String, context: String, deleteIfExists: Boolean): JNDITargetType(jndi, context, deleteIfExists)
class SqlJNDIHashType(jndi: String, context: String, val includeJson: Boolean, deleteIfExists: Boolean): JNDITargetType(jndi, context, deleteIfExists)
