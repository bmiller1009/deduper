package org.bradfordmiller.deduper.jndi

abstract class JNDITargetType(val jndi: String, val deleteIfExists: Boolean)
class CsvJNDITargetType(jndi: String, deleteIfExists: Boolean): JNDITargetType(jndi, deleteIfExists)
class SqlJNDITargetType(jndi: String, deleteIfExists: Boolean, val targetTable: String, val varcharPadding: Int = 0): JNDITargetType(jndi, deleteIfExists)
class SqlJNDIDupeType(jndi: String, deleteIfExists: Boolean): JNDITargetType(jndi, deleteIfExists)
class SqlJNDIHashType(jndi: String, val includeJson: Boolean, deleteIfExists: Boolean): JNDITargetType(jndi, deleteIfExists)
