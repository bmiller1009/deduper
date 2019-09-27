package org.bradfordmiller.deduper.jndi

abstract class JNDITargetType(val jndi: String)
class CsvJNDITargetType(jndi: String): JNDITargetType(jndi)
class SqlJNDITargetType(jndi: String, val targetTable: String, val varcharPadding: Int = 0): JNDITargetType(jndi)
class SqlJNDIDupeType(jndi: String): JNDITargetType(jndi)
