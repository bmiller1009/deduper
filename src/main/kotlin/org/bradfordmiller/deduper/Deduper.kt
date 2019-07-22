package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.Either
import org.bradfordmiller.deduper.utils.FileUtils
import org.bradfordmiller.deduper.utils.Left
import org.bradfordmiller.deduper.utils.Right
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.FileSystemException
import java.rmi.activation.UnknownObjectException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import javax.sql.DataSource

data class DedupeReport(val recordCount: Long, val columnsFound: Set<String>, val dupeCount: Long, var dupes: MutableMap<Long, Dupe>)
data class Dupe(val rowNumber: Long, val firstFoundRowNumber: Long, val dupeValues: String)

class CsvConfigParser(config: Map<String, String>) {
    val extension = config["ext"]!!
    val delimiter = config["delimiter"]!!
    val targetName = config["targetname"]!!
}

interface TargetPersistor {
    fun createTarget(rsmd: ResultSetMetaData)
}

interface DupePersistor {
    fun createDupe()
}

abstract class CsvPersistor(config: Map<String, String>) {
    val ccp = CsvConfigParser(config)
}

class CsvTargetPersistor(config: Map<String, String>): CsvPersistor(config), TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val columns = SqlUtils.getColumnsFromRs(rsmd)
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
}

class CsvDupePersistor(config: Map<String, String>): CsvPersistor(config), DupePersistor {
    override fun createDupe() {
        val columns = setOf("row_id, dupe_values")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
}

class SqlTargetPersistor(val targetName: String, val conn: Connection): TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val ddl = SqlUtils.generateDDL(targetName, rsmd)
        SqlUtils.executeDDL(conn, ddl.createStatement)
    }
}

class SqlDupePersistor(val conn: Connection): DupePersistor {
    override fun createDupe() {
        val sql = "CREATE TABLE dupes(row_id BIGINT NOT NULL, dupe_values VARCHAR(MAX) NOT NULL)"
        SqlUtils.executeDDL(conn, sql)
    }
}

class Deduper() {

    //abstract fun processRs(rs: ResultSet)

    private val logger = LoggerFactory.getLogger(javaClass)

    private fun getTargetPersistors(jndi: String, context:String, targetName: String): TargetPersistor {
        val ds = JNDIUtils.getDataSource(jndi, context)

        return when(ds) {
            is Left -> SqlTargetPersistor(targetName, ds.left!!.connection)
            is Right -> CsvTargetPersistor(ds.right as Map<String, String>)
        }
    }

    fun dedupe(srcJndi: String, srcName: String, context: String, tgtJndi: String, tgtName: String, dupesJndi: String): DedupeReport {
        return dedupe(srcJndi, srcName, context, tgtJndi, tgtName, dupesJndi, setOf())
    }

    fun dedupe(srcJndi: String, srcName: String, context: String, tgtJndi: String, tgtName: String, dupesJndi: String, keyOn: Set<String>): DedupeReport {

        var recordCount = 0L
        var dupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = setOf<String>()

        //Get src connection from JNDI - Note that this is always cast to a datasource
        val dsSrc = (JNDIUtils.getDataSource(srcJndi, context) as Left<DataSource?, String>).left!!

        JNDIUtils.getConnection(dsSrc)!!.use { conn ->

            val sql =
                    if (srcName.startsWith("SELECT", true)) {
                        srcName
                    } else {
                        "SELECT * FROM $srcName"
                    }

            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->

                    val rsmd = rs.metaData
                    val colCount = rsmd.columnCount
                    val targetPersistor = getTargetPersistors(tgtJndi, context, tgtName)
                    val dupesPersistor = getTargetPersistors(dupesJndi, context, tgtName)

                    targetPersistor.createTarget(rsmd)
                    //dupesPersistor.createDupe()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    if (!rsColumns.containsAll(keyOn))
                        throw IllegalArgumentException("One or more provided keys $keyOn not contained in resultset: $rsColumns")

                    val keysPopulated = keyOn.isNotEmpty()

                    while (rs.next()) {

                        val hashColumns =
                                if (keysPopulated) {
                                    keyOn.map { rs.getString(it) }.joinToString()
                                } else {
                                    (1 until colCount).toList().map { rs.getString(it) }.joinToString()
                                }

                        val hash = DigestUtils.md5Hex(hashColumns).toUpperCase()

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)
                        } else {
                            val firstSeenRow = seenHashes.get(hash)!!
                            val dupe = Dupe(recordCount, firstSeenRow, hashColumns)
                            dupeHashes.put(recordCount, dupe)
                            dupeCount += 1
                        }
                        recordCount += 1
                    }
                }
            }
        }
        val ddReport = DedupeReport(recordCount, rsColumns, dupeCount, dupeHashes)
        logger.info("Dedupe report: $ddReport")
        return ddReport
    }
}