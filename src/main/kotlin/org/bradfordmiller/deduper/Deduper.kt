package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.Left
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.FileSystemException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import javax.sql.DataSource

data class DedupeReport(val recordCount: Long, val columnsFound: Set<String>, val dupeCount: Long, var dupes: MutableMap<Long, Dupe>)
data class Dupe(val rowNumber: Long, val firstFoundRowNumber: Long, val dupeValues: String)

abstract class TargetPersistor(val rsmd: ResultSetMetaData) {

    abstract fun createTarget(targetName: String, columns: Set<String>)
    abstract fun createDupes()
}

class CsvTargetPersistor(rsmd: ResultSetMetaData, val config: Map<String, String>): TargetPersistor(rsmd) {

    val extension = config["ext"]
    val delimiter = config["delimiter"]!!

    override fun createTarget(targetName: String, columns: Set<String>) {

        val fileName = targetName + "." + extension

        val f = File("$fileName")

        if(f.exists() && !f.isFile)
            throw FileSystemException("tgt name $fileName is not a file")

        BufferedWriter(OutputStreamWriter(FileOutputStream(fileName), "utf-8")).use {bw ->
            bw.write(columns.joinToString{delimiter})
        }
    }

    override fun createDupes() {
        val columns = setOf("row_id", "dupe_values")
        createTarget("dupes", columns)
    }
}

class SqlTargetPersistor(rsmd: ResultSetMetaData, val conn: Connection): TargetPersistor(rsmd) {

    override fun createTarget(targetName: String) {
        val ddl = SqlUtils.generateDDL(targetName, rsmd)
        SqlUtils.executeDDL(conn, ddl.createStatement)
    }

    override fun createDupes() {

    }
}

class Deduper() {

    //abstract fun processRs(rs: ResultSet)

    private val logger = LoggerFactory.getLogger(javaClass)

    fun getTargetPersistors(jndi: String, context:String): TargetPersistor {
        val ds = JNDIUtils.getDataSource(jndi, context)

        when(ds) {
            is DataSource -> SqlTargetPersistor(ds.connection)
            is Map<*,*> -> CsvTargetPersistor(ds)
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
                    val targetPersistor = getTargetPersistors(srcJndi, context)

                    targetPersistor.createTarget(tgtName, rsmd)
                    //create dupes

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
        return DedupeReport(recordCount, rsColumns, dupeCount, dupeHashes)
    }
}