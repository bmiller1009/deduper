package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.sql.SqlUtils
import org.relique.jdbc.csv.CsvDriver
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.FileSystemException
import java.sql.ResultSet
import java.sql.ResultSetMetaData

data class DedupeReport(val recordCount: Long, val columnsFound: Set<String>, val dupeCount: Long, var dupes: MutableMap<Long, Dupe>)
data class Dupe(val rowNumber: Long, val firstFoundRowNumber: Long, val dupeValues: String)

abstract class BaseDeduper {

    abstract fun processRs(rs: ResultSet)
    abstract fun createTarget(tgtName: String, rsmd: ResultSetMetaData)
    abstract fun createDupes(dupesName: String)

    private val logger = LoggerFactory.getLogger(javaClass)

    fun dedupe(srcJndi: String, srcName: String, context: String, tgtJndi: String, tgtName: String, dupesName: String): DedupeReport {
        return dedupe(srcJndi, srcName, context, tgtJndi, tgtName, dupesName, setOf())
    }

    fun dedupe(srcJndi: String, srcName: String, context: String, tgtJndi: String, tgtName: String, dupesName: String, keyOn: Set<String>): DedupeReport {

        var recordCount = 0L
        var dupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = setOf<String>()

        //Get src connection from JNDI
        val dssrc = JNDIUtils.getDataSource(srcJndi, context)!!
        val dstgt = JNDIUtils.getDataSource(tgtJndi, context)!!

        JNDIUtils.getConnection(dssrc)!!.use { conn ->

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

                    createTarget(tgtName, rsmd)
                    //createDupes(dupesName)

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

class CsvDeduper(tgtDelimiter: String = ",", tgtExt:  String = "txt", dupesDelimiter: String = ",", dupesExt: String = "txt"): BaseDeduper() {
    override fun processRs(rs: ResultSet) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createDupes(dupesName: String) {



        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createTarget(tgtName: String, rsmd: ResultSetMetaData) {

        val f = File(tgtName)

        if(f.exists() && !f.isFile)
            throw FileSystemException("tgt name $tgtName is not a file")

        val columns = SqlUtils.getColumnsFromRs(rsmd).joinToString(",")

        BufferedWriter(OutputStreamWriter(FileOutputStream(tgtName), "utf-8")).use {bw ->
            bw.write(columns)
        }
    }
}