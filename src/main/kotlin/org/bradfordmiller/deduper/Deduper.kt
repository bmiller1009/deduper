package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.sql.SqlUtils

import org.bradfordmiller.deduper.utils.Left
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import javax.sql.DataSource

data class DedupeReport(val recordCount: Long, val columnsFound: Set<String>, val dupeCount: Long, var dupes: MutableMap<Long, Dupe>)
data class Dupe(val rowNumber: Long, val firstFoundRowNumber: Long, val dupeValues: String)

class Deduper(val config: Config) {

    //abstract fun processRs(rs: ResultSet)

    private val logger = LoggerFactory.getLogger(javaClass)

    fun dedupe(commitSize: Int = 500): DedupeReport {

        var recordCount = 0L
        var dupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = setOf<String>()

        //Get src connection from JNDI - Note that this is always cast to a datasource
        val dsSrc = (JNDIUtils.getDataSource(config.srcJndi, config.context) as Left<DataSource?, String>).left!!

        JNDIUtils.getConnection(dsSrc)!!.use { conn ->

            val sql =
                    if (config.srcName.startsWith("SELECT", true)) {
                        config.srcName
                    } else {
                        "SELECT * FROM ${config.srcName}"
                    }

            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->

                    val rsmd = rs.metaData
                    val colCount = rsmd.columnCount

                    config.targetPersistor!!.createTarget(rsmd)
                    config.dupePersistor!!.createDupe()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    if (!rsColumns.containsAll(config.keyOn))
                        throw IllegalArgumentException("One or more provided keys ${config.keyOn} not contained in resultset: $rsColumns")

                    val keysPopulated = config.keyOn.isNotEmpty()

                    while (rs.next()) {

                        val hashColumns =
                                if (keysPopulated) {
                                    config.keyOn.map { rs.getString(it) }.joinToString()
                                } else {
                                    (1 until colCount).toList().map { rs.getString(it) }.joinToString()
                                }

                        val hash = DigestUtils.md5Hex(hashColumns).toUpperCase()

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)
                            config.targetPersistor!!.writeRow(rs, colCount)
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