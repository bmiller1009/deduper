package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang.mutable.Mutable
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.persistors.*
import org.bradfordmiller.deduper.sql.SqlUtils

import org.bradfordmiller.deduper.utils.Left
import org.json.JSONObject
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import javax.sql.DataSource

data class DedupeReport(
    val recordCount: Long,
    val columnsFound: Set<String>,
    val dupeCount: Long,
    var dupes: MutableMap<Long, Dupe>
) {
    override fun toString(): String {
        return "recordCount=$recordCount, columnsFound=$columnsFound, dupeCount=$dupeCount"
    }
}

class Deduper(private val config: Config) {

    companion object {
        val logger = LoggerFactory.getLogger(Deduper::class.java)
    }
    fun dedupe(commitSize: Long = 500): DedupeReport {

        logger.info("Beginning the deduping process.")

        fun <T> writeData(writePersistor: WritePersistor<T>, data: MutableList<T>) {
            writePersistor.writeRows(data)
            data.clear()
        }
        fun <T> writeData(count: Long, writePersistor: WritePersistor<T>, data: MutableList<T>) {
            if(count > 0 && data.size % commitSize == 0L) {
                writeData(writePersistor, data)
            }
        }

        var recordCount = 0L
        var dupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = mapOf<Int, String>()

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
                    var dupeMap: MutableMap<String, Pair<MutableList<Long>, Dupe>> = mutableMapOf()
                    var data: MutableList<Map<String, Any>> = mutableListOf()

                    config.targetPersistor.createTarget(rsmd)
                    config.dupePersistor.createDupe()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    require(rsColumns.values.containsAll(config.keyOn)) {
                        "One or more provided keys ${config.keyOn} not contained in resultset: $rsColumns"
                    }

                    val keysPopulated = config.keyOn.isNotEmpty()

                    logger.info("Using ${rsColumns.values.joinToString(",")} to calculate hashes")

                    while (rs.next()) {

                        val hashColumns =
                                if (keysPopulated) {
                                    config.keyOn.map { rs.getString(it) }.joinToString()
                                } else {
                                    (1..colCount).toList().map { rs.getString(it) }.joinToString()
                                }

                        logger.trace("Using the following value(s): $hashColumns to calculate unique hash.")

                        val hash = DigestUtils.md5Hex(hashColumns).toUpperCase()

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)
                            data.add(config.targetPersistor.prepRow(rs, rsColumns))
                            writeData(recordCount, config.targetPersistor, data)
                        } else {
                            if(dupeMap.containsKey(hash)) {
                                dupeMap[hash]?.first?.add(recordCount)
                            } else {
                                val firstSeenRow = seenHashes[hash]!!
                                val dupeValues = config.targetPersistor.prepRow(rs, rsColumns)
                                val dupeJson = JSONObject(dupeValues).toString()
                                val dupe = Dupe(firstSeenRow, dupeJson)
                                dupeMap[hash] = Pair(mutableListOf(recordCount), dupe)
                            }

                            dupeCount += 1
                            writeData(dupeCount, config.dupePersistor, dupeMap.toList().toMutableList())
                        }
                        recordCount += 1
                    }
                    //Flush target/dupe data that's in the buffer
                    writeData(config.targetPersistor, data)
                    writeData(config.dupePersistor, dupeMap.toList().toMutableList())
                }
            }
        }
        val ddReport = DedupeReport(recordCount, rsColumns.values.toSet(), dupeCount, dupeHashes)
        logger.info("Dedupe report: $ddReport")
        logger.info("Deduping process complete.")
        return ddReport
    }
}