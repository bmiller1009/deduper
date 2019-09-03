package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.persistors.*
import org.bradfordmiller.deduper.sql.SqlUtils

import org.json.JSONObject
import org.slf4j.LoggerFactory

import java.sql.ResultSet

data class DedupeReport(
    val recordCount: Long,
    val columnsFound: Set<String>,
    val dupeCount: Long,
    val distinctDupeCount: Long,
    var dupes: MutableMap<Long, Dupe>
) {
    override fun toString(): String {
        return "recordCount=$recordCount, columnsFound=$columnsFound, dupeCount=$dupeCount, distinctDupeCount=$distinctDupeCount"
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

        val targetPersistor = config.getTargetPersistor()
        val duplicatePersistor = config.getDuplicatePersistor()
        val hashColumns = config.hashColumns

        var recordCount = 0L
        var dupeCount = 0L
        var distinctDupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = mapOf<Int, String>()

        //Get src connection from JNDI - Note that this is always cast to a datasource
        val dsSrc = config.sourceDataSource

        JNDIUtils.getConnection(dsSrc)!!.use { conn ->

            val sql = config.sqlStatement

            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->

                    val rsmd = rs.metaData
                    val colCount = rsmd.columnCount
                    var dupeMap: MutableMap<String, Pair<MutableList<Long>, Dupe>> = mutableMapOf()
                    var data: MutableList<Map<String, Any>> = mutableListOf()

                    targetPersistor.createTarget(rsmd)
                    duplicatePersistor.createDupe()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    require(rsColumns.values.containsAll(hashColumns)) {
                        "One or more provided keys $hashColumns not contained in resultset: $rsColumns"
                    }

                    val keysPopulated = hashColumns.isNotEmpty()

                    logger.info("Using ${rsColumns.values.joinToString(",")} to calculate hashes")

                    while (rs.next()) {

                        val md5Values =
                                if (keysPopulated) {
                                    hashColumns.map { rs.getString(it) }.joinToString()
                                } else {
                                    (1..colCount).toList().map { rs.getString(it) }.joinToString()
                                }

                        logger.trace("Using the following value(s): $md5Values to calculate unique hash.")

                        val hash = DigestUtils.md5Hex(md5Values).toUpperCase()

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)
                            data.add(targetPersistor.prepRow(rs, rsColumns))
                            writeData(recordCount, targetPersistor, data)
                        } else {
                            if(dupeMap.containsKey(hash)) {
                                dupeMap[hash]?.first?.add(recordCount)
                            } else {
                                val firstSeenRow = seenHashes[hash]!!
                                val dupeValues = targetPersistor.prepRow(rs, rsColumns)
                                val dupeJson = JSONObject(dupeValues).toString()
                                val dupe = Dupe(firstSeenRow, dupeJson)
                                dupeMap[hash] = Pair(mutableListOf(recordCount), dupe)
                                distinctDupeCount += 1
                            }

                            dupeCount += 1
                            writeData(dupeCount, duplicatePersistor, dupeMap.toList().toMutableList())
                        }
                        recordCount += 1
                    }
                    //Flush target/dupe data that's in the buffer
                    writeData(targetPersistor, data)
                    writeData(duplicatePersistor, dupeMap.toList().toMutableList())
                }
            }
        }
        val ddReport = DedupeReport(recordCount, rsColumns.values.toSet(), dupeCount, distinctDupeCount, dupeHashes)
        logger.info("Dedupe report: $ddReport")
        logger.info("Deduping process complete.")
        return ddReport
    }
}