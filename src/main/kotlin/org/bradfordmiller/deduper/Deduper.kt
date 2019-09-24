package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.csv.CsvConfigParser
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
    val hashColumns: Set<String>,
    val columnsFound: Set<String>,
    val dupeCount: Long,
    val distinctDupeCount: Long,
    var dupes: MutableMap<Long, Dupe>
) {
    override fun toString(): String {
        return "recordCount=$recordCount, " +
                "columnsFound=$columnsFound, " +
                "hashColumns=$hashColumns, " +
                "dupeCount=$dupeCount, " +
                "distinctDupeCount=$distinctDupeCount"
    }
}

class Deduper(private val config: Config) {

    private val tgtPersistor: TargetPersistor? by lazy {
        if(config.tgtTable.isNullOrEmpty()) {
            if(config.tgtJndi != null) {
                val tgtConfigMap = CsvConfigParser.getCsvMap(config.context, config.tgtJndi)
                logger.trace("tgtConfigMap = $tgtConfigMap")
                CsvTargetPersistor(tgtConfigMap)
            } else {
                null
            }
        } else {
            if(config.tgtJndi != null) {
                SqlTargetPersistor(config.tgtTable, config.tgtJndi, config.context)
            } else {
                null
            }
        }
    }

    private val duplicatePersistor: DupePersistor? by lazy {
        if(config.tgtTable.isNullOrEmpty()) {
            if(config.dupesJndi != null) {
                val dupesConfigMap = CsvConfigParser.getCsvMap(config.context, config.dupesJndi)
                logger.trace("dupesConfigMap = $dupesConfigMap")
                CsvDupePersistor(dupesConfigMap)
            } else {
                null
            }
        } else {
            if(config.dupesJndi != null) {
                SqlDupePersistor(config.dupesJndi, config.context)
            } else {
                null
            }
        }
    }

    val sourceDataSource: DataSource by lazy {(JNDIUtils.getDataSource(config.srcJndi, config.context) as Left<DataSource?, String>).left!!}

    val sqlStatement by lazy {
        if (config.srcName.startsWith("SELECT", true)) {
            config.srcName
        } else {
            "SELECT * FROM ${config.srcName}"
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger(Deduper::class.java)
    }

    fun dedupe(commitSize: Long = 500, outputReportCommitSize: Long = 1000000): DedupeReport {

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

        val hashColumns = config.hashColumns

        var recordCount = 0L
        var dupeCount = 0L
        var distinctDupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = mapOf<Int, String>()

        JNDIUtils.getConnection(sourceDataSource)!!.use { conn ->

            logger.trace("The following sql statement will be run: $sqlStatement")

            conn.prepareStatement(sqlStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->

                    val rsmd = rs.metaData
                    val colCount = rsmd.columnCount

                    logger.trace("$colCount columns have been found in the result set.")

                    var dupeMap: MutableMap<String, Pair<MutableList<Long>, Dupe>> = mutableMapOf()
                    var data: MutableList<Map<String, Any>> = mutableListOf()

                    val targetPersistor = tgtPersistor
                    val dupePersistor = duplicatePersistor

                    targetPersistor?.createTarget(rsmd)

                    dupePersistor?.createDupe()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    require(rsColumns.values.containsAll(hashColumns)) {
                        "One or more provided keys $hashColumns not contained in resultset: $rsColumns"
                    }

                    val keysPopulated = hashColumns.isNotEmpty()

                    logger.info("Using ${hashColumns.joinToString(",")} to calculate hashes")

                    while (rs.next()) {

                        val md5Values =
                                if (keysPopulated) {
                                    hashColumns.map { rs.getString(it) }.joinToString()
                                } else {
                                    (1..colCount).toList().map { rs.getString(it) }.joinToString()
                                }

                        logger.trace("Using the following value(s): $md5Values to calculate unique hash.")

                        val hash = DigestUtils.md5Hex(md5Values).toUpperCase()

                        logger.trace("MD-5 hash $hash generated for MD-5 values.")

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)
                            if(targetPersistor != null) {
                                data.add(SqlUtils.getMapFromRs(rs, rsColumns))
                                writeData(recordCount, targetPersistor, data)
                            }
                        } else {
                            if(dupeMap.containsKey(hash)) {
                                dupeMap[hash]?.first?.add(recordCount)
                            } else {
                                val firstSeenRow = seenHashes[hash]!!
                                val dupeValues = SqlUtils.getMapFromRs(rs, rsColumns)
                                val dupeJson = JSONObject(dupeValues).toString()
                                val dupe = Dupe(firstSeenRow, dupeJson)
                                dupeMap[hash] = Pair(mutableListOf(recordCount), dupe)

                                distinctDupeCount += 1
                            }

                            dupeCount += 1
                            if(dupePersistor != null) {
                                writeData(dupeCount, dupePersistor, dupeMap.toList().toMutableList())
                            }
                        }
                        recordCount += 1
                        if(recordCount % outputReportCommitSize == 0L)
                            logger.info("$recordCount records have been processed so far.")
                    }
                    //Flush target/dupe data that's in the buffer
                    if(targetPersistor != null) {
                        writeData(targetPersistor, data)
                    }
                    if(dupePersistor != null) {
                        writeData(dupePersistor, dupeMap.toList().toMutableList())
                    }
                }
            }
        }
        val ddReport = DedupeReport(recordCount, hashColumns, rsColumns.values.toSet(), dupeCount, distinctDupeCount, dupeHashes)
        logger.info("Dedupe report: $ddReport")
        logger.info("Deduping process complete.")
        return ddReport
    }
}