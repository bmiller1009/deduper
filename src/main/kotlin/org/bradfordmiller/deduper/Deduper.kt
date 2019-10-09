package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.hashing.Hasher
import org.bradfordmiller.deduper.jndi.CsvJNDITargetType
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.jndi.SqlJNDIHashType
import org.bradfordmiller.deduper.jndi.SqlJNDITargetType
import org.bradfordmiller.deduper.persistors.*
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.Left

import org.json.JSONObject
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import javax.sql.DataSource

data class SampleRow(val sampleString: String, val sampleHash: String)

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

    internal data class Persistors(
      val targetPersistor: TargetPersistor?,
      val deleteTargetIfExists: Boolean,
      val dupePersistor: DupePersistor?,
      val deleteDupeIfExists: Boolean,
      val hashPersistor: HashPersistor?,
      val deleteHashIfExists: Boolean
    )

    private val persistors: Persistors by lazy {

        val targetPersistor: Pair<TargetPersistor?, Boolean> =
                if (config.targetJndi != null) {
                    val deleteTarget = config.targetJndi.deleteIfExists
                    if (config.targetJndi is CsvJNDITargetType) {
                        val csvJndi = config.targetJndi
                        val tgtConfigMap = CsvConfigParser.getCsvMap(csvJndi.context, csvJndi.jndi)
                        logger.trace("tgtConfigMap = $tgtConfigMap")
                        Pair(CsvTargetPersistor(tgtConfigMap), deleteTarget)
                    } else {
                        val sqlJndi = config.targetJndi as SqlJNDITargetType
                        Pair(SqlTargetPersistor(sqlJndi.targetTable, sqlJndi.jndi, sqlJndi.context, sqlJndi.varcharPadding), deleteTarget)
                    }
                } else {
                    Pair(null, false)
                }

        val dupePersistor: Pair<DupePersistor?, Boolean> =
            if (config.dupesJndi != null) {
                val deleteDupe = config.dupesJndi.deleteIfExists
                if (config.dupesJndi is CsvJNDITargetType) {
                    val csvJndi = config.dupesJndi
                    val dupesConfigMap = CsvConfigParser.getCsvMap(csvJndi.context, csvJndi.jndi)
                    logger.trace("dupesConfigMap = $dupesConfigMap")
                    Pair(CsvDupePersistor(dupesConfigMap), deleteDupe)
                } else {
                    Pair(SqlDupePersistor(config.dupesJndi.jndi, config.dupesJndi.context), deleteDupe)
                }
            } else {
                Pair(null, false)
            }

        val hashPersistor: Pair<HashPersistor?, Boolean> =
            if (config.hashJndi != null) {
                val deleteHash = config.hashJndi.deleteIfExists
                if (config.hashJndi is CsvJNDITargetType) {
                    val csvJndi = config.hashJndi
                    val hashConfigMap = CsvConfigParser.getCsvMap(csvJndi.context, csvJndi.jndi)
                    logger.trace("hashConfigMap = $hashConfigMap")
                    Pair(CsvHashPersistor(hashConfigMap), deleteHash)
                } else {
                    Pair(SqlHashPersistor(config.hashJndi.jndi, config.hashJndi.context), deleteHash)
                }
            } else {
                Pair(null, false)
            }

        Persistors(targetPersistor.first, targetPersistor.second, dupePersistor.first, dupePersistor.second, hashPersistor.first, hashPersistor.second)
    }

    val sourceDataSource: DataSource by lazy {(JNDIUtils.getDataSource(config.sourceJndi.jndiName, config.sourceJndi.context) as Left<DataSource?, String>).left!!}

    val sqlStatement by lazy {
        if (config.sourceJndi.tableQuery.startsWith("SELECT", true)) {
            config.sourceJndi.tableQuery
        } else {
            "SELECT * FROM ${config.sourceJndi.tableQuery}"
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger(Deduper::class.java)
    }

    fun getSampleHash(): SampleRow {

        val hashColumns = config.sourceJndi.hashKeys

        JNDIUtils.getConnection(sourceDataSource)!!.use { conn ->
            conn.prepareStatement(sqlStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    val md5Values = SqlUtils.stringifyRow(rs, hashColumns)
                    val hash = DigestUtils.md5Hex(md5Values).toUpperCase()
                    return SampleRow(md5Values, hash)
                }
            }
        }
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

        val hashColumns = config.sourceJndi.hashKeys

        var recordCount = 0L
        var dupeCount = 0L
        var distinctDupeCount = 0L
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = mapOf<Int, String>()
        var seenHashes = mutableMapOf<String, Long>()

         if(config.seenHashesJndi != null) {

            logger.info("Seen hashes JNDI is populated. Attempting to load hashes...")

            val hashSourceDataSource = (JNDIUtils.getDataSource(config.seenHashesJndi.jndiName, config.seenHashesJndi.context) as Left<DataSource?, String>).left!!
            val sqlStatement =
                "SELECT ${config.seenHashesJndi.hashColumnName} FROM ${config.seenHashesJndi.hashTableName}"

            logger.info("Executing the following SQL against the seen hashes jndi: $sqlStatement")

            JNDIUtils.getConnection(hashSourceDataSource)!!.use { conn ->
                conn.prepareStatement(sqlStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                    .use { stmt ->
                        stmt.executeQuery().use { rs ->
                            while (rs.next()) {
                                seenHashes.put(rs.getString(1), 0)
                            }
                        }
                    }
            }
            logger.info("Seen hashes loaded. ${seenHashes.size} hashes loaded into memory.")
        }

        JNDIUtils.getConnection(sourceDataSource)!!.use { conn ->

            logger.trace("The following sql statement will be run: $sqlStatement")

            conn.prepareStatement(sqlStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->

                    val rsmd = rs.metaData
                    val colCount = rsmd.columnCount

                    logger.trace("$colCount columns have been found in the result set.")

                    var dupeMap: MutableMap<String, Pair<MutableList<Long>, Dupe>> = mutableMapOf()
                    var data: MutableList<Map<String, Any>> = mutableListOf()
                    var hashes: MutableList<HashRow> = mutableListOf()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    require(rsColumns.values.containsAll(hashColumns)) {
                        "One or more provided keys $hashColumns not contained in resultset: $rsColumns"
                    }

                    logger.info("Using ${hashColumns.joinToString(",")} to calculate hashes")

                    var targetIsNotNull: Boolean = false
                    lateinit var targetPersistor: TargetPersistor
                    if(persistors.targetPersistor != null) {
                        targetIsNotNull = true
                        targetPersistor = persistors.targetPersistor!!
                        targetPersistor.createTarget(rsmd, persistors.deleteTargetIfExists)
                    }

                    var dupeIsNotNull: Boolean = false
                    lateinit var dupePersistor: DupePersistor
                    if(persistors.dupePersistor != null) {
                        dupeIsNotNull = true
                        dupePersistor = persistors.dupePersistor!!
                        dupePersistor.createDupe(persistors.deleteDupeIfExists)
                    }

                    var hashIsNotNull: Boolean = false
                    var includeJson: Boolean = false
                    lateinit var hashPersistor: HashPersistor
                    if(persistors.hashPersistor != null) {
                        hashIsNotNull = true
                        hashPersistor = persistors.hashPersistor!!
                        includeJson = (config.hashJndi as SqlJNDIHashType).includeJson
                        hashPersistor.createHashTable(persistors.deleteHashIfExists)
                    }

                    while (rs.next()) {

                        val md5Values = SqlUtils.stringifyRow(rs, hashColumns)
                        //Hold data in map of columns/values
                        val rsMap = SqlUtils.getMapFromRs(rs, rsColumns)

                        logger.trace("Using the following value(s): $md5Values to calculate unique hash.")

                        val hash = DigestUtils.md5Hex(md5Values).toUpperCase()
                        val longHash = Hasher.hashString(hash)

                        logger.trace("MD-5 hash $hash generated for MD-5 values.")
                        logger.trace("Converted hash value to long value: $longHash")

                        //TODO - replace seenHashes with trove collection and store the long representation of the string hash
                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)

                            if(targetIsNotNull) {
                                data.add(rsMap)
                                writeData(recordCount, targetPersistor, data)
                            }
                            if(hashIsNotNull) {
                                val json =
                                    if(includeJson) {
                                        JSONObject(rsMap).toString()
                                    } else {
                                        null
                                    }
                                hashes.add(HashRow(hash, json))
                                writeData(recordCount, hashPersistor, hashes)
                            }
                        } else {
                            if(dupeMap.containsKey(hash)) {
                                dupeMap[hash]?.first?.add(recordCount)
                            } else {
                                val firstSeenRow = seenHashes[hash]!!
                                val dupeJson = JSONObject(rsMap).toString()
                                val dupe = Dupe(firstSeenRow, dupeJson)
                                dupeMap[hash] = Pair(mutableListOf(recordCount), dupe)

                                distinctDupeCount += 1
                            }

                            dupeCount += 1
                            if(dupeIsNotNull) {
                                writeData(dupeCount, dupePersistor, dupeMap.toList().toMutableList())
                            }
                        }
                        recordCount += 1
                        if(recordCount % outputReportCommitSize == 0L)
                            logger.info("$recordCount records have been processed so far.")
                    }
                    //Flush target/dupe/hash data that's in the buffer
                    if(targetIsNotNull) {
                        writeData(targetPersistor, data)
                    }
                    if(dupeIsNotNull) {
                        writeData(dupePersistor, dupeMap.toList().toMutableList())
                    }
                    if(hashIsNotNull) {
                        writeData(hashPersistor, hashes)
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