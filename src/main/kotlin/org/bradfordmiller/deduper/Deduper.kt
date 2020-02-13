package org.bradfordmiller.deduper

import gnu.trove.map.hash.THashMap
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
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import javax.sql.DataSource

import org.bradfordmiller.deduper.consumers.*

/**
 * reprsentation of a sample of data showing the comma-delimited [sampleString] and the associated [sampleHash] for that
 * sample string
 */
data class SampleRow(val sampleString: String, val sampleHash: String)
/**
 * summary of a dedupe operation, with total [recordCount], [hashColumns] used, [columnsFound] in the actual source
 * query, [dupeCount], [distinctDupeCount], and [dupes] found in the dedupe process.
 */
data class DedupeReport(
    val recordCount: Long,
    val hashColumns: Set<String>,
    val columnsFound: Set<String>,
    val dupeCount: Long,
    val distinctDupeCount: Long,
    val hashCount: Long,
    var dupes: MutableMap<String, Pair<MutableList<Long>, Dupe>>
) {
    override fun toString(): String {
        return "recordCount=$recordCount, " +
            "columnsFound=$columnsFound, " +
            "hashColumns=$hashColumns, " +
            "dupeCount=$dupeCount, " +
            "distinctDupeCount=$distinctDupeCount, " +
            "hashCount=$hashCount"
    }
}

class DeduperProducer(
    val dataQueue: BlockingQueue<MutableList<Map<String, Any>>>?,
    val dupeQueue: BlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>?,
    val hashQueue: BlockingQueue<MutableList<HashRow>>?,
    val controlQueues: Map<Deduper.ControlQueue, ArrayBlockingQueue<DedupeReport>>,
    val commitSize: Long = 500,
    val outputReportCommitSize: Long = 1000000,
    val config: Config,
    val persistors: Deduper.Persistors,
    val sourceDataSource: DataSource,
    val sqlStatement: String
): Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(DeduperProducer::class.java)
    }

    override fun run() {
        logger.info("Beginning the deduping process.")
        /**
         * writes data to a persistor
         *
         * @param T the type of persistor being used
         * @param writePersistor - the target persistor being leveraged for the write
         * @param data - the data being written
         */
        fun <T> writeData(data: MutableList<T>, queue: BlockingQueue<MutableList<T>>?) {
            val dataCopy = mutableListOf<T>()
            dataCopy.addAll(data)
            queue?.put(dataCopy)
            data.clear()
        }
        /**
         * writes data to a persistor
         *
         * @param T the type of persistor being used
         * @param count - the count of total rows processed so far
         * @param writePersistor - the target persistor being leveraged for the write
         * @param data - the data being written
         */
        fun <T> writeData(count: Long, data: MutableList<T>, queue: BlockingQueue<MutableList<T>>?) {
            if(count > 0 && data.size % commitSize == 0L) {
                writeData(data, queue)
            }
        }

        val seenHashes = THashMap<String, Long>()
        val hashColumns = config.sourceJndi.hashKeys
        val dupeMap: MutableMap<String, Pair<MutableList<Long>, Dupe>> = mutableMapOf()

        var distinctDupeCount = 0L
        var rsColumns = mapOf<Int, String>()
        var recordCount = 0L
        var dupeCount = 0L

        if(config.seenHashesJndi != null) {

            logger.info("Seen hashes JNDI is populated. Attempting to load hashes...")

            val hashSourceDataSource =
                (JNDIUtils.getDataSource(
                    config.seenHashesJndi.jndiName, config.seenHashesJndi.context
                ) as Left<DataSource?, String>).left!!

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

                    var data: MutableList<Map<String, Any>> = mutableListOf()
                    var hashes: MutableList<HashRow> = mutableListOf()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    require(rsColumns.values.containsAll(hashColumns)) {
                        "One or more provided keys $hashColumns not contained in resultset: $rsColumns"
                    }

                    val columns =
                        if(hashColumns.isEmpty())
                            rsColumns.values.joinToString(",")
                        else
                            hashColumns.joinToString(",")

                    logger.info("Using $columns to calculate hashes")

                    var targetIsNotNull: Boolean = false
                    if(persistors.targetPersistor != null) {
                        targetIsNotNull = true
                    }

                    var dupeIsNotNull: Boolean = false
                    if(persistors.dupePersistor != null) {
                        dupeIsNotNull = true
                    }

                    var hashIsNotNull: Boolean = false
                    var includeJson: Boolean = false
                    if(persistors.hashPersistor != null) {
                        hashIsNotNull = true
                        includeJson = (config.hashJndi as SqlJNDIHashType).includeJson
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

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)

                            if(targetIsNotNull) {
                                data.add(rsMap)
                                writeData(recordCount, data, dataQueue)
                            }
                            if(hashIsNotNull) {
                                val json =
                                    if(includeJson) {
                                        JSONObject(rsMap).toString()
                                    } else {
                                        null
                                    }
                                hashes.add(HashRow(hash, json))

                                writeData(recordCount, hashes, hashQueue)
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
                                writeData(dupeCount, dupeMap.toList().toMutableList(), dupeQueue)
                            }
                        }
                        recordCount += 1
                        if(recordCount % outputReportCommitSize == 0L)
                            logger.info("$recordCount records have been processed so far.")
                    }
                    //Flush target/dupe/hash data that's in the buffer
                    if(targetIsNotNull) {
                        writeData(data, dataQueue)
                        //Write empty list value to indicate the data stream is complete
                        writeData(mutableListOf(), dataQueue)
                    }
                    if(dupeIsNotNull) {
                        writeData(dupeMap.toList().toMutableList(), dupeQueue)
                        //Write empty list value to indicate the data stream is complete
                        writeData(mutableListOf(), dupeQueue)
                    }
                    if(hashIsNotNull) {
                        writeData(hashes, hashQueue)
                        //Write empty list value to indicate the data stream is complete
                        writeData(mutableListOf(), hashQueue)
                    }
                }
            }
        }

        val ddReport =
          DedupeReport(
              recordCount,
              hashColumns,
              rsColumns.values.toSet(),
              dupeCount,
              distinctDupeCount,
              seenHashes.size.toLong(),
              dupeMap
          )

        logger.info("Dedupe report: $ddReport")
        controlQueues.values.forEach{cq -> cq.put(ddReport)}
        logger.info("Deduping process complete.")
    }
}

/**
 * dedupes data based on [config] settings
 */
class Deduper(private val config: Config) {

    enum class ControlQueue { Producer, Target, Dupes, Hashes }

    data class Persistors(
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
                        Pair(
                                SqlTargetPersistor(
                                        sqlJndi.targetTable, sqlJndi.jndi, sqlJndi.context, sqlJndi.varcharPadding
                                ),
                                deleteTarget
                        )
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

        Persistors(
            targetPersistor.first,
            targetPersistor.second,
            dupePersistor.first,
            dupePersistor.second,
            hashPersistor.first,
            hashPersistor.second
        )
    }

    val sourceDataSource: DataSource
        by lazy {
            (JNDIUtils.getDataSource(
                    config.sourceJndi.jndiName, config.sourceJndi.context
            ) as Left<DataSource?, String>).left!!
        }

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

    /**
     * returns a sample row representing all values in that row plus the associated hash of the row
     */
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

    /**
     *  runs a dedupe process and returns a dedupe report
     *
     *  @param commitSize - the number of rows to write in a single transation
     *  @param outputReportCommitSize - the number of total rows committed before logging the result
     */
    fun dedupe(commitSize: Long = 500, outputReportCommitSize: Long = 1000000): DedupeReport {

        var threadCount = 1

        var dataQueue: BlockingQueue<MutableList<Map<String, Any>>>? = null
        var dupeQueue: BlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>? = null
        var hashQueue: BlockingQueue<MutableList<HashRow>>? = null

        var controlQueues = emptyMap<ControlQueue, ArrayBlockingQueue<DedupeReport>>()
        val controlQueue = ArrayBlockingQueue<DedupeReport>(1)
        controlQueues += ControlQueue.Producer to controlQueue

        if (config.targetJndi != null) {
            threadCount += 1
            dataQueue = ArrayBlockingQueue<MutableList<Map<String, Any>>>(100)
            controlQueues += ControlQueue.Target to ArrayBlockingQueue(1)
        }

        if (config.dupesJndi != null) {
            threadCount += 1
            dupeQueue = ArrayBlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>(100)
            controlQueues += ControlQueue.Dupes to ArrayBlockingQueue(1)
        }

        if (config.hashJndi != null) {
            threadCount += 1
            hashQueue = ArrayBlockingQueue<MutableList<HashRow>>(100)
            controlQueues += ControlQueue.Hashes to ArrayBlockingQueue(1)
        }

        val executorService = Executors.newFixedThreadPool(threadCount)

        val producer =
            DeduperProducer(
                dataQueue,
                dupeQueue,
                hashQueue,
                controlQueues,
                commitSize,
                outputReportCommitSize,
                config,
                persistors,
                sourceDataSource,
                sqlStatement
            )

        executorService.execute(producer)

        logger.info("Producer thread started")

        if (persistors.targetPersistor != null) {
            val targetConsumer =
                DeduperDataConsumer(
                    persistors.targetPersistor!!,
                    dataQueue!!,
                    controlQueues[ControlQueue.Target] ?: error(""),
                    persistors.deleteTargetIfExists,
                    sourceDataSource,
                    sqlStatement
                )
            executorService.execute(targetConsumer)
            logger.info("Data consumer thread started")
        }

        if(persistors.dupePersistor != null) {
            val dupeConsumer =
                DeduperDupeConsumer(
                    persistors.dupePersistor!!,
                    dupeQueue!!,
                    controlQueues[ControlQueue.Dupes] ?: error(""),
                    persistors.deleteDupeIfExists
                )
            executorService.execute(dupeConsumer)
            logger.info("Duplicates consumer thread started")
        }

        if(persistors.hashPersistor != null) {
            val hashConsumer =
                DeduperHashConsumer(
                    persistors.hashPersistor!!,
                    hashQueue!!,
                    controlQueues[ControlQueue.Hashes] ?: error(""),
                    persistors.deleteHashIfExists
                )
            executorService.execute(hashConsumer)
            logger.info("Hash consumer thread started")
        }

        var streamComplete = false
        lateinit var dedupeReport: DedupeReport

        while (!streamComplete) {
            dedupeReport = controlQueue.take()
            streamComplete = true
        }

        executorService.shutdown()
        try {
            if (!executorService.awaitTermination(config.executionServiceTimeout.interval, config.executionServiceTimeout.timeUnit)) {
                executorService.shutdownNow()
            }
        } catch(iex:InterruptedException) {
            executorService.shutdownNow()
            Thread.currentThread().interrupt()
            logger.error(iex.message)
            throw iex
        }

        logger.info("All consuming services are complete....shutting down.")

        return dedupeReport
    }
}