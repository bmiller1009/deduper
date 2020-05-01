package org.bradfordmiller.deduper

import com.google.common.collect.MutableClassToInstanceMap
import gnu.trove.map.hash.THashMap
import io.vavr.control.Either
import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.consumers.BaseConsumer
import org.bradfordmiller.deduper.consumers.DeduperDataConsumer
import org.bradfordmiller.deduper.consumers.DeduperDupeConsumer
import org.bradfordmiller.deduper.consumers.DeduperHashConsumer
import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.hashing.Hasher
import org.bradfordmiller.deduper.jndi.*
import org.bradfordmiller.deduper.persistors.*
import org.bradfordmiller.simplejndiutils.JNDIUtils
import org.bradfordmiller.sqlutils.SqlUtils
import org.jetbrains.annotations.Nullable
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.lang.IllegalArgumentException
import java.sql.ResultSet
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import javax.naming.ldap.Control
import javax.sql.DataSource
import org.bradfordmiller.deduper.ext.CollectionExtensions

typealias DeduperQueue<T> = ArrayBlockingQueue<MutableList<T>>
typealias DeduperQueueStar = ArrayBlockingQueue<MutableList<*>>

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
    var dupes: MutableMap<String, Pair<MutableList<Long>, Dupe>>,
    var success: Boolean
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
        val queueMap: MutableMap<Deduper.ControlQueue, ArrayBlockingQueue<out MutableList<*>>>,
        val controlQueues: Map<Deduper.ControlQueue, ArrayBlockingQueue<DedupeReport>>,
        val commitSize: Long = 500,
        val outputReportCommitSize: Long = 1000000,
        val config: Config,
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
        fun <T> writeData(data: MutableList<T>, queue: DeduperQueue<T>?) {
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
        fun <T> writeData(count: Long, data: MutableList<T>, queue: DeduperQueue<T>?) {
            if(count > 0 && data.size % commitSize == 0L) {
                writeData(data, queue)
            }
        }

        val seenHashes = THashMap<String, Long>()
        val dupeMap: MutableMap<String, Pair<MutableList<Long>, Dupe>> = mutableMapOf()
        val hashColumns = config.sourceJndi.hashKeys
        val dataQueue = queueMap[Deduper.ControlQueue.Target]
        val dupeQueue = queueMap[Deduper.ControlQueue.Dupes]
        val hashQueue = queueMap[Deduper.ControlQueue.Hashes]

        var distinctDupeCount = 0L
        var rsColumns = mapOf<Int, String>()
        var recordCount = 0L
        var dupeCount = 0L

        try {

            if (config.seenHashesJndi != null) {

                logger.info("Seen hashes JNDI is populated. Attempting to load hashes...")

                val ds = JNDIUtils.getDataSource(config.seenHashesJndi.jndiName, config.seenHashesJndi.context)
                val hashSourceDataSource = ds.left

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
                        val data: MutableList<Map<String, Any>> = mutableListOf()
                        val hashes: MutableList<HashRow> = mutableListOf()

                        logger.trace("$colCount columns have been found in the result set.")

                        rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                        require(rsColumns.values.containsAll(hashColumns)) {
                            "One or more provided keys $hashColumns not contained in resultset: $rsColumns"
                        }

                        val columns =
                                if (hashColumns.isEmpty())
                                    rsColumns.values.joinToString(",")
                                else
                                    hashColumns.joinToString(",")

                        logger.info("Using $columns to calculate hashes")

                        val includeJson = config.hashJndi.let {
                            (it as HashTargetType).includeJson
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

                                dataQueue.let {dq ->
                                    data.add(rsMap)
                                    writeData(recordCount, data, dq)
                                }
                                hashQueue.let {hq ->
                                    val json =
                                        if (includeJson) {
                                            JSONObject(rsMap).toString()
                                        } else {
                                            null
                                        }
                                    hashes.add(HashRow(hash, json))

                                    writeData(recordCount, hashes, hq)
                                }
                            } else {
                                if (dupeMap.containsKey(hash)) {
                                    dupeMap[hash]?.first?.add(recordCount)
                                } else {
                                    val firstSeenRow = seenHashes[hash]!!
                                    val dupeJson = JSONObject(rsMap).toString()
                                    val dupe = Dupe(firstSeenRow, dupeJson)
                                    dupeMap[hash] = Pair(mutableListOf(recordCount), dupe)

                                    distinctDupeCount += 1
                                }
                                dupeCount += 1
                                dupeQueue.let {dq ->
                                    writeData(dupeCount, dupeMap.toList().toMutableList(), dq)
                                }
                            }
                            recordCount += 1
                            if (recordCount % outputReportCommitSize == 0L)
                                logger.info("$recordCount records have been processed so far.")
                        }
                        //Flush target/dupe/hash data that's in the buffer
                        if (targetIsNotNull) {
                            writeData(data, dataQueue)
                            //Write empty list value to indicate the data stream is complete
                            writeData(mutableListOf(), dataQueue)
                        }
                        if (dupeIsNotNull) {
                            writeData(dupeMap.toList().toMutableList(), dupeQueue)
                            //Write empty list value to indicate the data stream is complete
                            writeData(mutableListOf(), dupeQueue)
                        }
                        if (hashIsNotNull) {
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
                            dupeMap,
                            true
                    )

            logger.info("Dedupe report: $ddReport")
            controlQueues.values.forEach { cq -> cq.put(ddReport) }
            logger.info("Deduping process complete.")
        } catch(ex: Exception) {
            logger.error("Error during dedupe process while publishing data: ${ex.message}")
            val ddReport = DedupeReport(
                    recordCount,
                    hashColumns,
                    rsColumns.values.toSet(),
                    dupeCount,
                    distinctDupeCount,
                    seenHashes.size.toLong(),
                    dupeMap,
                    false
            )
            //First write empty rows to each queue which indicates the stream of data is complete
            if(persistors.targetPersistor != null) {
                logger.error("Notifying data subscriber that data stream is over")
                writeData(mutableListOf(), dataQueue)
            }
            if(persistors.dupePersistor != null) {
                logger.error("Notifying data subscriber that dupe stream is over")
                writeData(mutableListOf(), dupeQueue)
            }
            if(persistors.hashPersistor != null) {
                logger.error("Notifying data subscriber that hash stream is over")
                writeData(mutableListOf(), hashQueue)
            }
            //Notify consumers and main thread that the process failed
            controlQueues.values.forEach {cq -> cq.put(ddReport)}
            logger.error("Notifying all consuming services that process failed")
        }
    }
}

/**
 * dedupes data based on [config] settings
 */
class Deduper(private val config: Config) {

    enum class ControlQueue { Producer, Target, Dupes, Hashes }

    private val persistorMap = MutableClassToInstanceMap.create<BasePersistor>()

    private fun <T: BasePersistor> addPersistorToMap(c: Class<T>, persistorBuilder: (() -> BasePersistor)?) {
        persistorMap[c] = persistorBuilder?.let { it() }
    }

    val targetPersistorBuilder: (() -> BasePersistor)? by lazy {
        when (val tj = config.targetJndi) {
            null -> null
            is CsvJNDITargetType -> {
                val csvPersistor = {
                    val tgtConfigMap = CsvConfigParser.getCsvMap(tj.context, tj.jndi)
                    CsvTargetPersistor(tgtConfigMap, tj.deleteIfExists)
                }
                csvPersistor
            }
            is SqlJNDITargetType -> {
                val sqlPersistor = { SqlTargetPersistor(tj.targetTable, tj.jndi, tj.context, tj.varcharPadding, tj.deleteIfExists) }
                sqlPersistor
            }
            else -> {
                throw IllegalArgumentException("Unrecognized Target type")
            }
        }
    }

    val dupePersistorBuilder: (() -> BasePersistor)? by lazy {
        when (val tj = config.dupesJndi) {
            null -> null
            is CsvJNDITargetType -> {
                val csvPersistor = {
                    val tgtConfigMap = CsvConfigParser.getCsvMap(tj.context, tj.jndi)
                    CsvDupePersistor(tgtConfigMap, tj.deleteIfExists)
                }
                csvPersistor
            }
            is SqlJNDITargetType -> {
                val sqlPersistor = { SqlDupePersistor(tj.jndi, tj.context, tj.deleteIfExists) }
                sqlPersistor
            }
            else -> {
                throw IllegalArgumentException("Unrecognized Target type")
            }
        }
    }

    val hashPersistorBuilder: (() -> BasePersistor)? by lazy {
        when (val tj = config.hashJndi) {
            null -> null
            is CsvJNDITargetType -> {
                val csvPersistor = {
                    val tgtConfigMap = CsvConfigParser.getCsvMap(tj.context, tj.jndi)
                    CsvHashPersistor(tgtConfigMap, tj.deleteIfExists)
                }
                csvPersistor
            }
            is SqlJNDITargetType -> {
                val sqlPersistor = { SqlHashPersistor(tj.jndi, tj.context, tj.deleteIfExists) }
                sqlPersistor
            }
            else -> {
                throw IllegalArgumentException("Unrecognized Target type")
            }
        }
    }

    private val sourceDataSource: DataSource
        by lazy {
            val ds = JNDIUtils.getDataSource(config.sourceJndi.jndiName, config.sourceJndi.context)
            ds.left
        }

    private val sqlStatement by lazy {
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
     *  @param commitSize - the number of rows to write in a single transaction
     *  @param outputReportCommitSize - the number of total rows committed before logging the result
     */
    fun dedupe(commitSize: Long = 500L, reportCommitSize: Long = 1000000L): DedupeReport {

        addPersistorToMap(TargetPersistor::class.java, targetPersistorBuilder)
        addPersistorToMap(DupePersistor::class.java, dupePersistorBuilder)
        addPersistorToMap(HashPersistor::class.java, hashPersistorBuilder)

        val queueMap: MutableMap<ControlQueue, ArrayBlockingQueue<out MutableList<*>>> = mutableMapOf()
        var controlQueues = emptyMap<ControlQueue, ArrayBlockingQueue<DedupeReport>>()
        var threadCount = 1

        controlQueues += ControlQueue.Producer to ArrayBlockingQueue<DedupeReport>(1)

        fun <T> addReportQueue(tj: JNDITargetType?, cq: ControlQueue) {
            if(tj != null) {
                threadCount += 1
                val arrayBlockingQueue = DeduperQueue<T>(100)
                queueMap.put(cq, arrayBlockingQueue)
                controlQueues += cq to ArrayBlockingQueue<DedupeReport>(1)
            }
        }

        @SuppressWarnings("unchecked")
        fun consumerBuilder (cq: ControlQueue): Runnable {
            val runnable =
               when(cq) {
                   ControlQueue.Target -> {

                       val a = queueMap[cq]
                       a

                       DeduperDataConsumer(
                           persistorMap.getInstance(TargetPersistor::class.java),
                           queueMap[cq] as DeduperQueue<Map<String, Any>>,
                           controlQueues[cq]!!,
                           sourceDataSource,
                           sqlStatement
                       )
                   }
                   ControlQueue.Dupes ->
                       DeduperDupeConsumer(
                               persistorMap.getInstance(DupePersistor::class.java),
                               queueMap[cq] as DeduperQueue<Pair<String, Pair<MutableList<Long>, Dupe>>>,
                               controlQueues[cq]!!
                       )
                   ControlQueue.Hashes ->
                       DeduperHashConsumer(
                               persistorMap.getInstance(HashPersistor::class.java),
                               queueMap[cq] as DeduperQueue<HashRow>,
                               controlQueues[cq]!!
                       )
                   else -> {
                        logger.error("Unknown ControlQueue value: ${cq.name}")
                        throw IllegalArgumentException(cq.name)
                   }
               }
            return runnable
        }

        addReportQueue<Map<String, Any>>(config.targetJndi, ControlQueue.Target)
        addReportQueue<Pair<String, Pair<MutableList<Long>, Dupe>>>(config.dupesJndi, ControlQueue.Dupes)
        addReportQueue<HashRow>(config.hashJndi, ControlQueue.Dupes)

        val executorService = Executors.newFixedThreadPool(threadCount)

        val producer =
            DeduperProducer(
                queueMap,
                controlQueues,
                commitSize,
                reportCommitSize,
                config,
                sourceDataSource,
                sqlStatement
            )

        executorService.execute(producer)

        logger.info("Producer thread started")

        queueMap.forEach {(cq,_) ->
            val consumer = consumerBuilder(cq)
            executorService.execute(consumer)
            logger.info("${cq.name} consumer thread started.")
        }

        var streamComplete = false
        lateinit var dedupeReport: DedupeReport
        val controlQueue = controlQueues[ControlQueue.Producer]!!
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