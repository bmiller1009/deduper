package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.CsvTargetPersistor
import org.bradfordmiller.deduper.persistors.WritePersistor
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

/**
 * Base definition of a runnable Consumer. Consumers are responsible for persisting data to disk
 *
 * @param T - the type of data being persisted
 * @param P - the type of [WritePersistor]
 * @property persistor - a [WritePersistor]
 * @property dataQueue - queue where persistor receives data to persist
 * @property controlQueue - queue where persistor receives [DedupeReport]
 * @property deleteIfExists - determines whether persistent object (table or file) is dropped before being recreated
 */
abstract class BaseConsumer<T, P: WritePersistor<T>>(
  val persistor: P,
  val dataQueue: BlockingQueue<MutableList<T>>,
  val controlQueue: BlockingQueue<DedupeReport>,
  val deleteIfExists: Boolean
): Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(BaseConsumer::class.java)
    }

    var totalRowsWritten = 0L

    /**
     * creates the target - either flat file or database table
     *
     * [deleteIfExists] determines whether persistent object (table or file) is dropped before being recreated
     * the [persistor] writing the data
     */
    abstract fun createTarget(deleteIfExists: Boolean, persistor: P)

    /**
     * the report metric in the [dedupeReport] to check
     *
     * the [dedupeReport] delivered from the publisher
     */
    abstract fun getDeduperReportCount(dedupeReport: DedupeReport): Long

    /**
     * pulls/processes the first message off of the [dataQueue] and returns whether or not the message is empty
     */
    fun processFirstMessage(): Boolean {
        val firstMsg = dataQueue.take()
        return if(firstMsg.isEmpty()) {
            //This should never happen
            logger.info("First message is empty, stream complete.")
            true
        } else {
            logger.info("${this.javaClass.canonicalName}:: Initializing target consumer")
            createTarget(deleteIfExists, persistor)
            totalRowsWritten += persistor.writeRows(firstMsg)
            logger.info("${this.javaClass.canonicalName}:: First data packet written to target.  $totalRowsWritten " +
                    "rows written" +
                    " so far.")
            false
        }
    }

    /**
     * loops over the queue consuming messages until [doneFlag] is set to true, meaning the last message was empty
     */
    fun processQueueData(doneFlag: Boolean) {
        var done = doneFlag
        while(!done) {
            val data = dataQueue.take()
            //Empty record hit, means stream is complete
            if(data.isEmpty()) {
                done = true
            } else {
                totalRowsWritten += persistor.writeRows(data)
                //TODO: Parameterize this
                if(totalRowsWritten % 100 == 0L) {
                    logger.info("${this.javaClass.canonicalName}:: Total rows written to target: $totalRowsWritten")
                }
            }
        }
    }

    /**
     * removes the lock file from a flat file once persistence to file is complete
     */
    fun unlockCsvFile() {
        if(persistor is CsvTargetPersistor) {
            persistor.unlockFile()
            logger.info("${this.javaClass.canonicalName}:: Target file unlocked.")
        }
    }

    /**
     * consumes the [controlQueue] for a [DedupeReport], indicating the publishing of all data is complete
     *
     * returns a [DedupeReport]
     */
    fun processDeduperReport(): DedupeReport {
        val dedupeReport = controlQueue.take()
        val dedupeCount = getDeduperReportCount(dedupeReport)
        //Check that dedupe report publish numbers match persisted numbers
        if(totalRowsWritten != dedupeCount) {
            logger.error(
                "Dedupe report records published (${dedupeCount}) does not match rows persisted by the target persistor " +
                        "(${totalRowsWritten})"
            )
        }
        return dedupeReport
    }

    /**
     * launches consumer as a runnable
     */
    override fun run() {
        val complete = processFirstMessage()
        processQueueData(complete)
        unlockCsvFile()
        processDeduperReport()
    }
}