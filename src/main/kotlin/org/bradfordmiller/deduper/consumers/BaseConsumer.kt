package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.CsvTargetPersistor
import org.bradfordmiller.deduper.persistors.WritePersistor
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

abstract class BaseConsumer<T>(
  val persistor: WritePersistor<T>,
  val dataQueue: BlockingQueue<MutableList<T>>,
  val controlQueue: BlockingQueue<DedupeReport>,
  val deleteIfExists: Boolean
): Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(BaseConsumer::class.java)
    }

    var totalRowsWritten = 0L

    abstract fun createTarget(deleteIfExists: Boolean)
    abstract fun getDeduperReportCount(dedupeReport: DedupeReport): Long

    fun processFirstMessage(): Boolean {
        val firstMsg = dataQueue.take()
        return if(firstMsg.isEmpty()) {
            //This should never happen
            logger.info("First message is empty, stream complete.")
            true
        } else {
            logger.info("Initializing target consumer")
            createTarget(deleteIfExists)
            totalRowsWritten += persistor.writeRows(firstMsg)
            logger.info("First data packet written to target.  $totalRowsWritten rows written so far.")
            false
        }
    }
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
                    logger.info("Total rows written to target: $totalRowsWritten")
                }
            }
        }
    }
    fun unlockCsvFile() {
        if(persistor is CsvTargetPersistor) {
            persistor.unlockFile()
            logger.info("Target file unlocked.")
        }
    }
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
    override fun run() {
        val complete = processFirstMessage()
        processQueueData(complete)
        unlockCsvFile()
        processDeduperReport()
    }
}