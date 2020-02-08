package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

class DeduperDupeConsumer(
    val dupePersistor: DupePersistor,
    val dupeQueue: BlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>,
    val controlQueue: BlockingQueue<DedupeReport>,
    val deleteDupeIfExists: Boolean
): Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(DeduperDupeConsumer::class.java)
    }

    override fun run() {

        var totalRowsWritten = 0L

        val firstMsg = dupeQueue.take()
        var done = if(firstMsg.isEmpty()) {
            //This should never happen
            logger.info("First message is empty, stream complete.")
            true
        } else {
            logger.info("Initializing dupe consumer")
            dupePersistor.createDupe(deleteDupeIfExists)
            totalRowsWritten += dupePersistor.writeRows(firstMsg)
            logger.info("First data packet written to dupe persistence.  $totalRowsWritten rows written so far.")
            false
        }

        while(!done) {
            val data = dupeQueue.take()
            //Empty record hit, means stream is complete
            if(data.isEmpty()) {
                done = true
            } else {
                totalRowsWritten += dupePersistor.writeRows(data)
                //TODO: Parameterize this
                if(totalRowsWritten % 100 == 0L) {
                    logger.info("Total rows written to target: $totalRowsWritten")
                }
            }
        }

        if(dupePersistor is CsvDupePersistor) {
            dupePersistor.unlockFile()
            logger.info("Target file unlocked.")
        }

        val dedupeReport = controlQueue.take()
        //Check that dedupe report publish numbers match persisted numbers
        if(totalRowsWritten != dedupeReport.distinctDupeCount) {
            logger.error(
                "Dedupe report records published (${dedupeReport.distinctDupeCount}) does not match rows persisted by the " +
                        "dupe persistor (${totalRowsWritten})"
            )
        }
    }
}