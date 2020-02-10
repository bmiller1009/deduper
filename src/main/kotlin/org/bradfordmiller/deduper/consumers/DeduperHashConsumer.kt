package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

class DeduperHashConsumer(
    val hashPersistor: HashPersistor,
    val hashQueue: BlockingQueue<MutableList<HashRow>>,
    val controlQueue: BlockingQueue<DedupeReport>,
    val deleteHashIfExists: Boolean
): Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(DeduperHashConsumer::class.java)
    }

    override fun run() {

        var totalRowsWritten = 0L

        val firstMsg = hashQueue.take()
        var done = if(firstMsg.isEmpty()) {
            //This should never happen
            logger.info("First message is empty, stream complete.")
            true
        } else {
            logger.info("Initializing hash consumer")
            hashPersistor.createHashTable(deleteHashIfExists)
            totalRowsWritten += hashPersistor.writeRows(firstMsg)
            logger.info("First data packet written to hash persistence.  $totalRowsWritten rows written so far.")
            false
        }

        while(!done) {
            val data = hashQueue.take()
            //Empty record hit, means stream is complete
            if(data.isEmpty()) {
                done = true
            } else {
                totalRowsWritten += hashPersistor.writeRows(data)
                //TODO: Parameterize this
                if(totalRowsWritten % 100 == 0L) {
                    logger.info("Total rows written to hash persistence: $totalRowsWritten")
                }
            }
        }

        if(hashPersistor is CsvHashPersistor) {
            hashPersistor.unlockFile()
            logger.info("Target file unlocked.")
        }

        val dedupeReport = controlQueue.take()
        //Check that dedupe report publish numbers match persisted numbers
        if(totalRowsWritten != dedupeReport.hashCount) {
            logger.error(
                "Dedupe report records published (${dedupeReport.hashCount}) does not match rows persisted by the " +
                        "dupe persistor (${totalRowsWritten})"
            )
        }
    }
}