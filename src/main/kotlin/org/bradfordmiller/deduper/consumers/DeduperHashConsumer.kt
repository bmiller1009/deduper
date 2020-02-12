package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import java.util.concurrent.BlockingQueue

class DeduperHashConsumer(
    hashPersistor: HashPersistor,
    hashQueue: BlockingQueue<MutableList<HashRow>>,
    controlQueue: BlockingQueue<DedupeReport>,
    deleteIfExists: Boolean
): BaseConsumer<HashRow, HashPersistor>(hashPersistor, hashQueue, controlQueue, deleteIfExists) {

    override fun createTarget(deleteIfExists: Boolean, persistor: HashPersistor) {
        persistor.createHashTable(deleteIfExists)
    }
    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.hashCount
    }
}