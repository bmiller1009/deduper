package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

/**
 * Consumer for processing and persisting MD-5 hashes data
 *
 * @property hashPersistor - a [WritePersistor]
 * @property hashQueue - queue where persistor receives data to persist
 * @property controlQueue - queue where persistor receives [DedupeReport]
 * @property deleteIfExists - determines whether persistent object (table or file) is dropped before being recreated
 */
class DeduperHashConsumer(
    hashPersistor: HashPersistor,
    hashQueue: BlockingQueue<MutableList<HashRow>>,
    controlQueue: ArrayBlockingQueue<DedupeReport>,
    deleteIfExists: Boolean
): BaseConsumer<HashRow, HashPersistor>(hashPersistor, hashQueue, controlQueue, deleteIfExists) {

    /**
     *  create/prep MD-5 hash persistence - can be database table or flat file
     *
     *  [deleteIfExists] indicates whether to delete the [hashPersistor] table/flat file if it already exists
     */
    override fun createTarget(deleteIfExists: Boolean, persistor: HashPersistor) {
        persistor.createHashTable(deleteIfExists)
    }

    /**
     * gets the MD-5 hash count from [dedupeReport]
     */
    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.hashCount
    }
}