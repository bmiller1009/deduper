package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

/**
 * Consumer for processing and persisting duplicate data
 *
 * @property dupePersistor - a [WritePersistor]
 * @property dupeQueue - queue where persistor receives data to persist
 * @property controlQueue - queue where persistor receives [DedupeReport]
 * @property deleteDupeIfExists - determines whether persistent object (table or file) is dropped before being recreated
 */
class DeduperDupeConsumer(
    dupePersistor: DupePersistor,
    dupeQueue: BlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>,
    controlQueue: ArrayBlockingQueue<DedupeReport>,
    deleteDupeIfExists: Boolean
): BaseConsumer<Pair<String, Pair<MutableList<Long>, Dupe>>, DupePersistor>(dupePersistor, dupeQueue, controlQueue,
    deleteDupeIfExists) {

    /**
     *  create/prep duplicate persistence - can be database table or flat file
     *
     *  [deleteIfExists] indicates whether to delete the [dupePersistor] table/flat file if it already exists
     */
    override fun createTarget(deleteIfExists: Boolean, persistor: DupePersistor) {
        persistor.createDupe(deleteIfExists)
    }
    /**
     * gets the duplicate count from [dedupeReport]
     */
    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.distinctDupeCount
    }
}