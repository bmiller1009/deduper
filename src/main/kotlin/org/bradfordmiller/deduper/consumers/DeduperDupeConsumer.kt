package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import java.util.concurrent.BlockingQueue

class DeduperDupeConsumer(
    dupePersistor: DupePersistor,
    dupeQueue: BlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>,
    controlQueue: BlockingQueue<DedupeReport>,
    deleteDupeIfExists: Boolean
): BaseConsumer<Pair<String, Pair<MutableList<Long>, Dupe>>, DupePersistor>(dupePersistor, dupeQueue, controlQueue,
    deleteDupeIfExists) {

    override fun createTarget(deleteIfExists: Boolean, persistor: DupePersistor) {
        persistor.createDupe(deleteIfExists)
    }
    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.distinctDupeCount
    }
}