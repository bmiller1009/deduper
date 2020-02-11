package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.persistors.*
import java.util.concurrent.BlockingQueue

class DeduperDupeConsumer(
    val dupePersistor: DupePersistor,
    dupeQueue: BlockingQueue<MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>>,
    controlQueue: BlockingQueue<DedupeReport>,
    deleteDupeIfExists: Boolean
): BaseConsumer<Pair<String, Pair<MutableList<Long>, Dupe>>>(dupePersistor, dupeQueue, controlQueue, deleteDupeIfExists) {

    override fun createTarget(deleteIfExists: Boolean) {
        dupePersistor.createDupe(deleteIfExists)
    }
    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.distinctDupeCount
    }
}