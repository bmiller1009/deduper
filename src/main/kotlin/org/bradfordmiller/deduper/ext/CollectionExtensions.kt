package org.bradfordmiller.deduper.ext

import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.DeduperProducer
import org.bradfordmiller.deduper.DeduperQueue
import org.bradfordmiller.deduper.DeduperQueueStar
import org.bradfordmiller.deduper.persistors.Dupe
import org.bradfordmiller.deduper.persistors.HashRow
import org.slf4j.LoggerFactory
import java.lang.IllegalArgumentException
import java.util.concurrent.ArrayBlockingQueue

class CollectionExtensions {
    companion object {
        val logger = LoggerFactory.getLogger(DeduperProducer::class.java)
    }
    fun <T> DeduperQueue<T>.readwithCast(): DeduperQueue<T> {
        this.
        /*when(cq) {
            Deduper.ControlQueue.Target -> this[cq] as DeduperQueue<Map<String, Any>>
            Deduper.ControlQueue.Dupes -> this[cq] as DeduperQueue<Pair<String, Pair<MutableList<Long>, Dupe>>>
            Deduper.ControlQueue.Hashes -> this[cq] as DeduperQueue<HashRow>
            else -> {
                logger.error("Unknown ControlQueue value: ${cq.name}")
                throw IllegalArgumentException(cq.name)
            }
        }*/
    }
}