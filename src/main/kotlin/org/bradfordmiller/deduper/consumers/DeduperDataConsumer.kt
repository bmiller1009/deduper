package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.persistors.TargetPersistor
import org.bradfordmiller.deduper.sql.SqlUtils
import java.util.concurrent.BlockingQueue
import javax.sql.DataSource

class DeduperDataConsumer(
    targetPersistor: TargetPersistor,
    dataQueue: BlockingQueue<MutableList<Map<String, Any>>>,
    controlQueue: BlockingQueue<DedupeReport>,
    deleteIfExists: Boolean,
    val sourceDataSource: DataSource,
    val sqlStatement: String
): BaseConsumer<Map<String, Any>, TargetPersistor>(targetPersistor, dataQueue, controlQueue, deleteIfExists) {

    override fun createTarget(deleteIfExists: Boolean, persistor: TargetPersistor) {
        val finalSqlStatement =
            if(sqlStatement.contains("WHERE")) {
                sqlStatement + " AND 1 = 2 "
            } else {
                sqlStatement + " WHERE 1 = 2 "
            }

        val qi =
            JNDIUtils.getConnection(sourceDataSource)!!.use { conn ->
                SqlUtils.getQueryInfo(finalSqlStatement, conn)
            }

        persistor.createTarget(qi, deleteIfExists)
    }

    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.recordCount - dedupeReport.dupeCount
    }
}