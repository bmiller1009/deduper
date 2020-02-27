package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.simplejndiutils.JNDIUtils
import org.bradfordmiller.deduper.persistors.TargetPersistor
import org.bradfordmiller.deduper.persistors.WritePersistor
import org.bradfordmiller.sqlutils.SqlUtils
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import javax.sql.DataSource

/**
 * Consumer for processing and persisting target data, IE "deduped" data
 *
 * @property targetPersistor - a [WritePersistor]
 * @property dataQueue - queue where persistor receives data to persist
 * @property controlQueue - queue where persistor receives [DedupeReport]
 * @property deleteIfExists - determines whether persistent object (table or file) is dropped before being recreated
 * @property sourceDataSource - the data source used by the publisher. This is needed to build a persistent store which
 * mirrors the published data
 * @property sqlStatement - SQL statement used by the publisher
 */
class DeduperDataConsumer(
    targetPersistor: TargetPersistor,
    dataQueue: BlockingQueue<MutableList<Map<String, Any>>>,
    controlQueue: ArrayBlockingQueue<DedupeReport>,
    deleteIfExists: Boolean,
    val sourceDataSource: DataSource,
    val sqlStatement: String
): BaseConsumer<Map<String, Any>, TargetPersistor>(targetPersistor, dataQueue, controlQueue, deleteIfExists) {

    /**
     *  create/prep target persistence - can be database table or flat file
     *
     *  [deleteIfExists] indicates whether to delete the [targetPersistor] table/flat file if it already exists
     */
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

    /**
     * gets the deduped count from [dedupeReport]
     */
    override fun getDeduperReportCount(dedupeReport: DedupeReport): Long {
        return dedupeReport.recordCount - dedupeReport.dupeCount
    }
}