package org.bradfordmiller.deduper.consumers

import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.persistors.CsvTargetPersistor
import org.bradfordmiller.deduper.persistors.TargetPersistor
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.concurrent.BlockingQueue
import javax.sql.DataSource

class DeduperDataConsumer(
    val targetPersistor: TargetPersistor,
    val dataQueue: BlockingQueue<MutableList<Map<String, Any>>>,
    val controlQueue: BlockingQueue<DedupeReport>,
    val sourceDataSource: DataSource,
    val sqlStatement: String,
    val deleteTargetIfExists: Boolean
): Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(DeduperDataConsumer::class.java)
    }

    override fun run() {

        var totalRowsWritten = 0L

        val finalSqlStatement =
            if(sqlStatement.contains("WHERE")) {
                sqlStatement + " AND 1 = 2 "
            } else {
                sqlStatement + " WHERE 1 = 2 "
            }

        val rsmd = JNDIUtils.getConnection(sourceDataSource)!!.use { conn ->
            logger.info("The following sql statement will be run: $finalSqlStatement")
            conn.prepareStatement(finalSqlStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.metaData
                }
            }
        }

        val firstMsg = dataQueue.take()
        var done = if(firstMsg.isEmpty()) {
            //This should never happen
            logger.info("First message is empty, stream complete.")
            true
        } else {
            logger.info("Initializing target consumer")
            targetPersistor.createTarget(rsmd, deleteTargetIfExists)
            totalRowsWritten += targetPersistor.writeRows(firstMsg)
            logger.info("First data packet written to target.  $totalRowsWritten rows written so far.")
            false
        }

        while(!done) {
            val data = dataQueue.take()
            //Empty record hit, means stream is complete
            if(data.isEmpty()) {
                done = true
            } else {
                totalRowsWritten += targetPersistor.writeRows(data)
                //TODO: Parameterize this
                if(totalRowsWritten % 100 == 0L) {
                    logger.info("Total rows written to target: $totalRowsWritten")
                }
            }
        }

        if(targetPersistor is CsvTargetPersistor) {
            targetPersistor.unlockFile()
            logger.info("Target file unlocked.")
        }

        val dedupeReport = controlQueue.take()
        val dedupeCount = dedupeReport.recordCount - dedupeReport.dupeCount
        //Check that dedupe report publish numbers match persisted numbers
        if(totalRowsWritten != dedupeCount) {
            logger.error(
                "Dedupe report records published (${dedupeCount}) does not match rows persisted by the target persistor " +
                        "(${totalRowsWritten})"
            )
        }
    }
}