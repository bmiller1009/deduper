package org.bradfordmiller.deduper

import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.slf4j.LoggerFactory
import java.sql.ResultSet

data class DedupeReport(val recordCount: Long, val dupeCount: Long, var dupes: MutableMap<Long, Dupe>)
data class Dupe(val rowNumber: Long, val firstFoundRowNumber: Long, val dupeValues: String)

class Deduper() {

    private val logger = LoggerFactory.getLogger(javaClass)

    companion object {

        fun dedupe(
            sourceJndi: String,
            sourceName: String,
            context: String,
            targetJndi: String,
            targetName: String,
            dupesName: String,
            keyOn: Set<String>
        ): DedupeReport {

            var recordCount = 0L
            var dupeCount = 0L
            var seenHashes = mutableMapOf<String, Long>()
            var dupeHashes = mutableMapOf<Long, Dupe>()

            //Get source connection from JNDI
            val ds = JNDIUtils.getDataSource(sourceJndi, context)!!
            JNDIUtils.getConnection(ds)!!.use { conn ->
                val sql = "SELECT * FROM $sourceName"
                conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                    stmt.executeQuery().use { rs ->

                        val colCount = rs.metaData.columnCount
                        while (rs.next()) {

                            val hashColumns =
                                if(!keyOn.isEmpty())
                                    keyOn.map {rs.getString(it)}.joinToString()
                                else
                                    (1 to colCount).toList().map{rs.getString(it)}.joinToString()

                            val hash = DigestUtils.md5Hex(hashColumns).toUpperCase()

                            if (!seenHashes.containsKey(hash)) {
                                seenHashes.put(hash, recordCount)
                            } else {
                                val firstSeenRow = seenHashes.get(hash)!!
                                val dupe = Dupe(recordCount, firstSeenRow, hashColumns)
                                dupeHashes.put(recordCount, dupe)
                                dupeCount += 1
                            }

                            recordCount += 1
                        }
                    }
                }
            }
            return DedupeReport(recordCount, dupeCount, dupeHashes)
        }
    }
}