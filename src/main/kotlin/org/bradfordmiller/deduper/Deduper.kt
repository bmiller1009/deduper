package org.bradfordmiller.deduper

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.commons.codec.digest.DigestUtils
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.persistors.Dupe
import org.bradfordmiller.deduper.sql.SqlUtils

import org.bradfordmiller.deduper.utils.Left
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import javax.sql.DataSource

import kotlinx.serialization.*
import kotlinx.serialization.json.*

@Serializable
data class DedupeReport(val recordCount: Long, val columnsFound: Set<String>, val dupeCount: Long, var dupes: MutableMap<Long, Dupe>)

@Serializable
data class DupeData(val data: Map<String, Any>)

class Deduper(val config: Config) {

    //abstract fun processRs(rs: ResultSet)

    private val logger = LoggerFactory.getLogger(javaClass)

    @ImplicitReflectionSerializer
    fun dedupe(commitSize: Long = 500): DedupeReport {

        var recordCount = 0L
        var dupeCount = 0L
        var seenHashes = mutableMapOf<String, Long>()
        var dupeHashes = mutableMapOf<Long, Dupe>()
        var rsColumns = mapOf<Int, String>()

        //Get src connection from JNDI - Note that this is always cast to a datasource
        val dsSrc = (JNDIUtils.getDataSource(config.srcJndi, config.context) as Left<DataSource?, String>).left!!

        JNDIUtils.getConnection(dsSrc)!!.use { conn ->

            val sql =
                    if (config.srcName.startsWith("SELECT", true)) {
                        config.srcName
                    } else {
                        "SELECT * FROM ${config.srcName}"
                    }

            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->

                    val rsmd = rs.metaData
                    val colCount = rsmd.columnCount
                    var data: MutableList<Map<String, Any>> = mutableListOf()
                    var dupesList: MutableList<Dupe> = mutableListOf()

                    config.targetPersistor!!.createTarget(rsmd)
                    config.dupePersistor!!.createDupe()

                    rsColumns = SqlUtils.getColumnsFromRs(rsmd)

                    if (!rsColumns.values.containsAll(config.keyOn))
                        throw IllegalArgumentException("One or more provided keys ${config.keyOn} not contained in resultset: $rsColumns")

                    val keysPopulated = config.keyOn.isNotEmpty()

                    val json = Json(JsonConfiguration.Stable)

                    while (rs.next()) {

                        val hashColumns =
                                if (keysPopulated) {
                                    config.keyOn.map { rs.getString(it) }.joinToString()
                                } else {
                                    (1 until colCount).toList().map { rs.getString(it) }.joinToString()
                                }

                        val hash = DigestUtils.md5Hex(hashColumns).toUpperCase()

                        if (!seenHashes.containsKey(hash)) {
                            seenHashes.put(hash, recordCount)
                            data.add(config.targetPersistor!!.prepRow(rs, rsColumns))
                            if(recordCount % commitSize == 0L) {
                                config.targetPersistor!!.writeRows(data)
                                data.clear()
                            }
                        } else {
                            val firstSeenRow = seenHashes.get(hash)!!
                            val dupeValues = config.targetPersistor!!.prepRow(rs, rsColumns)
                            val dupeJson = json.stringify(DupeData::class.serializer(), DupeData(dupeValues))
                            val dupe = Dupe(recordCount, firstSeenRow, hashColumns, dupeJson)
                            dupesList.add(dupe)
                            dupeHashes.put(recordCount, dupe)
                            dupeCount += 1

                            if(dupeCount % commitSize == 0L) {
                                config.dupePersistor!!.writeDupes(dupesList)
                            }

                        }
                        recordCount += 1
                    }
                    //Flush target/dupe data that's in the buffer
                    config.targetPersistor!!.writeRows(data)
                    data.clear()
                }
            }
        }
        val ddReport = DedupeReport(recordCount, rsColumns.values.toSet(), dupeCount, dupeHashes)
        logger.info("Dedupe report: $ddReport")
        return ddReport
    }
}