package org.bradfordmiller.deduper.persistors

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.sql.SqlVendorTypes
import org.bradfordmiller.deduper.utils.FileUtils
import org.json.JSONArray
import org.slf4j.LoggerFactory
import java.sql.*

data class Dupe(val firstFoundRowNumber: Long, val dupes: String)

interface WritePersistor<T> {
    fun writeRows(rows: MutableList<T>)
}
interface TargetPersistor: WritePersistor<Map<String, Any>> {
    override fun writeRows(rows: MutableList<Map<String, Any>>)
    fun createTarget(rsmd: ResultSetMetaData)
    fun prepRow(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
        return (1..colNames.size).map{
            val column = colNames[it] ?: error("Column Index $it does not have an entry in the column name map.")
            column to rs.getObject(column)
        }.toMap()
    }
}
interface DupePersistor: WritePersistor<Pair<String, Pair<MutableList<Long>, Dupe>>> {
    override fun writeRows(rows: MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>)
    fun createDupe()
}
abstract class CsvPersistor(config: Map<String, String>) {
    val ccp = CsvConfigParser(config)

    fun convertRowsToStrings(data: List<Map<String, Any>>): List<String> {
        return data.map {strings ->
            strings.values.joinToString(separator = ccp.delimiter) { it.toString() }
        }
    }
}
class CsvTargetPersistor(config: Map<String, String>): CsvPersistor(config), TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val columns = SqlUtils.getColumnsFromRs(rsmd)
        FileUtils.prepFile(ccp.targetName, columns.values.toSet(), ccp.extension, ccp.delimiter)
    }
    override fun writeRows(rows: MutableList<Map<String, Any>>) {
        val stringData = convertRowsToStrings(rows)
        FileUtils.writeStringsToFile(stringData, ccp.targetName, ccp.extension)
    }
}
class CsvDupePersistor(config: Map<String, String>): CsvPersistor(config), DupePersistor {
    override fun createDupe() {
        val columns = setOf("hash", "row_ids", "first_found_row_number", "dupe_values")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
    //TODO: Clean this up.  The pair syntax second.second.blah is clunky and hard to read
    override fun writeRows(rows: MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>) {
        val data = rows.map {
            val list = it.second.first
            val json = JSONArray(list).toString()
            it.first + ccp.delimiter +
              json + ccp.delimiter +
              it.second.second.firstFoundRowNumber + ccp.delimiter +
              it.second.second.dupes
        }
        FileUtils.writeStringsToFile(data, ccp.targetName, ccp.extension)
    }
}
class SqlTargetPersistor(
    private val targetName: String,
    private val targetJndi: String,
    private val context: String
): TargetPersistor {

    companion object {
        val logger = LoggerFactory.getLogger(SqlTargetPersistor::class.java)
    }
    private val dbInfo by lazy {
        val sql = "SELECT * FROM $targetName"
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val metadata = rs.metaData
                    Pair(
                        SqlUtils.generateInsert(targetName, metadata, conn.metaData.databaseProductName),
                        SqlUtils.getColumnIdxFromRs(metadata)
                    )
                }
            }
        }
    }
    override fun createTarget(rsmd: ResultSetMetaData) {
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            val vendor = conn.metaData.databaseProductName
            val ddl = SqlUtils.generateDDL(targetName, rsmd, vendor)
            SqlUtils.executeDDL(conn, ddl)
        }
    }
    override fun writeRows(rows: MutableList<Map<String, Any>>) {
        val sql = dbInfo.first
        val columnMap = dbInfo.second
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(sql).use {pstmt ->
                try {
                    rows.forEach { kvp ->
                        kvp.forEach {
                            val idx = columnMap[it.key]!!
                            pstmt.setObject(idx, it.value)
                        }
                        pstmt.addBatch()
                    }
                    try {
                        pstmt.executeBatch()
                    } catch(sqlEx: SQLException) {
                        logger.error("Error committing batch: ${sqlEx.message}")
                        return
                    }
                    conn.commit()
                } catch (sqlEx: SQLException) {
                    logger.error("Error while inserting data: ${sqlEx.message}")
                } finally {
                    conn.rollback()
                }
            }
        }
    }
}
class SqlDupePersistor(private val dupesJndi: String, private val context: String): DupePersistor {

    companion object {
        val logger = LoggerFactory.getLogger(SqlDupePersistor::class.java)
    }

    //TODO: Make a list of dupe columns and then pass it to both the INSERT and CREATE statements
    private val insertStatement = "INSERT INTO dupes(hash, row_ids, first_found_row_number, dupe_values) VALUES (?,?,?,?)"

    override fun createDupe() {
        JNDIUtils.getJndiConnection(dupesJndi, context).use { conn ->
            val vendor = conn.metaData.databaseProductName
            val sqlVendorTypes = SqlVendorTypes(vendor)
            val createStatement =
                "CREATE TABLE dupes(" +
                 "hash ${sqlVendorTypes.getStringType()} NOT NULL, " +
                 "row_ids ${sqlVendorTypes.getLongType()} NOT NULL, " +
                 "first_found_row_number ${sqlVendorTypes.getLongType()} NOT NULL, " +
                 "dupe_values ${sqlVendorTypes.getStringType()} ${sqlVendorTypes.getStringMaxSize()} NOT NULL," +
                 "${sqlVendorTypes.getPrimaryKeySyntax("hash")}" +
                 ")"
            SqlUtils.executeDDL(conn, createStatement)
        }
    }
    //TODO: Clean this up.  The pair syntax second.second.blah is clunky and hard to read
    override fun writeRows(rows: MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>) {
        JNDIUtils.getJndiConnection(dupesJndi, context).use {conn ->
            conn.autoCommit = false
            conn.prepareStatement(insertStatement).use {pstmt ->
                try {
                    rows.forEach {
                        pstmt.setString(1, it.first)
                        pstmt.setString(2, JSONArray(it.second.first).toString())
                        pstmt.setLong(3, it.second.second.firstFoundRowNumber)
                        pstmt.setString(4, it.second.second.dupes)
                        pstmt.addBatch()
                    }
                    try {
                        pstmt.executeBatch()
                    } catch(sqlEx: SQLException) {
                        logger.error("Error committing batch: ${sqlEx.message}")
                        return
                    }
                    conn.commit()
                } catch (sqlEx: SQLException) {
                    logger.error("Error while inserting duplicate values: ${sqlEx.message}")
                } finally {
                    conn.rollback()
                }
            }
        }
    }
}