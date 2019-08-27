package org.bradfordmiller.deduper.persistors

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.FileUtils
import org.slf4j.LoggerFactory
import java.sql.*

data class Dupe(val rowId: Long, val firstFoundRowNumber: Long, val dupes: String)

interface TargetPersistor {
    fun createTarget(rsmd: ResultSetMetaData)
    fun writeRows(data: List<Map<String, Any>>)
    fun prepRow(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
        val row = (1 until colNames.size).map{it ->
            val column = colNames[it]!!
            column to rs.getObject(column)
        }.toMap()

        return row
    }
}

interface DupePersistor {
    fun createDupe()
    fun writeDupes(dupes: MutableList<Dupe>)
}

abstract class CsvPersistor(config: Map<String, String>) {
    val ccp = CsvConfigParser(config)

    fun convertRowsToStrings(data: List<Map<String, Any>>): List<String> {
        val convert = data.map {strings ->
            strings.values.map{it -> it.toString()}.joinToString(separator=ccp.delimiter)
        }
        return convert
    }
}

class CsvTargetPersistor(config: Map<String, String>): CsvPersistor(config), TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val columns = SqlUtils.getColumnsFromRs(rsmd)
        FileUtils.prepFile(ccp.targetName, columns.values.toSet(), ccp.extension, ccp.delimiter)
    }
    override fun writeRows(data: List<Map<String, Any>>) {
        val stringData = convertRowsToStrings(data)
        FileUtils.writeStringsToFile(stringData, ccp.targetName, ccp.extension)
    }
}

class CsvDupePersistor(config: Map<String, String>): CsvPersistor(config), DupePersistor {
    override fun createDupe() {
        val columns = setOf("row_id", "first_found_row_number", "dupe_values")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
    override fun writeDupes(dupes: MutableList<Dupe>) {
        val data = dupes.map {it ->
            it.rowId.toString() + ccp.delimiter +
              it.firstFoundRowNumber.toString() + ccp.delimiter +
              it.dupes
        }
        FileUtils.writeStringsToFile(data, ccp.targetName, ccp.extension)
    }
}

class SqlTargetPersistor(val targetName: String, val targetJndi: String, val context: String): TargetPersistor {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val dbInfo by lazy {
        val sql = "SELECT * FROM $targetName"
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val metadata = rs.metaData
                    Pair(SqlUtils.generateInsert(targetName, metadata, conn.metaData.databaseProductName), SqlUtils.getColumnIdxFromRs(metadata))
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

    override fun writeRows(data: List<Map<String, Any>>) {
        val sql = dbInfo.first
        val columnMap = dbInfo.second
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            conn.setAutoCommit(false)
            conn.prepareStatement(sql).use {pstmt ->
                try {
                    data.forEach { kvp ->
                        kvp.forEach {
                            val idx = columnMap[it.key]!!
                            pstmt.setObject(idx, it.value)
                        }
                        pstmt.execute()
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

class SqlDupePersistor(val dupesJndi: String, val context: String): DupePersistor {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val insertStatement = "INSERT INTO dupes(row_id, first_found_row_number, dupe_values) VALUES (?,?,?)"

    override fun createDupe() {
        JNDIUtils.getJndiConnection(dupesJndi, context).use { conn ->
            val vendor = conn.metaData.databaseProductName
            val createStatement =
                "CREATE TABLE dupes(row_id ${SqlUtils.getLongType(vendor)} NOT NULL, first_found_row_number ${SqlUtils.getLongType(vendor)} NOT NULL, dupe_values ${SqlUtils.getStringType(vendor)} ${SqlUtils.getStringMaxSize(vendor)} NOT NULL)"
            SqlUtils.executeDDL(conn, createStatement)
        }
    }
    override fun writeDupes(dupes: MutableList<Dupe>) {
        JNDIUtils.getJndiConnection(dupesJndi, context).use {conn ->
            conn.setAutoCommit(false)
            conn.prepareStatement(insertStatement).use {pstmt ->
                try {
                    dupes.forEach {
                        pstmt.setLong(1, it.rowId)
                        pstmt.setLong(2, it.firstFoundRowNumber)
                        pstmt.setString(3, it.dupes)
                        pstmt.execute()
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