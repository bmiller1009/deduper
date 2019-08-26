package org.bradfordmiller.deduper.persistors

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.FileUtils
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException

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

abstract class SqlPersistor(val conn: Connection) {

    internal val logger = LoggerFactory.getLogger(javaClass)

    init {
        conn.setAutoCommit(false)
    }
}

class SqlDDL(val rsmd: ResultSetMetaData, val targetName: String, conn: Connection) {
    val createDDL = SqlUtils.generateDDL(targetName, rsmd)
    private val insertDDL = SqlUtils.generateInsert(targetName, rsmd)
    val pstmt = conn.prepareStatement(insertDDL)
}

class SqlTargetPersistor(val targetName: String, conn: Connection): SqlPersistor(conn), TargetPersistor {

    lateinit var sqlDDL: SqlDDL

    override fun createTarget(rsmd: ResultSetMetaData) {
        sqlDDL = SqlDDL(rsmd, targetName, conn)
        val ddl = sqlDDL.createDDL
        SqlUtils.executeDDL(conn, ddl)
    }
    override fun writeRows(data: List<Map<String, Any>>) {
        /*data.forEach {
            sqlDDL.pstmt.
        }*/
    }
}

class SqlDupePersistor(conn: Connection): SqlPersistor(conn), DupePersistor {

    private val createStatement = "CREATE TABLE dupes(row_id BIGINT NOT NULL, first_found_row_number BIGINT NOT NULL, dupe_values VARCHAR(MAX) NOT NULL)"
    private val insertStatement = "INSERT INTO TABLE dupes(row_id, first_found_row_number, dupe_values) VALUES (?,?,?)"
    private val pstmt = conn.prepareStatement(insertStatement)

    override fun createDupe() {
        SqlUtils.executeDDL(conn, createStatement)
    }
    override fun writeDupes(dupes: MutableList<Dupe>) {
        try {
            dupes.forEach {
                pstmt.setLong(1, it.rowId)
                pstmt.setLong(2, it.firstFoundRowNumber)
                pstmt.setString(3, it.dupes)
                pstmt.execute()
            }
            conn.commit()
        } catch(sql: SQLException) {
            logger.error("Error while inserting duplicate values: ${sql.message}")
        } finally {
            conn.rollback()
        }
    }
}