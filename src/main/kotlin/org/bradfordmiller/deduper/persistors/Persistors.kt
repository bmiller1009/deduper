package org.bradfordmiller.deduper.persistors

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.FileUtils
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import kotlinx.serialization.*
import kotlinx.serialization.json.*

@Serializable
data class Dupe(val rowId: Long, val firstFoundRowNumber: Long, val hashColumns: String, val dupes: String) {
    override fun toString(): String {
        return rowId.toString() + "," +
        firstFoundRowNumber.toString() + "," +
        hashColumns + "," +
        dupes
    }
}

interface TargetPersistor {
    fun createTarget(rsmd: ResultSetMetaData)
    fun prepRow(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any>
    fun writeRows(data: List<Map<String, Any>>)
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
    override fun prepRow(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
        val row = (1 until colNames.size).map{it ->
            val column = colNames[it]!!
            column to rs.getObject(column)
        }.toMap()

        return row
    }
    override fun writeRows(data: List<Map<String, Any>>) {
        val stringData = convertRowsToStrings(data)
        FileUtils.writeStringsToFile(stringData, ccp.targetName, ccp.extension)
    }
}

class CsvDupePersistor(config: Map<String, String>): CsvPersistor(config), DupePersistor {
    override fun createDupe() {
        val columns = setOf("row_id","hash_columns","dupe_values")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
    override fun writeDupes(dupes: MutableList<Dupe>) {
        val data = dupes.map {it.toString()}
        FileUtils.writeStringsToFile(data, ccp.targetName, ccp.extension)
    }
}

class SqlTargetPersistor(val targetName: String, val conn: Connection): TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val ddl = SqlUtils.generateDDL(targetName, rsmd)
        SqlUtils.executeDDL(conn, ddl.createStatement)
    }
    override fun prepRow(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
    override fun writeRows(data: List<Map<String, Any>>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class SqlDupePersistor(val conn: Connection): DupePersistor {
    override fun createDupe() {
        val sql = "CREATE TABLE dupes(row_id BIGINT NOT NULL, hash_columns VARCHAR(MAX), dupe_values VARCHAR(MAX) NOT NULL)"
        SqlUtils.executeDDL(conn, sql)
    }
    override fun writeDupes(dupes: MutableList<Dupe>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}