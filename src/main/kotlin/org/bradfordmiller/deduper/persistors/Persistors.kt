package org.bradfordmiller.deduper.persistors

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.sql.SqlUtils
import org.bradfordmiller.deduper.utils.FileUtils
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData

interface TargetPersistor {
    fun createTarget(rsmd: ResultSetMetaData)
    fun writeRow(rs: ResultSet, colCount: Int)
}

interface DupePersistor {
    fun createDupe()
    fun writeDupe(rowId: Long, dupes: String)
}

abstract class CsvPersistor(config: Map<String, String>) {
    val ccp = CsvConfigParser(config)
}

class CsvTargetPersistor(config: Map<String, String>): CsvPersistor(config), TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val columns = SqlUtils.getColumnsFromRs(rsmd)
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
    override fun writeRow(rs: ResultSet, colCount: Int) {
        val row = (1 until colCount).map{rs.getObject(it).toString()}.joinToString(separator=ccp.delimiter)
        FileUtils.writeStringToFile(row, ccp.targetName, ccp.extension)
    }
}

class CsvDupePersistor(config: Map<String, String>): CsvPersistor(config), DupePersistor {
    override fun createDupe() {
        val columns = setOf("row_id","dupe_values")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter)
    }
    override fun writeDupe(rowId: Long, dupes: String) {
        
    }
}

class SqlTargetPersistor(val targetName: String, val conn: Connection): TargetPersistor {
    override fun createTarget(rsmd: ResultSetMetaData) {
        val ddl = SqlUtils.generateDDL(targetName, rsmd)
        SqlUtils.executeDDL(conn, ddl.createStatement)
    }
    override fun writeRow(rs: ResultSet, colCount: Int) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class SqlDupePersistor(val conn: Connection): DupePersistor {
    override fun createDupe() {
        val sql = "CREATE TABLE dupes(row_id BIGINT NOT NULL, dupe_values VARCHAR(MAX) NOT NULL)"
        SqlUtils.executeDDL(conn, sql)
    }

    override fun writeDupe(rowId: Long, dupes: String) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}