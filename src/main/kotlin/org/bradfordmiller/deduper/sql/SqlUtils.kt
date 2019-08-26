package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSetMetaData

class SqlUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(javaClass)

        fun getColumnsFromRs(rsmd: ResultSetMetaData): Map<Int, String> {

            val colCount = rsmd.columnCount

            val rsColumns = (1 until colCount).toList().map {i ->
                i to rsmd.getColumnName(i)
            }.toMap()

            return rsColumns
        }

        private fun getColumnsCommaDelimited(rsmd: ResultSetMetaData): String {
            val colCount = rsmd.columnCount
            val columnsComma = (1 until colCount).map { c ->

                val colName = rsmd.getColumnName(c)
                val type = rsmd.getColumnType(c)
                val typeName = JDBCType.valueOf(type).name
                val size = rsmd.getColumnDisplaySize(c)

                colName + " " +
                        if (type == java.sql.Types.VARCHAR) {
                            typeName + " (" + size.toString() + ")"
                        } else {
                            typeName
                        }
            }.joinToString(",")

            return columnsComma
        }

        fun generateInsert(tableName: String, rsmd: ResultSetMetaData): String {
            val colCount = rsmd.columnCount
            val insertClause = "INSERT INTO $tableName "
            val wildcards = (1 until colCount).map {"?"}.joinToString(",")
            val columnsComma = getColumnsCommaDelimited(rsmd)
            val insertFinal = "$insertClause ($columnsComma) VALUES ($wildcards)"
            return insertFinal
        }

        fun generateDDL(tableName: String, rsmd: ResultSetMetaData): String {
            val ctasClause = "CREATE TABLE $tableName AS "
            val columnsComma = getColumnsCommaDelimited(rsmd)
            val ctasFinal = ctasClause + columnsComma
            return ctasFinal
        }

        fun executeDDL(conn: Connection, ddl: String) {
            conn.createStatement().use {stmt ->
                stmt.executeUpdate(ddl)
            }
        }
    }
}