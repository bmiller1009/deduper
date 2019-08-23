package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData

data class SqlDDL(val createStatement: String, val insertStatement: String)

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

        fun generateDDL(tableName: String, rsmd: ResultSetMetaData): SqlDDL {

            val ctasClause = "CREATE TABLE $tableName AS "
            val insertClause = "INSERT INTO $tableName "

            val colCount = rsmd.columnCount
            val wildcards = (1 until colCount).map {"?"}.joinToString(",")

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

            val ctasFinal = ctasClause + columnsComma
            val insertFinal = "$insertClause ($columnsComma) VALUES ($wildcards)"

            return SqlDDL(ctasFinal, insertFinal)
        }

        fun executeDDL(conn: Connection, ddl: String) {
            conn.createStatement().use {stmt ->
                stmt.executeUpdate(ddl)
            }
        }

        fun writeDataToTarget(conn: Connection, insertSql: String, data: Array<Map<String, Object>>) {

        }
    }
}