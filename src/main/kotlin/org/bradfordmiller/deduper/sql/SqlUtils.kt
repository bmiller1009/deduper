package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSetMetaData

class SqlUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(javaClass)

        fun getStringType(vendor: String): String {
            return if(vendor.toLowerCase().contains("sqlite")) {
                "TEXT"
            } else {
                "VARCHAR"
            }
        }

        fun getStringMaxSize(vendor: String): String {
            return if(vendor.toLowerCase().contains("sqlite")) {
                ""
            } else {
                "MAX"
            }
        }

        fun getStringSize(vendor: String, size: Int): String {
            return if(vendor.toLowerCase().contains("sqlite")) {
                ""
            } else {
                "(${size.toString()})"
            }
        }

        fun getLongType(vendor: String): String {
            return if(vendor.toLowerCase().contains("sqlite")) {
                "INTEGER"
            } else {
                "BIGINT"
            }
        }

        fun getDecimalType(vendor: String, type: String): String {
            return if(vendor.toLowerCase().contains("sqlite")) {
                "REAL"
            } else {
                type
            }
        }

        fun getColumnsFromRs(rsmd: ResultSetMetaData): Map<Int, String> {

            val colCount = rsmd.columnCount

            val rsColumns = (1 until colCount).toList().map {i ->
                i to rsmd.getColumnName(i)
            }.toMap()

            return rsColumns
        }

        fun getColumnIdxFromRs(rsmd: ResultSetMetaData): Map<String, Int> {
            val colCount = rsmd.columnCount

            val rsColumns = (1 until colCount).toList().map {i ->
                rsmd.getColumnName(i) to i
            }.toMap()

            return rsColumns
        }

        private fun getType(vendor: String, typeName: String, type: Int, size: Int): String {
            return if (type == java.sql.Types.VARCHAR) {
                getStringType(vendor) + getStringSize(vendor, size)
            } else if(type == java.sql.Types.BIGINT) {
                getLongType(vendor)
            } else if(type == java.sql.Types.DOUBLE || type == java.sql.Types.DECIMAL || type == java.sql.Types.FLOAT ) {
                getDecimalType(vendor, typeName)
            } else {
                typeName
            }
        }

        private fun getColumnsCommaDelimited(rsmd: ResultSetMetaData, vendor: String, includeType: Boolean = false): String {
            val colCount = rsmd.columnCount
            val columnsComma = (1 until colCount).map { c ->

                val colName = rsmd.getColumnName(c)
                val type = rsmd.getColumnType(c)
                val typeName = JDBCType.valueOf(type).name
                val size = rsmd.getColumnDisplaySize(c)
                val sqlType =
                    if(includeType) {
                        getType(vendor, typeName, type, size)
                    } else {
                        ""
                    }
                colName + " " + sqlType
            }.joinToString(",")

            return columnsComma
        }

        fun generateInsert(tableName: String, rsmd: ResultSetMetaData, vendor: String): String {
            val colCount = rsmd.columnCount
            val insertClause = "INSERT INTO $tableName "
            val wildcards = (1 until colCount).map {"?"}.joinToString(",")
            val columnsComma = getColumnsCommaDelimited(rsmd, vendor)
            val insertFinal = "$insertClause ($columnsComma) VALUES ($wildcards)"
            return insertFinal
        }

        fun generateDDL(tableName: String, rsmd: ResultSetMetaData, vendor: String): String {
            val ctClause = "CREATE TABLE $tableName "
            val columnsComma = getColumnsCommaDelimited(rsmd, vendor, true)
            return "$ctClause ($columnsComma)"
        }

        fun executeDDL(conn: Connection, ddl: String) {
            conn.createStatement().use {stmt ->
                stmt.executeUpdate(ddl)
            }
        }
    }
}