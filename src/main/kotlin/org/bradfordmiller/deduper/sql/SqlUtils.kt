package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSetMetaData

class SqlUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(javaClass::class.java)

        fun getColumnsFromRs(rsmd: ResultSetMetaData): Map<Int, String> {
            val colCount = rsmd.columnCount

            return (1..colCount).toList().map {i ->
                i to rsmd.getColumnName(i)
            }.toMap()
        }
        fun getColumnIdxFromRs(rsmd: ResultSetMetaData): Map<String, Int> {
            val colCount = rsmd.columnCount

            return (1..colCount).toList().map {i ->
                rsmd.getColumnName(i) to i
            }.toMap()
        }
        private fun getType(vendor: String, typeName: String, type: Int, size: Int): String {
            val sqlVendorTypes = SqlVendorTypes(vendor)
            return if (type == java.sql.Types.VARCHAR) {
                sqlVendorTypes.getStringType() + sqlVendorTypes.getStringSize(size)
            } else if(type == java.sql.Types.BIGINT) {
                sqlVendorTypes.getLongType()
            } else if(type == java.sql.Types.DOUBLE || type == java.sql.Types.DECIMAL || type == java.sql.Types.FLOAT ) {
                sqlVendorTypes.getDecimalType(typeName)
            } else {
                typeName
            }
        }
        private fun getColumnsCommaDelimited(rsmd: ResultSetMetaData, vendor: String, includeType: Boolean = false): String {
            val colCount = rsmd.columnCount
            return (1..colCount).map { c ->
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
                "$colName $sqlType"
            }.joinToString(",")
        }
        fun generateInsert(tableName: String, rsmd: ResultSetMetaData, vendor: String): String {
            val colCount = rsmd.columnCount
            val insertClause = "INSERT INTO $tableName "
            val wildcards = (1..colCount).map {"?"}.joinToString(",")
            val columnsComma = getColumnsCommaDelimited(rsmd, vendor)
            return "$insertClause ($columnsComma) VALUES ($wildcards)"
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