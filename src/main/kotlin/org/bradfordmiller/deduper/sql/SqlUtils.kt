package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.*

class SqlUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(SqlUtils::class.java)

        fun getMapFromRs(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
            return (1..colNames.size).map{
                val column = colNames[it] ?: error("Column Index $it does not have an entry in the column name map.")
                column to rs.getObject(column)
            }.toMap()
        }
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
            val insertSql = "$insertClause ($columnsComma) VALUES ($wildcards)"
            logger.trace("Insert SQL $insertSql has been generated.")
            return insertSql
        }
        fun generateDDL(tableName: String, rsmd: ResultSetMetaData, vendor: String): String {
            val ctClause = "CREATE TABLE $tableName "
            val columnsComma = getColumnsCommaDelimited(rsmd, vendor, true)
            val ddl = "$ctClause ($columnsComma)"
            logger.trace("DDL $ddl has been generated.")
            return ddl
        }
        fun executeDDL(conn: Connection, ddl: String) {
            conn.createStatement().use {stmt ->
                logger.trace("Executing the following ddl SQL: $ddl")
                stmt.executeUpdate(ddl)
            }
        }
        fun tableExists(dbmd: DatabaseMetaData, tableName: String): Boolean {
            val rs = dbmd.getTables(null, null, tableName, null)
            return rs.next()
        }
    }
}