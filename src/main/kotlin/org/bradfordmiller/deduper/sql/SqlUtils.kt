package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.*

/**
 * Sql operations utility class
 */
class SqlUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(SqlUtils::class.java)
        /**
         * returns a map of column names and associated values from rs and list of column names contained in [colNames]
         */
        fun getMapFromRs(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
            return (1..colNames.size).map{
                val column = colNames[it] ?: error("Column Index $it does not have an entry in the column name map.")
                column to rs.getObject(column)
            }.toMap()
        }
        /**
         *  returns column list with index and column name from [rsmd]
         */
        fun getColumnsFromRs(rsmd: ResultSetMetaData): Map<Int, String> {
            val colCount = rsmd.columnCount

            return (1..colCount).toList().map {i ->
                i to rsmd.getColumnName(i)
            }.toMap()
        }
        /**
         * returns a list of columns and their indices from [rsmd]
         */
        fun getColumnIdxFromRs(rsmd: ResultSetMetaData): Map<String, Int> {
            val colCount = rsmd.columnCount

            return (1..colCount).toList().map {i ->
                rsmd.getColumnName(i) to i
            }.toMap()
        }
        /**
         * returns stringified version of database column type based on [vendor], [typeName], [type], and [size]
         */
        private fun getType(vendor: String, typeName: String, type: Int, size: Int): String {
            val sqlVendorTypes = SqlVendorTypes(vendor)
            return if (type == Types.VARCHAR) {
                sqlVendorTypes.getStringType() + sqlVendorTypes.getStringSize(size)
            } else if(type == Types.BIGINT) {
                sqlVendorTypes.getLongType()
            } else if(type == Types.DOUBLE || type == Types.DECIMAL || type == Types.FLOAT ) {
                sqlVendorTypes.getDecimalType(typeName)
            } else {
                typeName
            }
        }
        /**
         * returns a stringified list of columns based on [rsmd], [vendor], [varcharPadding], [includeType], and
         * [includeNullability]
         */
        private fun getColumnsCommaDelimited(
            rsmd: ResultSetMetaData,
            vendor: String,
            varcharPadding: Int = 0,
            includeType: Boolean = false,
            includeNullability: Boolean = false
        ): String {
            val colCount = rsmd.columnCount
            return (1..colCount).map { c ->
                val colName = rsmd.getColumnName(c)
                val type = rsmd.getColumnType(c)
                val typeName = JDBCType.valueOf(type).name
                val size = rsmd.getColumnDisplaySize(c) + varcharPadding
                val isNull =
                    if(includeNullability) {
                        if (rsmd.isNullable(c) == ResultSetMetaData.columnNullable) {
                            "NULL"
                        } else {
                            "NOT NULL"
                        }
                    } else {
                        ""
                    }
                val sqlType =
                    if(includeType) {
                        getType(vendor, typeName, type, size)
                    } else {
                        ""
                    }
                "$colName $sqlType $isNull"
            }.joinToString(",")
        }
        /**
         * returns a string representing a parameterized INSERT sql statement based on the [tableName] which is the
         * target of the INSERT, [rsmd], and the database [vendor]
         */
        fun generateInsert(tableName: String, rsmd: ResultSetMetaData, vendor: String): String {
            val colCount = rsmd.columnCount
            val insertClause = "INSERT INTO $tableName "
            val wildcards = (1..colCount).map {"?"}.joinToString(",")
            val columnsComma = getColumnsCommaDelimited(rsmd, vendor)
            val insertSql = "$insertClause ($columnsComma) VALUES ($wildcards)"
            logger.trace("Insert SQL $insertSql has been generated.")
            return insertSql
        }
        /**
         * returns a CREATE TABLE sql DDL based on the [tableName], [rsmd], [vendor], and [varcharPadding]
         */
        fun generateDDL(tableName: String, rsmd: ResultSetMetaData, vendor: String, varcharPadding: Int): String {
            val ctClause = "CREATE TABLE $tableName "
            val columnsComma = getColumnsCommaDelimited(rsmd, vendor, varcharPadding, true)
            val ddl = "$ctClause ($columnsComma)"
            logger.trace("DDL $ddl has been generated.")
            return ddl
        }
        /**
         *  runs arbitrary [ddl] statements on a target [conn]
         */
        fun executeDDL(conn: Connection, ddl: String) {
            conn.createStatement().use {stmt ->
                logger.trace("Executing the following ddl SQL: $ddl")
                stmt.executeUpdate(ddl)
            }
        }
        /**
         * returns true/false based on if a [tableName] exists in [dbmd]
         */
        fun tableExists(dbmd: DatabaseMetaData, tableName: String): Boolean {
            dbmd.getTables(null, null, tableName, null).use {rs ->
                val hasNext = rs.next()
                return hasNext
            }
        }
        /**
         *  deletes a [tableName] if it exists for a specific [conn]
         */
        fun deleteTableIfExists(conn: Connection, tableName: String) {
            if(tableExists(conn.metaData, tableName)) {
                logger.info("Table '$tableName' exists. Generating script to drop table")
                val dropSql = "Drop table $tableName"
                executeDDL(conn, dropSql)
                logger.info("Table '$tableName' dropped")
            }
        }
        /**
         *  returns the string reprsentation of a row in [rs] for a given [columnList]
         */
        fun stringifyRow(rs: ResultSet, columnList: MutableSet<String> = mutableSetOf()): String {
            val rsmd = rs.metaData
            val colCount = rsmd.columnCount
            return if (columnList.isNotEmpty()) {
                columnList.map { rs.getString(it) }.joinToString()
            } else {
                (1..colCount).toList().map { rs.getString(it) }.joinToString()
            }
        }
    }
}