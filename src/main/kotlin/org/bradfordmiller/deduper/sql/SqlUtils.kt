package org.bradfordmiller.deduper.sql

import org.slf4j.LoggerFactory
import java.sql.*

/**
 * represents the metadata found in a particular SQL query
 *
 * @property columnIndex the positional index of a column within the SQL query
 * @property columnName the name of the column
 * @property columnType the integer representation of the data type of the column
 * @property columnType the stringified representation of the data type of the column
 * @property columnDisplaySize the size of the column
 * @property isColumnNull the nullability of a column
 */
data class QueryMetadata(
  val columnIndex: Int,
  val columnName: String,
  val columnType: Int,
  val columnTypeName: String,
  val columnDisplaySize: Int,
  val isColumnNull: Boolean
)

/**
 * the metadata about a particular SQL query comprised of the [columnCount] and the [columnSet] of [QueryInfo]
 */
data class QueryInfo(
  val columnCount: Int,
  val columnSet: Set<QueryMetadata>
)

/**
 * Sql operations utility class
 */
class SqlUtils {

    companion object {

        private val logger = LoggerFactory.getLogger(SqlUtils::class.java)
        /**
         * returns a map of column names and associated values from [rs] and list of column names contained in [colNames]
         */
        fun getMapFromRs(rs: ResultSet, colNames: Map<Int, String>): Map<String, Any> {
            return (1..colNames.size).map{
                val column = colNames[it] ?: error("Column Index $it does not have an entry in the column name map.")
                column to rs.getObject(column)
            }.toMap()
        }
        /**
         *  returns column list with index and column name from [qi]
         */
        fun getColumnsFromRs(qi: QueryInfo): Map<Int, String> {
            return qi.columnSet.map {qm ->
                qm.columnIndex to qm.columnName
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
         * returns a list of columns and their indices from [qi]
         */
        //TODO: This should really be called off of the QueryInfo object
        fun getColumnIdxFromRs(qi: QueryInfo): Map<String, Int> {
            return qi.columnSet.map {qm ->
                qm.columnName to qm.columnIndex
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
         * returns a stringified list of columns based on [queryInfo], [vendor], [varcharPadding], [includeType], and
         * [includeNullability]
         */
        private fun getColumnsCommaDelimited(
            queryInfo: QueryInfo,
            vendor: String,
            varcharPadding: Int = 0,
            includeType: Boolean = false,
            includeNullability: Boolean = false
        ): String {
            return queryInfo.columnSet.map { qi ->
                val colName = qi.columnName
                val typeName = qi.columnTypeName
                val size = qi.columnDisplaySize + varcharPadding
                val isNull =
                    if(includeNullability) {
                        if (qi.isColumnNull) {
                            "NULL"
                        } else {
                            "NOT NULL"
                        }
                    } else {
                        ""
                    }
                val sqlType =
                    if(includeType) {
                        getType(vendor, typeName, qi.columnType, size)
                    } else {
                        ""
                    }
                "$colName $sqlType $isNull"
            }.joinToString(",")
        }
        /**
         * returns a string representing a parameterized INSERT sql statement based on the [tableName] which is the
         * target of the INSERT, [qi], and the database [vendor]
         */
        fun generateInsert(tableName: String, qi: QueryInfo, vendor: String): String {
            val insertClause = "INSERT INTO $tableName "
            val wildcards = (1..qi.columnCount).map {"?"}.joinToString(",")
            val columnsComma = getColumnsCommaDelimited(qi, vendor)
            val insertSql = "$insertClause ($columnsComma) VALUES ($wildcards)"
            logger.trace("Insert SQL $insertSql has been generated.")
            return insertSql
        }
        /**
         * returns a CREATE TABLE sql DDL based on the [tableName], [qi], [vendor], and [varcharPadding]
         */
        fun generateDDL(tableName: String, qi: QueryInfo, vendor: String, varcharPadding: Int): String {
            val ctClause = "CREATE TABLE $tableName "
            val columnsComma = getColumnsCommaDelimited(qi, vendor, varcharPadding, true)
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
         *  returns the string representation of a row in [rs] for a given [columnList]
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

        /**
         * returns the [QueryInfo] of a [sql] query based on a specific [conn]
         */
        fun getQueryInfo(sql: String, conn: Connection): QueryInfo {
            logger.info("The following sql statement will be run: $sql")
            conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val rsmd = rs.metaData
                    val columnCount = rsmd.columnCount
                    val queryMetadata = (1..columnCount).map { i ->
                        val type = rsmd.getColumnType(i)
                        QueryMetadata(
                                i,
                                rsmd.getColumnName(i),
                                type,
                                JDBCType.valueOf(type).name,
                                rsmd.getColumnDisplaySize(i),
                                rsmd.isNullable(i) == ResultSetMetaData.columnNullable
                        )
                    }.toMutableSet()
                    return QueryInfo(columnCount, queryMetadata)
                }
            }
        }
    }
}