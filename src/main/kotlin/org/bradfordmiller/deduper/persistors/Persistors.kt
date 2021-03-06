package org.bradfordmiller.deduper.persistors

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.simplejndiutils.JNDIUtils
import org.bradfordmiller.sqlutils.QueryInfo
import org.bradfordmiller.sqlutils.SqlUtils
import org.bradfordmiller.sqlutils.SqlVendorTypes
import org.bradfordmiller.deduper.utils.FileUtils
import org.json.JSONArray
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.SQLException

/**
 * represents a simple duplicate value found by deduper.
 *
 * @property firstFoundRowNumber row location of the value that this data is a duplicate of
 * @property dupes json representation of the duplicate data row
 */
data class Dupe(val firstFoundRowNumber: Long, val dupes: String)
/**
 * represents hashed data created by deduper
 *
 * @property hash a hash value of a row found by deduper
 * @property hash_json an optional json representation of the data which comprises the hash value
 */
data class HashRow(val hash: String, val hash_json: String?)

interface BasePersistor {
    val deleteIfExists: Boolean
}

/**
 * base definition for writing out output data
 * @param T type of row list persisting output data
 */
interface WritePersistor<T>: BasePersistor {
    /**
     *  writes list of data contained in [rows] to output and returns the number of rows written
     */
    fun writeRows(rows: MutableList<T>): Long
}
/**
 * definition for writing out deduped data to a target flat file or sql table
 */
interface TargetPersistor: WritePersistor<Map<String, Any>> {
    /**
     * writes out a list of key/value pairs contained in [rows]. The key is the column name, and the value is the data
     * value for the column
     */
    override fun writeRows(rows: MutableList<Map<String, Any>>): Long
    /**
     *  writes out target file or sql table based on [qi]. [deleteIfTargetExists] determines if the target is deleted
     *  before populating it with data
     */
    fun createTarget(qi: QueryInfo)
}
/**
 *  definition for writing out duplicate data to a target flat file or sql table
 */
interface DupePersistor: WritePersistor<Pair<String, Pair<MutableList<Long>, Dupe>>> {
    /**
     * writes out a list of duplicate data contained in [rows].
     */
    override fun writeRows(rows: MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>): Long
    /**
     * creates the duplicate file or sql table. [deleteIfDupeExists] determines if the duplicate file/table is
     * deleted/dropped before creation.
     */
    fun createDupe()
}
/**
 *  definition for writing out hash values of rows found in source data
 */
interface HashPersistor: WritePersistor<HashRow> {
    /**
     * writes out list of hash values contained in [rows]
     */
    override fun writeRows(rows: MutableList<HashRow>): Long
    /**
     * creates the hash file or sql table. [deleteIfDupeExists] determines if the hash file/table is deleted/dropped
     * before creation.
     */
    fun createHashTable()
}
/**
 *  parser for jndi entries which are configured for csv output. Parses the values found in [config]
 */
open class CsvPersistor(config: Map<String, String>) {

    companion object {
        val logger = LoggerFactory.getLogger(CsvPersistor::class.java)
    }

    /**
     * represents a lock file for a csv file
     *
     * @property name name of the lock file
     * @property path path of the file to be locked
     * @property file the lock file
     */
    data class LockFile(val name: String, val path: String, val file: File)

    val ccp = CsvConfigParser(config)

    /**
     * returns a [LockFile]
     */
    private fun getLockFile(): LockFile {
        val f = File("${ccp.targetName}.${ccp.extension}")
        val path = f.parent
        val name = f.name
        val lockFileName = "$path/.LOCK_$name"
        logger.info("Lock file established: $lockFileName")
        return LockFile(name, path, File(lockFileName))
    }

    /**
     *  Locks a file based on a [LockFile] definition
     *
     *  @throws IllegalAccessError if an attempt is made to lock a file which is already locked
     */
    internal fun lockFile() {
        logger.info("Locking target file.")
        val lockFile = getLockFile()
        if(lockFile.file.exists()) {
            val message = "${lockFile.name} at path ${lockFile.path} is currently locked and cannot be written to."
            logger.error(message)
            throw IllegalAccessError(message)
        } else {
            lockFile.file.createNewFile()
            logger.info("Lock file created.")
        }
    }

    /**
     * Unlocks a file by removing it's associated lock file
     */
    fun unlockFile() {
        val lockFile = getLockFile()
        lockFile.file.delete()
    }
}
/**
 *  create and writes out "deduped" data to csv target. target is configured in [config]
 */
class CsvTargetPersistor(config: Map<String, String>, val deleteIfTargetExists: Boolean): CsvPersistor(config), TargetPersistor {
    /**
     * creates the target csv file based on the metadata found in [qi]. [deleteIfTargetExists] determines whether the
     * target csv file is deleted if it already exists before creating.
     */
    override val deleteIfExists = deleteIfTargetExists

    override fun createTarget(qi: QueryInfo) {
        lockFile()
        val columns = SqlUtils.getColumnsFromRs(qi)
        FileUtils.prepFile(ccp.targetName, columns.values.toSet(), ccp.extension, ccp.delimiter, deleteIfExists)
    }
    /**
     * writes out a list of key/value pairs contained in [rows] to a csv. The key is the column name, and the value is
     * the data value for the column. Returns the number of rows written.
     */
    override fun writeRows(rows: MutableList<Map<String, Any>>): Long {
        logger.info("Writing ${rows.size} rows to ${ccp.targetName}")
        val copyRows = mutableListOf<Map<String, Any>>()
        copyRows.addAll(rows)
        val data = copyRows.map {r ->
            r.values.map {v ->
                if(v != null)
                    v.toString()
                else
                    ""
            }.toTypedArray()
        }.toTypedArray()
        FileUtils.writeStringsToFile(data, ccp.targetName, ccp.extension, ccp.delimiter)
        logger.info("Writing complete.")
        return data.size.toLong()
    }
}
/**
 *  creates and writes out duplicate data to csv target. duplicate target is configured in [config]
 */
class CsvDupePersistor(config: Map<String, String>, deleteIfDupeExists: Boolean): CsvPersistor(config), DupePersistor {

    override val deleteIfExists = deleteIfDupeExists
    /**
     * creates duplicate output csv file. [deleteIfDupeExists] determines whether the file is deleted if it already
     * exists
     */
    override fun createDupe() {
        lockFile()
        val columns = setOf("hash", "row_ids", "first_found_row_number", "dupe_values")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter, deleteIfExists)
    }
    /**
     * writes out a list of duplicate data [rows] to a csv and returns the number of rows written
     */
    //TODO: Clean this up.  The pair syntax second.second.blah is clunky and hard to read
    override fun writeRows(rows: MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>): Long {
        val data = rows.map {
            val list = it.second.first
            val json = JSONArray(list).toString()
            arrayOf(it.first, json, it.second.second.firstFoundRowNumber.toString(), it.second.second.dupes)
        }.toTypedArray()
        FileUtils.writeStringsToFile(data, ccp.targetName, ccp.extension, ccp.delimiter)
        return data.size.toLong()
    }
}
/**
 * creates and writes out hash values found in a deduper process to a csv defined in [config]
 */
class CsvHashPersistor(config: Map<String, String>, deleteIfHashExists: Boolean): CsvPersistor(config), HashPersistor {

    override val deleteIfExists = deleteIfHashExists
    /**
     * creates a hash output csv file. [deleteIfDupeExists] determines whether the file is deleted if it already
     */
    override fun createHashTable() {
        lockFile()
        val columns = setOf("hash", "json_row")
        FileUtils.prepFile(ccp.targetName, columns, ccp.extension, ccp.delimiter, deleteIfExists)
    }
    /**
     * writes out a list of hash data [rows] to a csv and returns the number of rows written
     */
    override fun writeRows(rows: MutableList<HashRow>): Long {
        val data = rows.map {hr ->
            arrayOf(hr.hash, hr.hash_json.orEmpty())
        }.toTypedArray()
        FileUtils.writeStringsToFile(data, ccp.targetName, ccp.extension, ccp.delimiter)
        return data.size.toLong()
    }
}

/**
 * create and writes out "deduped" data to a sql table. [targetName] is the table name in the [javax.sql.DataSource]
 * configured in the [targetJndi] for the associated [context].  [varcharPadding] is a number of extra bytes which can
 * be configured if the target needs larger varchar fields than were extracted by the source.
 */
class SqlTargetPersistor(
    private val targetName: String,
    private val targetJndi: String,
    private val context: String,
    private val varcharPadding: Int,
    deleteIfTargetExists: Boolean
): TargetPersistor {

    companion object {
        val logger = LoggerFactory.getLogger(SqlTargetPersistor::class.java)
    }

    override val deleteIfExists = deleteIfTargetExists

    private val dbInfo by lazy {
        val sql = "SELECT * FROM $targetName"
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            val qi = SqlUtils.getQueryInfo(sql, conn)
            Pair(
                SqlUtils.generateInsert(targetName, qi, conn.metaData.databaseProductName),
                SqlUtils.getColumnIdxFromRs(qi)
            )
        }
    }
    /**
     * creates a target sql table based on the [qi] found in the source. [deleteIfTargetExists] will drop the table if
     * it already exists before attempting to create it
     */
    override fun createTarget(qi: QueryInfo) {
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            if (deleteIfExists) {
                logger.info(
                    "deleteIfTargetExists is set to true.  Checking database to see if target $targetName exists."
                )
                SqlUtils.deleteTableIfExists(conn, targetName)
            }
            val vendor = conn.metaData.databaseProductName
            val ddl = SqlUtils.generateDDL(targetName, qi, vendor, varcharPadding)
            SqlUtils.executeDDL(conn, ddl)
        }
    }
    /**
     * writes out a list of data [rows] to a a sql table and returns the number of rows written
     */
    override fun writeRows(rows: MutableList<Map<String, Any>>): Long {
        val sql = dbInfo.first
        val columnMap = dbInfo.second
        JNDIUtils.getJndiConnection(targetJndi, context).use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(sql).use {pstmt ->
                try {
                    rows.forEach { kvp ->
                        kvp.forEach {
                            val idx = columnMap[it.key]!!
                            pstmt.setObject(idx, it.value)
                        }
                        pstmt.addBatch()
                    }
                    try {
                        pstmt.executeBatch()
                    } catch(sqlEx: SQLException) {
                        logger.error("Error committing batch: ${sqlEx.message}")
                        throw sqlEx
                    }
                    conn.commit()
                    return rows.size.toLong()
                } catch (sqlEx: SQLException) {
                    logger.error("Error while inserting data: ${sqlEx.message}")
                    conn.rollback()
                    throw sqlEx
                }
            }
        }
    }
}
/**
 * creates a sql table for persisting duplicate data. This is configured using the [dupesJndi] [javax.sql.DataSource]
 * contained in the associated [context]
 */
class SqlDupePersistor(private val dupesJndi: String, private val context: String, deleteIfDupeExists: Boolean): DupePersistor {

    companion object {
        val logger = LoggerFactory.getLogger(SqlDupePersistor::class.java)
    }
    override val deleteIfExists = deleteIfDupeExists

    //TODO: Make a list of dupe columns and then pass it to both the INSERT and CREATE statements
    private val insertStatement =
        "INSERT INTO dupes(hash, row_ids, first_found_row_number, dupe_values) VALUES (?,?,?,?)"
    /**
     * creates a sql table for persisting duplicates found in a deduper process. [deleteIfDupeExists] will drop the
     * table if it already exists before attempting to create it
     */
    override fun createDupe() {
        JNDIUtils.getJndiConnection(dupesJndi, context).use { conn ->

            if(deleteIfExists) {
                logger.info("deleteIfDupeExists is set to true. Checking to see if table 'dupes' exists.")
                SqlUtils.deleteTableIfExists(conn, "dupes")
            }

            val vendor = conn.metaData.databaseProductName
            val sqlVendorTypes = SqlVendorTypes(vendor)
            val createStatement =
                "CREATE TABLE dupes(" +
                 "hash ${sqlVendorTypes.getStringType()} NOT NULL, " +
                 "row_ids ${sqlVendorTypes.getStringType()} NOT NULL, " +
                 "first_found_row_number ${sqlVendorTypes.getLongType()} NOT NULL, " +
                 "dupe_values ${sqlVendorTypes.getStringType()} ${sqlVendorTypes.getStringMaxSize()} NOT NULL," +
                 "${sqlVendorTypes.getPrimaryKeySyntax("hash")}" +
                 ")"
            SqlUtils.executeDDL(conn, createStatement)
        }
    }
    /**
     * writes out a list of duplicate data [rows] to a a sql table and returns the number of rows written
     */
    //TODO: Clean this up.  The pair syntax second.second.blah is clunky and hard to read
    override fun writeRows(rows: MutableList<Pair<String, Pair<MutableList<Long>, Dupe>>>): Long {
        JNDIUtils.getJndiConnection(dupesJndi, context).use {conn ->
            conn.autoCommit = false
            conn.prepareStatement(insertStatement).use {pstmt ->
                try {
                    rows.forEach {
                        pstmt.setString(1, it.first)
                        pstmt.setString(2, JSONArray(it.second.first).toString())
                        pstmt.setLong(3, it.second.second.firstFoundRowNumber)
                        pstmt.setString(4, it.second.second.dupes)
                        pstmt.addBatch()
                    }
                    try {
                        pstmt.executeBatch()
                    } catch(sqlEx: SQLException) {
                        logger.error("Error committing batch: ${sqlEx.message}")
                        throw sqlEx
                    }
                    conn.commit()
                    return rows.size.toLong()
                } catch (sqlEx: SQLException) {
                    logger.error("Error while inserting duplicate values: ${sqlEx.message}")
                    conn.rollback()
                    throw sqlEx
                }
            }
        }
    }
}
/**
 * creates a sql table for persisting hashed data rows. This is configured using the [hashJndi] [javax.sql.DataSource]
 * contained in the associated [context]
 */
class SqlHashPersistor(private val hashJndi: String, private val context: String, deleteIfHashTableExists: Boolean): HashPersistor {

    companion object {
        val logger = LoggerFactory.getLogger(SqlHashPersistor::class.java)
    }

    override val deleteIfExists = deleteIfHashTableExists

    private val insertStatement = "INSERT INTO hashes(hash, json_row) VALUES (?,?)"
    /**
     * creates a sql table for persisting hash rows found in a deduper process. [deleteIfHashTableExists] will drop the
     * table if it already exists before attempting to create it
     */
    override fun createHashTable() {
        JNDIUtils.getJndiConnection(hashJndi, context).use { conn ->

            if (deleteIfExists) {
                logger.info("deleteIfHashTableExists is set to true. Checking to see if table 'hashes' exists.")
                SqlUtils.deleteTableIfExists(conn, "hashes")
            }

            //TODO - extract this into a createTableMethod in SqlUtils
            val vendor = conn.metaData.databaseProductName
            val sqlVendorTypes = SqlVendorTypes(vendor)
            val createStatement =
                "CREATE TABLE hashes(" +
                        "hash ${sqlVendorTypes.getStringType()} NOT NULL, " +
                        "json_row ${sqlVendorTypes.getStringType()} NULL, " +
                        "${sqlVendorTypes.getPrimaryKeySyntax("hash")}" +
                        ")"
            SqlUtils.executeDDL(conn, createStatement)
        }
    }
    /**
     * writes out a list of duplicate data [rows] to a a sql table and returns number of rows written
     */
    override fun writeRows(rows: MutableList<HashRow>): Long {
        JNDIUtils.getJndiConnection(hashJndi, context).use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(insertStatement).use { pstmt ->
                try {
                    rows.forEach { hr ->
                        pstmt.setString(1, hr.hash)
                        pstmt.setString(2, hr.hash_json)
                        pstmt.addBatch()
                    }
                    try {
                        pstmt.executeBatch()
                    } catch (sqlEx: SQLException) {
                        logger.error("Error committing batch: ${sqlEx.message}")
                        throw sqlEx
                    }
                    conn.commit()
                    return rows.size.toLong()
                } catch (sqlEx: SQLException) {
                    logger.error("Error while inserting hash values: ${sqlEx.message}")
                    conn.rollback()
                    throw sqlEx
                }
            }
        }
    }
}