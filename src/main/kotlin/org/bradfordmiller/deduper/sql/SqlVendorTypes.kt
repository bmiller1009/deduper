package org.bradfordmiller.deduper.sql

/**
 * class for returning correctly formatted types based on a specific database [vendor]
 */
class SqlVendorTypes(vendor: String) {

    private val formattedVendor = vendor.toLowerCase()
    /**
     * returns a specific string type (IE TEXT, VARCHAR, etc) base on database vendor
     */
    fun getStringType(): String {
        return if(formattedVendor.contains("sqlite")) {
            "TEXT"
        } else {
            "VARCHAR"
        }
    }
    /**
     * returns a specific string type (IE MAX) base on database vendor
     */
    fun getStringMaxSize(): String {
        return if(formattedVendor.contains("sqlite") || formattedVendor.contains("postgres")) {
            ""
        } else {
            "MAX"
        }
    }
    /**
     * returns the formatted [size] of a string
     */
    fun getStringSize(size: Int): String {
        return if(formattedVendor.contains("sqlite")) {
            ""
        } else {
            "(${size.toString()})"
        }
    }
    /**
     *  returns the formatted long type based on database vendor
     */
    fun getLongType(): String {
        return if(formattedVendor.contains("sqlite")) {
            "INTEGER"
        } else {
            "BIGINT"
        }
    }
    /**
     * returns the formatted decimal [type] based on database vendor
     */
    fun getDecimalType(type: String): String {
        return if(formattedVendor.contains("sqlite")) {
            "REAL"
        } else {
            type
        }
    }
    /**
     * returns correct DDL for primary key based on vendor and [column] name
     */
    fun getPrimaryKeySyntax(column: String): String {
        return if(formattedVendor.contains("sqlite")) {
            "PRIMARY KEY($column)"
        } else {
            "CONSTRAINT PK_$column PRIMARY KEY ($column)"
        }
    }
}