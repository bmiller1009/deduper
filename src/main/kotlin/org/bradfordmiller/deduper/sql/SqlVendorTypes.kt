package org.bradfordmiller.deduper.sql

class SqlVendorTypes(vendor: String) {
    private val formattedVendor = vendor.toLowerCase()
    fun getStringType(): String {
        return if(formattedVendor.contains("sqlite")) {
            "TEXT"
        } else {
            "VARCHAR"
        }
    }
    fun getStringMaxSize(): String {
        return if(formattedVendor.toLowerCase().contains("sqlite")) {
            ""
        } else {
            "MAX"
        }
    }
    fun getStringSize(size: Int): String {
        return if(formattedVendor.toLowerCase().contains("sqlite")) {
            ""
        } else {
            "(${size.toString()})"
        }
    }
    fun getLongType(): String {
        return if(formattedVendor.toLowerCase().contains("sqlite")) {
            "INTEGER"
        } else {
            "BIGINT"
        }
    }
    fun getDecimalType(type: String): String {
        return if(formattedVendor.toLowerCase().contains("sqlite")) {
            "REAL"
        } else {
            type
        }
    }
}