package org.bradfordmiller.deduper.hashing

/**
 * Utility class for hashing methods
 */
class Hasher {
    companion object {
        /**
         * returns a long value representation of [hash]
         */
        fun hashString(hash: String): Long {
            return (hash.reversed().hashCode().toLong() shl 32) or (hash.hashCode().toLong() and 4294967295L)
        }
    }
}