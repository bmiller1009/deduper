package org.bradfordmiller.deduper.hashing

class Hasher {
    companion object {
        fun hashString(hash: String): Long {
            return (hash.reversed().hashCode().toLong() shl 32) or (hash.hashCode().toLong() and 4294967295L)
        }
    }
}