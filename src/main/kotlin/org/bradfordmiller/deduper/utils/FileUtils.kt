package org.bradfordmiller.deduper.utils

import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.file.FileSystemException

class FileUtils {
    companion object {
        fun prepFile(targetName: String, columns: Set<String>, extension: String, delimiter: String) {
            val fileName = targetName + "." + extension

            val f = File("$fileName")

            if(f.exists() && !f.isFile)
                throw FileSystemException("tgt name $fileName is not a file")

            BufferedWriter(OutputStreamWriter(FileOutputStream(fileName), "utf-8")).use { bw ->
                bw.write(columns.joinToString(separator=delimiter))
            }
        }
    }
}