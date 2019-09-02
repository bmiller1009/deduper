package org.bradfordmiller.deduper.config

import org.apache.commons.lang.NullArgumentException
import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.jndi.JNDIUtils
import org.bradfordmiller.deduper.persistors.*
import org.bradfordmiller.deduper.utils.Left
import javax.sql.DataSource

enum class ConfigType {Sql, Csv}

class Config private constructor(
    srcJndi: String,
    srcName: String,
    context: String,
    val hashColumns: Set<String>,
    tgtJndi: String,
    dupesJndi: String,
    tgtTable: String?
) {

    internal data class Persistors(val targetPersistor: TargetPersistor, val dupePersistor: DupePersistor, val configType: ConfigType)

    private val persistors: Persistors by lazy {
        if(tgtTable.isNullOrEmpty()) {
            val tgtConfigMap = CsvConfigParser.getCsvMap(context, tgtJndi)
            val dupesConfigMap = CsvConfigParser.getCsvMap(context, dupesJndi)
            Persistors(CsvTargetPersistor(tgtConfigMap), CsvDupePersistor(dupesConfigMap), ConfigType.Csv)
        } else {
            Persistors(SqlTargetPersistor(tgtTable, tgtJndi, context), SqlDupePersistor(dupesJndi, context), ConfigType.Sql)
        }
    }

    val sourceDataSource: DataSource by lazy {(JNDIUtils.getDataSource(srcJndi, context) as Left<DataSource?, String>).left!!}

    val sqlStatement by lazy {
        if (srcName.startsWith("SELECT", true)) {
            srcName
        } else {
            "SELECT * FROM $srcName"
        }
    }

    fun getTargetPersistor() = persistors.targetPersistor
    fun getDuplicatePersistor() = persistors.dupePersistor
    fun getConfigType() = persistors.configType

    data class ConfigBuilder(
        private var srcJndi: String? = null,
        private var srcName: String? = null,
        private var context: String? = null,
        private var keyOn: Set<String>? = null,
        private var tgtJndi: String? = null,
        private var dupesJndi: String? = null,
        private var tgtTable: String? = null
    ) {
        fun sourceJndi(srcJndi: String) = apply { this.srcJndi = srcJndi }
        fun sourceName(srcName: String) = apply { this.srcName = srcName }
        fun jndiContext(context: String) = apply { this.context = context }
        fun hashColumns(hashColumns: Set<String>) = apply { this.keyOn = hashColumns }
        fun targetJndi(tgtJndi: String) = apply { this.tgtJndi = tgtJndi }
        fun dupesJndi(dupesJndi: String) = apply { this.dupesJndi = dupesJndi }
        fun targetTable(targetTable: String) = apply { this.tgtTable = targetTable }
        fun build(): Config {
            val sourceJndi = srcJndi ?: throw NullArgumentException("Source JNDI must be set")
            val sourceName = srcName ?: throw NullArgumentException("Source JNDI name must be set")
            val finalContext = context ?: throw NullArgumentException("JNDI context must be set")
            val targetJndi = tgtJndi ?: throw NullArgumentException("Target JNDI must be set")
            val finalDupesJndi = dupesJndi ?: throw NullArgumentException("Duplicates JNDI must be set")

            return Config(sourceJndi, sourceName, finalContext, keyOn.orEmpty(), targetJndi, finalDupesJndi, tgtTable)
        }
    }
}