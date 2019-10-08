import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.config.HashSourceJndi
import org.bradfordmiller.deduper.config.SourceJndi
import org.bradfordmiller.deduper.jndi.CsvJNDITargetType
import org.bradfordmiller.deduper.jndi.SqlJNDIDupeType
import org.bradfordmiller.deduper.jndi.SqlJNDIHashType
import org.bradfordmiller.deduper.jndi.SqlJNDITargetType
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files

class DeduperTest {

    companion object {
        private val logger = LoggerFactory.getLogger(DeduperTest::class.java)
        private val dataDir = "src/test/resources/data/outputData"

        fun clearDataDir() {
            logger.info("Cleaning the output directory $dataDir")

            val dir = File(dataDir)
            val files = dir.listFiles()
            files.forEach {
                if(!it.isHidden)
                    Files.delete(it.toPath())
            }
        }
        @BeforeAll
        @JvmStatic
        fun cleanUpBefore() {
            clearDataDir()
        }
    }

    @Test fun dedupeCsvTest() {

        val csvTargetJndi = CsvJNDITargetType("RealEstateOut", false)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupes", false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi(csvTargetJndi)
            .dupesJndi(csvDupesJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }
    @Test fun dedupeSqlTest() {

        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", false,"real_estate")
        val sqlDupeJndi = SqlJNDIDupeType("SqlLiteTest", true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupeJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testCsvTargetWithDefaults() {

        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", false)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi(csvTargetJndi)
            .dupesJndi(csvDupesJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testRunWithoutTarget() {

        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testRunWithoutTargetAndDupe() {

        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteTarget() {

        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", true)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteDupe() {

        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", false)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteTargetAndDupe() {

        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", true)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteTarget() {

        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteDupe() {

        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteDupeAndTarget() {

        val sqlTargetJndi = SqlJNDITargetType("PostGresTest", true,"target_data", 20)
        val sqlDupesJndi = SqlJNDIDupeType("PostGresTest", true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        var build = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street", "city", "state", "zip", "price"))
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .build()

        val deduper = Deduper(build)

        deduper.dedupe()
    }

    @Test fun testHashPersistor() {
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", true, true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupesJndi)
            .hashJndi(sqlHashJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testHashPersistorNoJson() {
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", false, true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "Sacramentorealestatetransactions")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupesJndi)
            .hashJndi(sqlHashJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testNullsInSource() {
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", true, true)
        val sqlSourceJndi = SourceJndi("SqliteChinook", "tracks")

        val config = Config.ConfigBuilder()
                .sourceJndi(sqlSourceJndi)
                .jndiContext("default_ds")
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .hashJndi(sqlHashJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testSourceHashTable() {

        val sqlSourceJndi = SourceJndi("SqlLiteTest", "real_estate")
        val sqlHashSourceJndi = HashSourceJndi("SqlLiteTest", "hashes", "hash")

        val config = Config.ConfigBuilder()
            .sourceJndi(sqlSourceJndi)
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .seenHashesJndi(sqlHashSourceJndi)
            .jndiContext("default_ds")
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testCsvTargetCreation() {

        /*val hash = "7328393ce354e4b1b574d2d532ea3625".toUpperCase()
        val tgtName = "/tmp/targetName"

        Deduper().dedupe(
                "RealEstateIn",
                "Sacramentorealestatetransactions",
                "default_ds",
                "RealEstateOut",
                tgtName,
            "RealEstateOutDupes",
                mutableSetOf("street", "city", "state", "zip", "price")
        )

        val md5 = Files.newInputStream(Paths.get("$tgtName.txt")).use {
            org.apache.commons.codec.digest.DigestUtils.md5Hex(it)
        }.toUpperCase()

        assert(md5 == hash)*/
    }
}