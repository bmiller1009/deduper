import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.config.Config
import org.junit.jupiter.api.AfterAll
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
        /*@AfterAll
        @JvmStatic
        fun cleanUpAfter() {
            clearDataDir()
        }*/
    }

    @Test fun dedupeCsvTest() {

        val config = Config.ConfigBuilder()
            .sourceJndi("RealEstateIn")
            .sourceName("Sacramentorealestatetransactions")
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi("RealEstateOut")
            .dupesJndi("RealEstateOutDupes")
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }
    @Test fun dedupeSqlTest() {

        val config = Config.ConfigBuilder()
            .sourceJndi("RealEstateIn")
            .sourceName("Sacramentorealestatetransactions")
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi("SqlLiteTest")
            .dupesJndi("SqlLiteTest")
            .targetTable("real_estate")
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testCsvTargetWithDefaults() {
        val config = Config.ConfigBuilder()
            .sourceJndi("RealEstateIn")
            .sourceName("Sacramentorealestatetransactions")
            .jndiContext("default_ds")
            .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
            .targetJndi("RealEstateOutTargetUseDefaults")
            .dupesJndi("RealEstateOutDupesUseDefaults")
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testRunWithoutTarget() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .dupesJndi("RealEstateOutDupesUseDefaults")
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testRunWithoutTargetAndDupe() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteTarget() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi("RealEstateOutTargetUseDefaults")
                .dupesJndi("RealEstateOutDupesUseDefaults")
                .deleteTargetIfExists(true)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteDupe() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi("RealEstateOutTargetUseDefaults")
                .dupesJndi("RealEstateOutDupesUseDefaults")
                .deleteDupeIfExists(true)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteTargetAndDupe() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi("RealEstateOutTargetUseDefaults")
                .dupesJndi("RealEstateOutDupesUseDefaults")
                .deleteTargetIfExists(true)
                .deleteDupeIfExists(true)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteTarget() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi("SqlLiteTest")
                .targetTable("target_data")
                .dupesJndi("SqlLiteTest")
                .deleteTargetIfExists(true)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteDupe() {
        val config = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
                .targetJndi("SqlLiteTest")
                .targetTable("target_data")
                .dupesJndi("SqlLiteTest")
                .deleteDupeIfExists(true)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteDupeAndTarget() {
        var build = Config.ConfigBuilder()
                .sourceJndi("RealEstateIn")
                .sourceName("Sacramentorealestatetransactions")
                .jndiContext("default_ds")
                .hashColumns(mutableSetOf("street", "city", "state", "zip", "price"))
                .targetJndi("SqlLiteTest")
                .targetTable("target_data")
                .dupesJndi("SqlLiteTest")
                .deleteTargetIfExists(true)
                .deleteDupeIfExists(true)
                .varcharPadding(20)
                .build()
        val config = build

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