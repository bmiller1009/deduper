import org.apache.commons.io.FileUtils
import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.config.Config
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files

class DeduperTest {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        private val dataDir = "src/test/resources/data/outputData"

        @BeforeAll
        @JvmStatic
        fun cleanUp() {
            logger.info("Cleaning the output directory $dataDir")

            val dir = File(dataDir)
            val files = dir.listFiles()
            files.forEach {
                if(!it.isHidden)
                    Files.delete(it.toPath())
            }
        }
    }

    @Test fun dedupeCsvTest() {

        val config = Config(
                "RealEstateIn",
                "Sacramentorealestatetransactions",
                "default_ds",
                "RealEstateOut",
                "RealEstateOutDupes",
                mutableSetOf("street","city", "state", "zip", "price")
        )

        val deduper = Deduper(config)

        deduper.dedupe()
    }
    @Test fun dedupeSqlTest() {
        val config = Config(
            "RealEstateIn",
            "Sacramentorealestatetransactions",
            "default_ds",
            "SqlLiteTest",
            "real_estate",
            "SqlLiteTest",
            mutableSetOf("street","city", "state", "zip", "price")
        )

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