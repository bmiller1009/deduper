import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.config.HashSourceJndi
import org.bradfordmiller.deduper.config.SourceJndi
import org.bradfordmiller.deduper.jndi.CsvJNDITargetType
import org.bradfordmiller.deduper.jndi.SqlJNDIDupeType
import org.bradfordmiller.deduper.jndi.SqlJNDIHashType
import org.bradfordmiller.deduper.jndi.SqlJNDITargetType
import org.bradfordmiller.deduper.persistors.Dupe
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

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvTargetJndi = CsvJNDITargetType("RealEstateOut", "default_ds",false)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupes", "default_ds",false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val expectedReport =
          DedupeReport(
            986,
             setOf("street","city", "state", "zip", "price"),
             setOf("street", "city", "zip", "state", "beds", "baths", "sq__ft", "type", "sale_date", "price",
                     "latitude", "longitude"),
                  4,
                  3,
             mutableMapOf(
               "3230065898C61AE414BA58E7B7C99C0B" to
                 Pair(
                   mutableListOf(342L, 984L),
                   Dupe(
                     341L,
                           """{"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq__ft":"834","longitude":"-121.450858"}"""
                   )
                 )
             ,
             "0A3E9B5F1BDEDF777A313388B815C294" to
               Pair(
                 mutableListOf(404L),
                 Dupe(
                     403L,
                         """{"zip":"95621","baths":"2","city":"CITRUS HEIGHTS","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"8306 CURLEW CT","price":"167293","latitude":"38.715781","state":"CA","beds":"4","type":"Residential","sq__ft":"1280","longitude":"-121.298519"}"""
                 )
               )
             ,
             "C4E3F2029871080759FC1C0F878236C3" to
               Pair(
                 mutableListOf(601L),
                 Dupe(
                     600L,
                        """{"zip":"95648","baths":"0","city":"LINCOLN","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"7 CRYSTALWOOD CIR","price":"4897","latitude":"38.885962","state":"CA","beds":"0","type":"Residential","sq__ft":"0","longitude":"-121.289436"}"""
                 )
               )
          )
          )

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(csvTargetJndi)
            .dupesJndi(csvDupesJndi)
            .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        assert(report == expectedReport)
    }
    @Test fun dedupeSqlTest() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",false,"real_estate")
        val sqlDupeJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupeJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testCsvTargetWithDefaults() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", "default_ds",false)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", "default_ds",false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(csvTargetJndi)
            .dupesJndi(csvDupesJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testRunWithoutTarget() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testRunWithoutTargetAndDupe() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        println(report)
        println(report.dupes)
    }

    @Test fun testDeleteCsvDeleteTarget() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", "default_ds",true)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteDupe() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", "default_ds",true)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteCsvDeleteTargetAndDupe() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvTargetJndi = CsvJNDITargetType("RealEstateOutTargetUseDefaults", "default_ds",true)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaults", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteTarget() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteDupe() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testDeleteSqlDeleteDupeAndTarget() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data", 20)
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        var build = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .build()

        val deduper = Deduper(build)

        deduper.dedupe()
    }

    @Test fun testHashPersistor() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", "default_ds",true, true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupesJndi)
            .hashJndi(sqlHashJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testHashPersistorNoJson() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", "default_ds",false, true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupesJndi)
            .hashJndi(sqlHashJndi)
            .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        println(report)
        println(report.dupes)
    }

    @Test fun testNullsInSource() {
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", "default_ds",true, true)
        val sqlSourceJndi = SourceJndi("SqliteChinook", "default_ds","tracks")

        val config = Config.ConfigBuilder()
                .sourceJndi(sqlSourceJndi)
                .targetJndi(sqlTargetJndi)
                .dupesJndi(sqlDupesJndi)
                .hashJndi(sqlHashJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()
    }

    @Test fun testSourceHashTable() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlSourceJndi = SourceJndi("SqlLiteTest", "default_ds","real_estate", hashColumns)
        val sqlHashSourceJndi = HashSourceJndi("SqlLiteTest", "default_ds","hashes", "hash")

        val config = Config.ConfigBuilder()
            .sourceJndi(sqlSourceJndi)
            .seenHashesJndi(sqlHashSourceJndi)
            .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        println(report)
    }

    @Test fun testSampleHash() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .build()

        val deduper = Deduper(config)

        val sampleRow = deduper.getSampleHash()

        println(sampleRow)
    }
}