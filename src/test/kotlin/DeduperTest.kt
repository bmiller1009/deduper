import org.bradfordmiller.deduper.DedupeReport
import org.bradfordmiller.deduper.Deduper
import org.bradfordmiller.deduper.config.Config
import org.bradfordmiller.deduper.config.HashSourceJndi
import org.bradfordmiller.deduper.config.SourceJndi
import org.bradfordmiller.deduper.jndi.*
import org.bradfordmiller.deduper.persistors.Dupe
import org.bradfordmiller.deduper.sql.SqlUtils
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
                if(!it.isHidden) {
                    Files.delete(it.toPath())
                } else {
                    if(it.name != ".gitignore") {
                        Files.delete(it.toPath())
                    }
                }
            }
        }
        @BeforeAll
        @JvmStatic
        fun cleanUpBefore() {
            clearDataDir()
        }
    }
    private fun getExpectedReport(): DedupeReport {
        return DedupeReport(
            986,
            setOf("street","city", "state", "zip", "price"),
            setOf("street", "city", "zip", "state", "beds", "baths", "sq__ft", "type", "sale_date", "price",
                "latitude", "longitude"),
            4,
            3,
            982,
            mutableMapOf(
                "3230065898C61AE414BA58E7B7C99C0B" to
                Pair(
                    mutableListOf(342L, 984L),
                    Dupe(
                        341L,
                        """{"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq__ft":"834","longitude":"-121.450858"}"""
                    )
                ),
                "0A3E9B5F1BDEDF777A313388B815C294" to
                Pair(
                    mutableListOf(404L),
                    Dupe(
                        403L,
                        """{"zip":"95621","baths":"2","city":"CITRUS HEIGHTS","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"8306 CURLEW CT","price":"167293","latitude":"38.715781","state":"CA","beds":"4","type":"Residential","sq__ft":"1280","longitude":"-121.298519"}"""
                    )
                ),
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
    }
    private fun getSourceCount(sourceJndi: SourceJndi): Long {
        return JNDIUtils.getJndiConnection(sourceJndi).use {conn ->
            val sql = "SELECT COUNT(1) FROM ${sourceJndi.tableQuery}"
            conn.prepareStatement(sql).use {stmt ->
                stmt.executeQuery().use {rs ->
                    rs.next()
                    rs.getLong(1)
                }
            }
        }
    }
    private fun getColumnsFromSource(sourceJndi: SourceJndi): Map<Int, String> {
        return JNDIUtils.getJndiConnection(sourceJndi).use {conn ->
            val sql = "SELECT * FROM ${sourceJndi.tableQuery}"
            conn.prepareStatement(sql).use {stmt ->
                stmt.executeQuery().use {rs ->
                    SqlUtils.getColumnsFromRs(rs.metaData)
                }
            }
        }
    }
    private fun getFirstRowFromSource(sourceJndi: SourceJndi): Array<String> {
        JNDIUtils.getJndiConnection(sourceJndi).use {conn ->
            val sql = "SELECT * FROM ${sourceJndi.tableQuery} LIMIT 1"
            conn.prepareStatement(sql).use {stmt ->
                stmt.executeQuery().use {rs ->
                    val cols = SqlUtils.getColumnsFromRs(rs.metaData)
                    rs.next()
                    return cols.map {c ->
                        rs.getString(c.value)
                    }.toTypedArray()
                }
            }
        }
    }
    @Test fun dedupeCsv() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvTargetJndi = CsvJNDITargetType("RealEstateOut", "default_ds",false)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupes", "default_ds",false)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)
        val outputDataJndi = SourceJndi("OutputDataTarget", "default_ds", "targetName")
        val outputDupeJndi = SourceJndi("OutputDataDupe", "default_ds", "dupeName")

        val expectedReport = getExpectedReport()

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(csvTargetJndi)
            .dupesJndi(csvDupesJndi)
            .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        val rowCountTarget = getSourceCount(outputDataJndi)
        val columnsTarget = getColumnsFromSource(outputDataJndi)
        val firstRowTarget = getFirstRowFromSource(outputDataJndi)
        val expectedColumnMapTarget = mapOf(
            1 to "street", 2 to "city", 3 to "zip", 4 to "state", 5 to "beds", 6 to "baths", 7 to "sq__ft", 8 to "type",
            9 to "sale_date", 10 to "price", 11 to "latitude", 12 to "longitude"
        )
        val expectedFirstRowTarget = arrayOf(
            "3526 HIGH ST","SACRAMENTO","95838","CA","2","1","836","Residential","Wed May 21 00:00:00 EDT 2008","59222","38.631913","-121.434879"
        )

        val rowCountDupe = getSourceCount(outputDupeJndi)
        val columnsDupe = getColumnsFromSource(outputDupeJndi)
        val firstRowDupe = getFirstRowFromSource(outputDupeJndi)

        val expectedColumnMapDupe = mapOf(
            1 to "hash", 2 to "row_ids", 3 to "first_found_row_number", 4 to "dupe_values"
        )
        val expectedFirstRowDupe = arrayOf(
            "3230065898C61AE414BA58E7B7C99C0B","[342,984]","341",
            """{"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq__ft":"834","longitude":"-121.450858"}"""
        )

        assert(report == expectedReport)

        assert(rowCountTarget == 982L)
        assert(columnsTarget == expectedColumnMapTarget)
        assert(firstRowTarget.contentEquals(expectedFirstRowTarget))

        assert(rowCountDupe == 3L)
        assert(columnsDupe == expectedColumnMapDupe)
        assert(firstRowDupe.contentEquals(expectedFirstRowDupe))
    }
    @Test fun dedupeSql() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",false,"real_estate")
        val sqlDupeJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)
        val outputJndiTarget = SourceJndi("SqlLiteTest", "default_ds", "real_estate")
        val outputJndiDupe = SourceJndi("SqlLiteTest", "default_ds", "dupes")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupeJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()

        val rowCountTarget = getSourceCount(outputJndiTarget)
        val columnsTarget = getColumnsFromSource(outputJndiTarget)
        val firstRowTarget = getFirstRowFromSource(outputJndiTarget)

        val rowCountDupe = getSourceCount(outputJndiDupe)
        val columnsDupe = getColumnsFromSource(outputJndiDupe)
        val firstRowDupe = getFirstRowFromSource(outputJndiDupe)

        val expectedColumnMapTarget = mapOf(
            1 to "street", 2 to "city", 3 to "zip", 4 to "state", 5 to "beds", 6 to "baths", 7 to "sq__ft", 8 to "type",
            9 to "sale_date", 10 to "price", 11 to "latitude", 12 to "longitude"
        )

        val expectedFirstRowTarget = arrayOf(
            "3526 HIGH ST","SACRAMENTO","95838","CA","2","1","836","Residential","Wed May 21 00:00:00 EDT 2008","59222","38.631913","-121.434879"
        )

        val expectedColumnMapDupe = mapOf(
            1 to "hash", 2 to "row_ids", 3 to "first_found_row_number", 4 to "dupe_values"
        )

        val expectedFirstRowDupe = arrayOf(
            "3230065898C61AE414BA58E7B7C99C0B","[342,984]","341",
            """{"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq__ft":"834","longitude":"-121.450858"}"""
        )

        assert(rowCountTarget == 982L)
        assert(columnsTarget == expectedColumnMapTarget)
        assert(firstRowTarget.contentEquals(expectedFirstRowTarget))

        assert(rowCountDupe == 3L)
        assert(columnsDupe == expectedColumnMapDupe)
        assert(firstRowDupe.contentEquals(expectedFirstRowDupe))
    }
    @Test fun justDupes() {

        File("src/test/resources/data/outputData/targetName.txt").delete()

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupesUseDefaultsWithPipes", "default_ds",true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)
        val outputDupeJndi = SourceJndi("OutputDataDupe", "default_ds", "dupeName")

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()

        val rowCountDupe = getSourceCount(outputDupeJndi)
        val columnsDupe = getColumnsFromSource(outputDupeJndi)
        val firstRowDupe = getFirstRowFromSource(outputDupeJndi)

        val expectedColumnMapDupe = mapOf(
                1 to "hash", 2 to "row_ids", 3 to "first_found_row_number", 4 to "dupe_values"
        )

        val expectedFirstRowDupe = arrayOf(
                "3230065898C61AE414BA58E7B7C99C0B","[342,984]","341",
                """{"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq__ft":"834","longitude":"-121.450858"}"""
        )

        assert(rowCountDupe == 3L)
        assert(columnsDupe == expectedColumnMapDupe)
        assert(firstRowDupe.contentEquals(expectedFirstRowDupe))
        assert(!File("src/test/resources/data/outputData/targetName.txt").exists())
    }
    @Test fun targetOnly() {
        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)
        val csvTargetJndi = CsvJNDITargetType("RealEstateOut", "default_ds",false)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .targetJndi(csvTargetJndi)
                .build()

        val deduper = Deduper(config)

        val deduperReport = deduper.dedupe()
    }
    @Test fun withoutTargetAndDupe() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        val expectedReport = getExpectedReport()

        assert(report == expectedReport)
    }
    @Test fun hashPersistor() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", "default_ds",true, true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

        val hashSourceJndi = SourceJndi("SqlLiteTest", "default_ds", "hashes")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupesJndi)
            .hashJndi(sqlHashJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()

        val sourceCount = getSourceCount(hashSourceJndi)
        val sourceColumns = getColumnsFromSource(hashSourceJndi)
        val sourceFirstRow = getFirstRowFromSource(hashSourceJndi)

        assert(sourceCount == 982L)
        assert(sourceColumns == mapOf(1 to "hash", 2 to "json_row"))
        assert(sourceFirstRow[0] == "B23CF69F6FC378E0A9C1AF14F2D2083C")
        assert(sourceFirstRow[1] == "{\"zip\":\"95838\",\"baths\":\"1\",\"city\":\"SACRAMENTO\",\"sale_date\":\"Wed May 21 00:00:00 EDT 2008\",\"street\":\"3526 HIGH ST\",\"price\":\"59222\",\"latitude\":\"38.631913\",\"state\":\"CA\",\"beds\":\"2\",\"type\":\"Residential\",\"sq__ft\":\"836\",\"longitude\":\"-121.434879\"}")
    }
    @Test fun hashPersistorNoJson() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlTargetJndi = SqlJNDITargetType("SqlLiteTest", "default_ds",true,"target_data")
        val sqlDupesJndi = SqlJNDIDupeType("SqlLiteTest", "default_ds",true)
        val sqlHashJndi = SqlJNDIHashType("SqlLiteTest", "default_ds",false, true)
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)
        val hashSourceJndi = SourceJndi("SqlLiteTest", "default_ds", "hashes")

        val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .targetJndi(sqlTargetJndi)
            .dupesJndi(sqlDupesJndi)
            .hashJndi(sqlHashJndi)
            .build()

        val deduper = Deduper(config)

        deduper.dedupe()

        val sourceFirstRow = getFirstRowFromSource(hashSourceJndi)

        assert(sourceFirstRow[1].isNullOrBlank())
    }
    @Test fun nullsInSource() {
        val csvTargetJndi = CsvJNDITargetType("RealEstateOut", "default_ds",true)
        val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupes", "default_ds",true)
        val sqlSourceJndi = SourceJndi("SqliteChinook", "default_ds","tracks")
        val sourceTestNulls = SourceJndi("SqlLiteTest", "default_ds","target_data")
        val sourceTestNullsFirstRow = SourceJndi("SqlLiteTest", "default_ds","target_data WHERE TrackId = 2")

        val config = Config.ConfigBuilder()
                .sourceJndi(sqlSourceJndi)
                .targetJndi(csvTargetJndi)
                .dupesJndi(csvDupesJndi)
                .build()

        val deduper = Deduper(config)

        deduper.dedupe()

        val sourceCount = getSourceCount(sourceTestNulls)
        val sourceColumns = getColumnsFromSource(sourceTestNulls)
        val sourceFirstRow = getFirstRowFromSource(sourceTestNullsFirstRow)

        assert(sourceCount == 3503L)
        assert(
          sourceColumns ==
          mapOf(
            1 to "TrackId",
            2 to "Name",
            3 to "AlbumId",
            4 to "MediaTypeId",
            5 to "GenreId",
            6 to "Composer",
            7 to "Milliseconds",
            8 to "Bytes",
            9 to "UnitPrice"
          )
        )
        assert(sourceFirstRow[5].isNullOrBlank())
    }
    @Test fun sourceHashTable() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val sqlSourceJndi = SourceJndi("SqlLiteTestInput", "default_ds","real_estate", hashColumns)
        val sqlHashSourceJndi = HashSourceJndi("SqlLiteTestInput", "default_ds","hashes", "hash")

        val config = Config.ConfigBuilder()
            .sourceJndi(sqlSourceJndi)
            .seenHashesJndi(sqlHashSourceJndi)
            .build()

        val deduper = Deduper(config)

        val report = deduper.dedupe()

        assert(report.hashCount == 982L)
        assert(report.recordCount == 982L)
        assert(report.dupeCount == 982L)
        assert(report.distinctDupeCount == 982L)
        assert(report.dupes.size == 982)
    }
    @Test fun sampleHash() {

        val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
        val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

        val config = Config.ConfigBuilder()
                .sourceJndi(csvSourceJndi)
                .build()

        val deduper = Deduper(config)

        val sampleRow = deduper.getSampleHash()

        assert(sampleRow.sampleString == "3526 HIGH ST, SACRAMENTO, CA, 95838, 59222")
        assert(sampleRow.sampleHash == "B23CF69F6FC378E0A9C1AF14F2D2083C")
    }
}