import org.bradfordmiller.deduper.Deduper
import org.junit.Test
import java.nio.file.Paths
import java.nio.file.Files

class DeduperTest {
    @Test fun DedupeTest() {

        val rpt =
                Deduper().dedupe(
                "RealEstateIn",
                "Sacramentorealestatetransactions",
                "default_ds",
                "RealEstateOut",
                "targetName",
                    "RealEstateDupes",
                    mutableSetOf("street","city", "state", "zip", "price")
                )

        println(rpt)
    }

    @Test fun testCsvTargetCreation() {

        val hash = "7328393ce354e4b1b574d2d532ea3625".toUpperCase()
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

        assert(md5 == hash)
    }
}