# deduper

[![Build Status](https://travis-ci.org/bmiller1009/deduper.svg?branch=master)](https://travis-ci.org/bmiller1009/deduper)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.bradfordmiller/deduper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.bradfordmiller/deduper)

General deduping engine for JDBC sources with output to JDBC/csv targets 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

What things you need to install the software and how to install them

* Gradle 5.4.1 or greater if you want to build from source
* JVM 8+

### Installing

#### Running with Maven

If you're using [Maven](maven.apache.org) simply specify the GAV coordinate below and Maven will do the rest

```
<dependency>
  <groupId>org.bradfordmiller</groupId>
  <artifactId>deduper</artifactId>
  <version>0.0.20</version>
</dependency>
```

#### Running with SBT

Add this GAV coordinate to your SBT dependency list

```
libraryDependencies += "org.bradfordmiller" %% "deduper" % "0.0.20"
```

#### Running with Gradle

Add this GAV coordinate to your Gradle dependencies section

```
dependencies {
    ...
    ...
    implementation 'org.bradfordmiller:deduper:0.0.20'
}
```

## Building from source

Gradle Build instructions will go here

## Using the library

Configuation information is stored using the [simple-jndi](https://github.com/h-thurow/Simple-JNDI)
API so a [jndi.properties](https://github.com/bmiller1009/deduper/blob/master/src/test/resources/jndi.properties) file will need to be present
in src/main/resources and correctly configured for this library to work.  

### Configuring jndi
See [jndi.properties](https://github.com/bmiller1009/deduper/blob/master/src/test/resources/jndi.properties) sample file
in this project. This will need to be configured and dropped into **_src/main/resources_** for the API to work.  Actual jndi
files will be searched and loaded based upon the path in the **_org.osjava.sj.root_** property setting.  In the example file 
for this project, you will see the path is **_src/main/resources/jndi_**.  

####Configuring jndi contexts
Jndi property files can be dropped into the **_org.osjava.sj.root_** configured path. In our case, that path is **_src/main/resources/jndi_**.  There are two types of contexts that deduper can handle:  A **javax.sql.DataSource** and a **java.util.Map**. 

Datasources are used when reading or writing data using a JDBC interface. Maps are used primarily for writing output to a flat file. These concepts will be explained in detail later.  You can see a sample jndi context file [here](https://github.com/bmiller1009/deduper/blob/master/src/test/resources/jndi/default_ds.properties).  Note the location of the context file is in the directory set in **_org.osjava.sj.root_**:  **_src/main/resources/jndi_**.  All jndi context files must be placed under this directory.

Here is a sample DataSource entry for a sql lite database which is used by this projects unit tests (note that username and password are optional and depend on how the security of the database being targeted is configured):

    SqliteChinook/type=javax.sql.DataSource  
    SqliteChinook/driver=org.sqlite.JDBC  
    SqliteChinook/url=jdbc:sqlite:src/test/resources/data/chinook.db  
    SqliteChinook/user=  
    SqliteChinook/password=

The jndi name in this case is "SqliteChinook".  The context is "default\_ds" because the name of the property file is "default_ds.properties".

Here is a sample Map entry for a target csv file. Currently these are gathered in a key-value pair pattern. In addition to the "targetName" property which is the path to the csv file, other optional parameters include the file delimiter ("delimiter" property) and file extension ("extension" property). Note that the delimiter property defaults to a comma and the extension property defaults to txt if not otherwised specified in the Map entry:

    RealEstateOutDupes/type=java.util.Map   
    RealEstateOutDupes/ext=txt   
    RealEstateOutDupes/delimiter=|  
    RealEstateOutDupes/targetName=src/test/resources/data/outputData/dupeName

The jndi name in this case is "RealEstateOutDupes".  The context is "default\_ds" because the name of the property file is "default_ds.properties".

#### Adding Jndi entries programatically

Use the **_JNDIUtils_** class in the deduper library to add jndi entries programatically

Kotlin code for adding a new DataSource jndi entry to the default_ds.properties jndi file:
```kotlin
    import org.bradfordmiller.deduper.jndi.JNDIUtils  
    ...  
    JNDIUtils.addJndiConnection(  
                    "BradTestJNDI_23",  
                    "default_ds",  
                     mapOf(  
                            "type" to "javax.sql.DataSource",  
                            "driver" to "org.sqlite.JDBC",  
                            "url" to "jdbc:sqlite:src/test/resources/data/outputData/real_estate.db",  
                            "user" to "test_user",  
                            "password" to "test_password"  
                    )  
            )  
```
### Configuring and running a deduper process

The library uses the [builder](https://www.baeldung.com/kotlin-builder-pattern) design pattern to construct the configuration to run a deduping job.  

There are a bunch of options which can be configured as part of a deduper process.  Let's start with the basics. Use the **_Config_** class to set up the deduper job.  This object will be passed into the **_Deduper_** class as part of the instantiation of the **_Deduper_** class. 

The only _required_ input to deduper is a JDBC souce in the form of a JNDI Connection.  This is set up using the SourceJndi class.  Here is some Kotlin code which instantiates a **_SourceJndi_** object. 

    import org.bradfordmiller.deduper.config.SourceJndi  
    ...  
    val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions")

In the above case "RealEstateIn" is the jndi name, "default\_ds" is the context name (and correlates to "default\_ds.properties"), and "Sacramentorealestatetransactions" is the table to be queried. 

By default, a "SELECT *" query will be issued against the table ("Sacramentorealestatetransactions" in this case). It is also possible to pass in a query, rather than a table name, like so:

    import org.bradfordmiller.deduper.config.SourceJndi  
    ...  
    val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "SELECT street from Sacramentorealestatetransactions")

Deduper is an engine which can detect duplicates, so by default it will use every value in the row to create a duplicate. The API also accepts a subset of columns in the table on which to "dedupe".  Here is some Kotlin code which demonstrates this:

    import org.bradfordmiller.deduper.config.SourceJndi  
    ...  
    val hashColumns = mutableSetOf("street","city", "state", "zip", "price")  
    val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

Now only the columns specified in the column set will be considered for detecting duplicates.

### Complete example

    import org.bradfordmiller.deduper.config.SourceJndi
    import org.bradfordmiller.deduper.Deduper
    import org.bradfordmiller.deduper.config.Config
    ...
    val hashColumns = mutableSetOf("street","city", "state", "zip", "price")  
    val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

    val config = 
      Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .build()

    val deduper = Deduper(config)

    val report = deduper.dedupe()

    println(report)
    println(report.dupes)
    
The output of this run is:

    Dedupe report: recordCount=986, columnsFound=[street, city, zip, state, beds, baths, sq__ft, type, sale_date, price, latitude, longitude], hashColumns=[street, city, state, zip, price], dupeCount=4, distinctDupeCount=3

    {3230065898C61AE414BA58E7B7C99C0B=([342, 984], Dupe(firstFoundRowNumber=341, dupes={"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq__ft":"834","longitude":"-121.450858"})), 0A3E9B5F1BDEDF777A313388B815C294=([404], Dupe(firstFoundRowNumber=403, dupes={"zip":"95621","baths":"2","city":"CITRUS HEIGHTS","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"8306 CURLEW CT","price":"167293","latitude":"38.715781","state":"CA","beds":"4","type":"Residential","sq__ft":"1280","longitude":"-121.298519"})), C4E3F2029871080759FC1C0F878236C3=([601], Dupe(firstFoundRowNumber=600, dupes={"zip":"95648","baths":"0","city":"LINCOLN","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"7 CRYSTALWOOD CIR","price":"4897","latitude":"38.885962","state":"CA","beds":"0","type":"Residential","sq__ft":"0","longitude":"-121.289436"}))}

So this run found a total of **986** rows in the source table.  Using the columns "street, city, state, zip, price" **four** total duplicates were found.  **Three** distinct duplicates were found, meaning one duplicate actually occurred twice.  By examining the dupes object, we can see there are three unique hashes which occurred more than once.  If we take the first example, hash **3230065898C61AE414BA58E7B7C99C0B** was first seen at row **341** and was then seen again at rows **342** and **984**.

A look at the [Sacramentorealestatetransactions.csv](https://github.com/bmiller1009/deduper/blob/master/src/test/resources/data/Sacramentorealestatetransactions.csv) file in the test data folder, we can indeed see that there are **986** rows in the file.  We can also see that column values for columns "street, city, state, zip, price" indeed first occurred on line **341** and were repeated on lines
**342** and **984**.

### Deduping data against a known set of hashes

This can be useful if you want to take an existing set of hashes and look for matches between the known set and the current set.  Loading in an existing hash set is simply setting a **_HashSourceJndi_** object in the configuration builder, as well as a table name for the stored hashes and the column under which the hashes are stored:

	import org.bradfordmiller.deduper.config.HashSourceJndi
    import org.bradfordmiller.deduper.config.SourceJndi
    import org.bradfordmiller.deduper.Deduper
    import org.bradfordmiller.deduper.config.Config
    ...
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

Before examining the results of the run let's go over the code. There is a new property being set in the config builder which is a **_HashSourceJndi_** object. That object contains the jndi details (jndi name and context) as well as the table name (**_hashes_**) and the column in the **_hashes_** table where the hash values are stored.  In this case the column name in **_hashes_** where the hash values are stored is simply **_hash_**.

The **_hashes_** table in this contains all of the unique hashes from the [Sacramentorealestatetransactions.csv](https://github.com/bmiller1009/deduper/blob/master/src/test/resources/data/Sacramentorealestatetransactions.csv) file and they are stored in the column **_hash_**.  Thus, the expected output of this run should report that **_no_** unique values were found, and that all were duplicates.  The report confirms this:

    Dedupe report: recordCount=982, columnsFound=[street, city, zip, state, beds, baths, sq__ft, type, sale_date, price, latitude, longitude], hashColumns=[street, city, state, zip, price], dupeCount=982, distinctDupeCount=982

We saw earlier that the [Sacramentorealestatetransactions.csv](https://github.com/bmiller1009/deduper/blob/master/src/test/resources/data/Sacramentorealestatetransactions.csv) file had four duplicates, meaning there were **_982_** unique rows in the dataset.  We can see above that the dupeCount of the report was **_982_**.

### Sampling the hash

You can see a sample row and how it is hashed to get a sense of the hash value and the actual values being passed in:
    
    import org.bradfordmiller.deduper.config.SourceJndi
    import org.bradfordmiller.deduper.Deduper
    import org.bradfordmiller.deduper.config.Config
    
    ...
    
    val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
    val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds", "Sacramentorealestatetransactions", hashColumns)

    val config = Config.ConfigBuilder()
            .sourceJndi(csvSourceJndi)
            .build()

    val deduper = Deduper(config)

    val sampleRow = deduper.getSampleHash()

    println(sampleRow)

The output of this call is as follows

    SampleRow(sampleString=3526 HIGH ST, SACRAMENTO, CA, 95838, 59222, sampleHash=B23CF69F6FC378E0A9C1AF14F2D2083C)


### Configuring output files

The deduper library has three optional outputs that can be configured either as SQL tables in a JDBC connection or output csv files.  All three of these outputs are **optional** and can be easily configured in the config builder.  All of the classes mentioned below can be found in the **_org.bradfordmiller.deduper.jndi_** package.

#### JDBC output

For the JDBC interface, the classes to use are **_SqlJNDITargetType_**, **_SqlJNDIDupeType_**, and **_SqlJNDIHashType_**.  

**_SqlJNDITargetType_** has a dynamic schema and is for configuring the output of the deduplicated data and the schema for the table is automatically generated based on the Jndi source metadata. 

**_SqlJNDIDupeType_** has a static schema and is for configuring the output of any duplicate data in a json format.

**_SqlJNDIHashType_** has a static schema and is for configuring the output of "found" hashes in the Jndi source data and will also optional emit the json representation which comprises the hash data.

For the csv interface, the classes to use are **_CsvTargetPersistor_**, **_CsvDupePersistor_**, and **_CsvHashPersistor_**.  These classes follow a similar output format to the above JDBC classes, but they produce csv files rather than database tables.

Note that the csv and JDBC interfaces can be used interchangably in the same dedupe job. IE you can output dupes to a flat file and hashes to a JDBC, etc, etc.

Let's look at an example:
	 
	import org.bradfordmiller.deduper.Deduper
    import org.bradfordmiller.deduper.config.Config
    import org.bradfordmiller.deduper.config.HashSourceJndi
    import org.bradfordmiller.deduper.config.SourceJndi
    import org.bradfordmiller.deduper.jndi.CsvJNDITargetType
    import org.bradfordmiller.deduper.jndi.SqlJNDIDupeType
    import org.bradfordmiller.deduper.jndi.SqlJNDIHashType
    import org.bradfordmiller.deduper.jndi.SqlJNDITargetType
	 
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

    val report = deduper.dedupe()

    println(report)
    println(report.dupes)

Before examining the output, let's walk through the code.  

**_SqlJNDITargetType_** is instantiated with the **jndi name, context, a boolean flag** which indicates whether or not to drop the table if it already exists, and a **table name**.

**_SqlJNDIDupeType_** is instantiated with the **jndi name, context, and a boolean flag** which indicates whether or not to drop the dupe table if it already exists. Note:  **The table name for the duplicate data is always "dupes".**

**_SqlJNDIHashType_** is instantiated with the **jndi name, context, a boolean flag** which indicates whether to include the data row expressed in json format, and **a boolean flag** which indicates whether or not to drop the dupe table if it already exists.

The report variable in the above example will be the same, but lets look at the persistent objects created by this run of deduper:

A **sql table** named **target_data** in the sqlite database.  The schema of this table will closely mirror the data types found in the source.  Note that if the source is a csv, all of the column types in the target_data table will be strings.
  
A **sql table** named **dupes** in the sqlite database with the following schema:

    CREATE TABLE dupes(hash TEXT NOT NULL, row_ids TEXT NOT NULL, first_found_row_number INTEGER NOT NULL, dupe_values TEXT NOT NULL,PRIMARY KEY(hash))

Here is a sample row from the **_dupes_** table from the run of the sample code above:

"3230065898C61AE414BA58E7B7C99C0B","[342,984]","341",	"{"zip":"95820","baths":"1","city":"SACRAMENTO","sale_date":"Mon May 19 00:00:00 EDT 2008","street":"4734 14TH AVE","price":"68000","latitude":"38.539447","state":"CA","beds":"2","type":"Residential","sq\_\_ft":"834","longitude":"-121.450858"}"

A **sql table** named **hashes** in the sqlite database with the following schema:

    CREATE TABLE hashes(hash TEXT NOT NULL, json_row TEXT NULL, PRIMARY KEY(hash))

Here is a sample row from the **_hashes_** table from the run of the sample code above:

"B23CF69F6FC378E0A9C1AF14F2D2083C","{"zip":"95838","baths":"1","city":"SACRAMENTO","sale_date":"Wed May 21 00:00:00 EDT 2008","street":"3526 HIGH ST","price":"59222","latitude":"38.631913","state":"CA","beds":"2","type":"Residential","sq\_\_ft":"836","longitude":"-121.434879"}"

Note that because the boolean flag in the hashes source class was set to true, the json that makes up the hash is included for each hash written to the table.

#### Csv output

As mentioned earlier, csv outputs are defined in a jndi context as follows:

    RealEstateOutDupes/type=java.util.Map   
    RealEstateOutDupes/ext=txt   
    RealEstateOutDupes/delimiter=|  
    RealEstateOutDupes/targetName=src/test/resources/data/outputData/dupeName
    
The _minimum_ information needed to configure a csv output is the **targetName** as this is a path to output file location.  If the "ext" property and "delimiter" property aren't populated then the defaults will be used, which are "txt" for "ext" and "," for "delimiter".  All csv output definitions use the **_CsvJNDITargetType_** class.  This class takes in the jndi name, context, and **_deleteIfExists_** boolean flag.  

Here is an example outputting data to csv.  Here are the jndi configurations for **RealEstateOut** and **RealEstateOutDupes**:

    RealEstateOut/type=java.util.Map
	RealEstateOut/ext=txt
	RealEstateOut/delimiter=,
	RealEstateOut/targetName=src/test/resources/data/outputData/targetName

and
	
	RealEstateOutDupes/type=java.util.Map
	RealEstateOutDupes/ext=txt
	RealEstateOutDupes/delimiter=|
	RealEstateOutDupes/targetName=src/test/resources/data/outputData/dupeName

Here is the code to output csv target data and csv dupe data:

    val hashColumns = mutableSetOf("street","city", "state", "zip", "price")
    val csvTargetJndi = CsvJNDITargetType("RealEstateOut", "default_ds",false)
    val csvDupesJndi = CsvJNDITargetType("RealEstateOutDupes", "default_ds",false)
    val csvSourceJndi = SourceJndi("RealEstateIn", "default_ds","Sacramentorealestatetransactions", hashColumns)

    val config = Config.ConfigBuilder()
        .sourceJndi(csvSourceJndi)
        .targetJndi(csvTargetJndi)
        .dupesJndi(csvDupesJndi)
        .build()

    val deduper = Deduper(config)

    val report = deduper.dedupe()

    println(report)
    println(report.dupes)

Here is the log output:

    Dedupe report: recordCount=986, columnsFound=[street, city, zip, state, beds, baths, sq__ft, type, sale_date, price, latitude, longitude],      hashColumns=[street, city, state, zip, price], dupeCount=4, distinctDupeCount=3
    2019-10-17 14:40:09,206 [main] INFO  Deduper:296 - Deduping process complete.
    recordCount=986, columnsFound=[street, city, zip, state, beds, baths, sq__ft, type, sale_date, price, latitude, longitude], hashColumns=[street, city, state, zip, price], dupeCount=4, distinctDupeCount=3

Also produced were two files, in the **_src/test/resources/data/outputData_** directory:  dupeName.txt (which contains the duplicates found) and targetName.txt (which contains the deduped data set).  A similar method can be used to persist "found" hashes to a csv.

## Built With

* [kotlin](https://kotlinlang.org/) - The programming language
* [simple-jndi](https://github.com/h-thurow/Simple-JNDI) - source and target configuration management
* [opencsv](http://opencsv.sourceforge.net/) - formatting output of flat files
* [csvjdbc](http://csvjdbc.sourceforge.net/) - Provides a query API on top of csv

## Versioning

For the versions available, see the [tags on this repository](https://github.com/bmiller1009/deduper/tags). 

## Authors

* **Bradford Miller** - *Initial work* - [bfm](https://github.com/bmiller1009)

See also the list of [contributors](https://github.com/bmiller1009/deduper/contributors) who participated in this project.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Thanks to [PurpleBooth](https://gist.github.com/PurpleBooth) for the README template as seen [here](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
