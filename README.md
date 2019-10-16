# deduper

[![Build Status](https://travis-ci.org/bmiller1009/deduper.svg?branch=master)](https://travis-ci.org/bmiller1009/deduper)

General deduping engine for JDBC sources with output to JDBC/csv targets 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

What things you need to install the software and how to install them

* Gradle 5.4.1 or greater if you want to build from source
* JVM 8+

### Installing

If you're using [Maven](maven.apache.org) simply specify the GAV coordinate below and Maven will do the rest

```
<dependency>
  <groupId>org.bradfordmiller</groupId>
  <artifactId>deduper</artifactId>
  <version>0.0.10</version>
</dependency>
```

#### Running with SBT

Add this GAV coordinate to your SBT dependency list

```
libraryDependencies += "org.bradfordmiller" %% "deduper" % "0.0.10"
```

### Running with Gradle

Add this GAV coordinate to your Gradle dependencies section

```
dependencies {
    ...
    ...
    implementation 'org.bradfordmiller:deduper:0.0.10'
}
```

## Building from source

Gradle Build instructions will go here

## Using the library

The library uses the [builder](https://www.baeldung.com/kotlin-builder-pattern) design pattern to construct the
configuration to run a deduping job.  Configuation information is stored using the [simple-jndi](https://github.com/h-thurow/Simple-JNDI)
API so a [jndi.properties](https://github.com/bmiller1009/deduper/blob/master/src/main/resources/jndi.properties) file will need to be present
in src/main/resources and correctly configured for this library to work.  More on this later.

## Built With

* [kotlin](https://kotlinlang.org/) - The programming language
* [simple-jndi](https://github.com/h-thurow/Simple-JNDI) - source and target configuration management

## Versioning

For the versions available, see the [tags on this repository](https://github.com/bmiller1009/deduper/tags). 

## Authors

* **Bradford Miller** - *Initial work* - [bfm](https://github.com/bmiller1009)

See also the list of [contributors](https://github.com/bmiller1009/deduper/contributors) who participated in this project.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Thanks to [PurpleBooth](https://gist.github.com/PurpleBooth) for the README template as seen [here](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)