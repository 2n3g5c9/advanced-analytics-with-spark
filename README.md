<div align="center">
  <img width="512" src="https://raw.githubusercontent.com/2n3g5c9/advanced-analytics-with-spark/master/img/advanced-analytics-with-spark_banner.png" alt="advanced-analytics-with-spark">
</div>

<br />

<div align="center">Unofficial DataFrame-oriented solutions to case studies in "Advanced Analytics with Spark" published by O'Reilly</div>

<br />

<div align="center">
  <!-- Build Status -->
  <a href="https://circleci.com/gh/2n3g5c9/advanced-analytics-with-spark">
    <img src="https://circleci.com/gh/2n3g5c9/advanced-analytics-with-spark.svg?style=svg" alt="Build Status" />
  </a>
</div>


## Code quick access

[Chapter 3 - Recommending Music and the Audioscrobbler Data Set](https://github.com/2n3g5c9/advanced-analytics-with-spark/tree/master/src/main/scala/com/datascience/recommender)

## Getting started

### Prerequisites

Everything is written in Scala code so make sure you have a flavor of **JDK 8** installed as well as **[Scala 2.12](https://www.scala-lang.org/)** (compatible with Apache Spark >2.4) and **[SBT](https://www.scala-sbt.org/)**.

### Installation

Clone the repository:

````bash
git clone https://github.com/2n3g5c9/advanced-analytics-with-spark
````

Before running any Spark job, make sure you have downloaded the associated dataset, for example:

```bash
cd data/recommender
./download-data.sh
```

### How to use

Simply compile the project at the root:

````bash
sbt compile
````

Then you can run any particular class, for example:

````bash
sbt "runMain com.datascience.recommender.RunRecommender"
````

You can also package the project in a JAR file at the root:
````bash
sbt package
````

## Tech/frameworks used

- [Apache Spark](https://spark.apache.org/): Unified analytics engine for large-scale data processing.
- [Scala](https://www.scala-lang.org/): Combination of object-oriented and functional programming in one concise, high-level language on the JVM.
- [SBT](https://www.scala-sbt.org/): Interactive build tool for Scala.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details