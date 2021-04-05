<div align="center">
  <img width="512" src="https://raw.githubusercontent.com/2n3g5c9/advanced-analytics-with-spark/master/img/banner.png" alt="advanced-analytics-with-spark">
</div>

<p align="center">
    <a href="#-code-quick-access">Code quick access</a>
    &nbsp; • &nbsp;
    <a href="#-getting-started">Getting started</a>
    &nbsp; • &nbsp;
    <a href="#-apache-zeppelin-notebooks">Apache Zeppelin notebooks</a>
    &nbsp; • &nbsp;
    <a href="#-techframeworks-used">Tech/frameworks used</a>
    &nbsp; • &nbsp;
    <a href="#-license">License</a>
</p>

<div align="center">
  <!-- Build Status -->
  <a href="https://circleci.com/gh/2n3g5c9/advanced-analytics-with-spark">
    <img src="https://circleci.com/gh/2n3g5c9/advanced-analytics-with-spark.svg?style=svg" alt="Build Status" />
  </a>
  <!-- Maintainability Status -->
  <a href="https://codeclimate.com/github/2n3g5c9/advanced-analytics-with-spark/maintainability">
    <img src="https://api.codeclimate.com/v1/badges/90d0d67a63d5e805693f/maintainability" />
  </a>
</div>

## 🚀 Code quick access

[Chapter 3 - Recommending Music and the Audioscrobbler Data Set](https://github.com/2n3g5c9/advanced-analytics-with-spark/tree/master/src/main/scala/com/datascience/recommender)

[Chapter 4 - Predicting Forest Cover with Decision Trees](https://github.com/2n3g5c9/advanced-analytics-with-spark/tree/master/src/main/scala/com/datascience/rdf)

## 🏁 Getting started

### Prerequisites

Everything is written in Scala code so make sure you have a flavor of **JDK 8** installed as well as **[Scala 2.11](https://www.scala-lang.org/)** and **[SBT](https://www.scala-sbt.org/)**.

### Installation

Clone the repository:

```bash
git clone https://github.com/2n3g5c9/advanced-analytics-with-spark
```

Before running any Spark job, make sure you have downloaded the associated dataset, for example:

```bash
cd data/recommender
./download-data.sh
```

### How to use

Simply compile the project at the root:

```bash
sbt compile
```

Then you can run any particular class, for example:

```bash
sbt "runMain com.datascience.recommender.RunRecommender"
```

You can also package the project in a JAR file at the root:

```bash
sbt package
```

## 📓 Apache Zeppelin notebooks

The case studies are also discussed in Apache Zeppelin notebooks.

Simply start a container:

```bash
docker-compose up -d
```

Remember that the data must be downloaded to run the notebooks.

To stop and remove the container, simply run:

```bash
docker-compose down
```

## 🪄 Tech/frameworks used

- [Apache Spark](https://spark.apache.org/): Unified analytics engine for large-scale data processing.
- [Scala](https://www.scala-lang.org/): Combination of object-oriented and functional programming in one concise, high-level language on the JVM.
- [SBT](https://www.scala-sbt.org/): Interactive build tool for Scala.
- [Apache Zeppelin](https://zeppelin.apache.org/): Web-based notebook that enables data-driven, interactive data analytics and collaborative documents with SQL, Scala and more.

## 📃 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
