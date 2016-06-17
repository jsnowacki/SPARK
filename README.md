# SPARK

Materials for course: Introduction to Big Data with Apache Spark

# Structure

* `core` - Apache Spark core examples  
* `data` - data for the exercises
* `docker` - Docker used in training
* `exercises` - exercise questions
* `notebooks` - Jupyter notebooks
* `sql` - Apache Spark SQL examples  
* `streaming` - Apache Spark Streaming examples  

# Setup

## Required software

The below are software packages needed for this course:

 * Git
 * Python 3.4+, installed via Anaconda (contains the majority of necessary packages)
 * PySpark (1.6.0+)

## Docker setup

Docker setup requires moderate resources but assures that everyone has a working environment for the training.

Setup steps:

* Download and install Git https://git-scm.com/downloads
* Download and install Docker following the instructions:
    * https://docs.docker.com/windows/
    * https://docs.docker.com/linux/
    * https://docs.docker.com/mac/
    * Note that for Mac OS X and Windows Docker Toolbox is the suggested installation method https://www.docker.com/products/docker-toolbox
* (OS X / Win) Open Docker Quickstart Terminal (use `Terminal`, not `iTerm`)
* Go into this repository
* Build docker `docker-compose build`
* To start Docker run `docker-compose up`
    * If one of the above docker commands fail, run `eval "$(docker-machine env default)"` and then the command, e.g. `docker-compose build`
    * Jupyter runs on port 8888 on localhost on Linux on Docker VM IP available from `docker-machine ip` on Mac OS X and Windows
    * `data` and `notebooks` directories are mounted directly from the host file system
    * Note that the container will close with the current terminal session closure 

## Manual setup

This setup requires least resources but can be difficult on Windows machines.

Setup steps:

* Download and install Git https://git-scm.com/downloads
* Download and install Anaconda Python 3.4+ https://www.continuum.io/downloads
* Download Spark from http://spark.apache.org/downloads.html
    * You sould add Spark to your PYTHONPATH
    * You can also use Findspark package https://github.com/minrk/findspark

# Building Java

Most of the examples are written in Java 8 apart from _Word Count_ examples, which are written in Java 7 and 8 and Scala; see the file suffixes.

The project is build with Apache Maven (http://maven.apache.org).

```bash
mvn clean
mvn install
```
