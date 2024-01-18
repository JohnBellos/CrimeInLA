# CrimeInLA

## Overview

This project involves analyzing crime data in Los Angeles using Apache Spark. It includes queries implemented in DataFrame, RDD and SQL APIs.

## Installation Requirements

You can check out this setup guide provided by our professors: 
- [Setup guide](https://colab.research.google.com/drive/1eE5FXf78Vz0KmBK5W8d4EUvEFATrVLmr?usp=drive_link)

Before running the scripts, ensure that you have the following prerequisites:

1. **Cluster Setup:**
   - Install and configure at least 2 Ubuntu 22.04 clusters.
   - Set up Apache Spark, Java, Hadoop Distributed File System (HDFS), and YARN on each cluster.

2. **Access to UIs:**
   - Ensure proper configuration for accessing Spark, HDFS, and YARN UIs.

3. **Download Datasets:**
   - Download the basic crime datasets:
     - [Crime Data from 2010 to 2019](https://catalog.data.gov/dataset/crime-data-from-2010-to-2019)
     - [Crime Data from 2020 to Present](https://catalog.data.gov/dataset/crime-data-from-2020-to-present)
   - Download income 2015 dataset and reverse geocoding dataset:
     - [Data Tarball](http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz)

4. **Store Datasets in HDFS:**
   - Store downloaded datasets in the HDFS of your machine.

## Running the Scripts

Follow these steps to run the scripts (for instance Query 1 with DataFrame API):

Clone this repository:
   ```bash
   git clone https://github.com/ntua-el19613/CrimeInLA
   cd CrimeInLA
   cd query1
   spark-submit Q1DF.py 
