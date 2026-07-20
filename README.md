# PySpark: Transforming Data From Hive Distributed Warehouse To Kafka Events Format

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black) ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka) ![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white) ![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white) ![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)

Welcome to the PySpark: Data Transformation project repository. This project covers end-to-end ETL processes; ingesting data from a source, tranforming via Apache Spark and load to target destination and optimizing Spark compute requirements based on size of data being process. Designed as a portfolio project, it highlights production-grade best practices in data engineering field.

## 📌 Project Overview
this project focuses on:
- **The end-to-end data processing** - Data Extraction, Transformation and Load (ETL) via Apache Spark
- **Modular programming practices** - Breaking huge line of codes as a single, separate functions for easier debugging and unit testing
- **CI/CD** - Introductory of GitHub as a version control tool and high level overview of how automated CI/CD works behind the scene

## 📑 Project Background
### A. Problem Statement
You are working in a multinational company as  data engineer. The Chief Data Officer (CDO) sent an email to the entire data & analytics team saying that the board executives are planning to implement a new approach of handling data transfer between Master Data Management (MDM) to the downstream systems. During the meeting with the leaders, you learned that the organization is looking for the opportunity to integrate Confluent Kafka as a bridge between the MDM and the downstream systems.

The reason behind the decision is as follow:
- Currently, data moves from MDM to downstream systems via customized APIs. As recent tech moves rapidly where data are now able to be processed in petabytes as well as the emergence of new analytics and machine learning tools, the downstream users are now able to analyze data in lightning faster rate. 
- Moreover, the increase of data demand from the newly developed downstream systems penalized the performance of the current data pipelines. Thus, making the legacy architecture un-scalable.

### B. System Architecture
**Legacy Architecture**
<br><br>
<img width="3036" height="1408" alt="MDM Architecture" src="https://github.com/user-attachments/assets/f1d25e46-65e1-4130-bb88-f716120f3e67" />
<br><br>

1. The MDM platform acts as the single source of repository, where it clones all the transformed transactional data across all line of businesses (LOB) in the organization.
2. Amazon S3 is used to host the MDM platform.
3. The MDM team built thousands of granular security policies inside Apache Ranger or Sentry. These policies are tightly coupled to S3 Apache Hive Metastore (HMS). 
4. The MDM is managed by a different data team which is out of our team scope.
5. Initially, our team developed custom internal API to fetch data from MDM and feed the data to the downstream system such as customer churn, fraud detection, compliance & risks.
6. The legacy workflow unable to scale with the demand of data requested from the downstream system where the custom API could not handle multi parallel requests in gigabytes simultaneously.
7. Additinally, the legacy workflow also requires expertise in tweaking and adjusting the custom APIs if there is any changes of data structures or schemas from the MDM platform. This led to complexity in tracking the changes as well as ensuring readability of the scripts.

### C. Desired Output
<img width="3184" height="1344" alt="Kafka connection" src="https://github.com/user-attachments/assets/31e033a8-89b3-46d2-93f8-85ada4858194" />
<br><br>

1. New connection pipeline that leverage Confluent Kafka as a bridge to move data from MDM to downstream systems.
2. To leverage the maximum throughput of Confluent kafka, data from MDM needs to be transform to a key value pair with a custome json-like format as a Kafka events.
3. The organization is looking for an implementation of Apache Spark as the transformation tool. Apache spark will be hosted in Amazon EMR.

### D. Objectives
1. Develop Apache kafka scripts to transform the data from MDM based on specific rquirements for each downstream system and load to Confluent kafka.
2. Ensure readability and modular programming methodolodies are implemented.
3. Spark scripts need to be developed locally via any preferable IDEs i.e. PyCharm, VSC.
4. Unit testing is done locally in machine during the building/developmental stage to avoid any compute cost consumption.
5. Anonymized Production Snapshots (data sample) that has been sanitized/masked to comply with privacy regulations (like GDPR or HIPAA) will be provided every month.
6. Estimate and optimize Spark computation requirements.

## Project Requirements and Resources
#### 1. [Project Milestone](https://app.notion.com/p/PySpark-Transforming-Data-From-Hive-Distributed-Warehouse-To-Kafka-Events-Format-39a7d7cd41a6809eae92e3886cdb6754?source=copy_link) 
A comprehensive checkpoint in a project timeline that marks a major event, phase completion, or key deliverables.

#### 2. Repository Structure
Clear file and folder structure that follows modular design, isolation and breaking large complex code into a small function unit.

### 3. 
