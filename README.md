# PySpark: Transforming Data From Hive Distributed Warehouse To Kafka Events Format

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black) ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka) ![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white) ![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white) ![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)

Welcome to the PySpark: Data Transformation project repository. This project covers end-to-end ETL processes; ingesting data from a source, tranforming via Apache Spark and load to target destination and optimizing Spark compute requirements based on size of data being process. Designed as a portfolio project, it highlights production-grade best practices in data engineering field. 

## 📌 Project Overview
this project focuses on:
- **The end-to-end data processing** - Data Extraction, Transformation and Load (ETL) via Apache Spark
- **Spark complex transformation** - Data transformation that requires the format of data to follow custom JSON key value pairs. This require complex nested format where struct() and array() are implemented in the transformations script.
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

1. The MDM platform acts as the central data management platform, where it designed to synchronize all the transformed transactional data across all line of businesses (LOB) in the organization.
2. Amazon S3 is used to host the MDM platform.
3. The MDM team built thousands of granular security policies inside Apache Ranger or Sentry. These policies are tightly coupled to S3 Apache Hive Metastore (HMS). 
4. The MDM is managed by a different data team which is out of our team scope.
5. The MDM system exposes custom internal API to fetch data from MDM and feed the data to the downstream system such as customer churn, fraud detection, compliance & risks.
6. The legacy workflow unable to scale with the demand of data requested from the downstream system where the custom API could not handle multi parallel requests in gigabytes simultaneously.
7. Additinally, the legacy workflow also requires expertise in tweaking and adjusting the custom APIs if there is any changes of data structures or schemas from the MDM platform. This led to complexity in tracking the changes as well as ensuring readability of the scripts.

### C. Desired Output
<img width="3184" height="1344" alt="desired output" src="https://github.com/user-attachments/assets/b09f2532-c65b-47f9-869d-bb26da9d43fb" />
<br><br>

1. Maintain the architecture of the source system where MDM data is hosted in S3, integrated with Apache HIVE.
2. Since the MDM has already integrated with Apache HIVE, we can read the data easily with the Apache Spark read.table method for transformations.
3. To leverage the maximum throughput of Confluent kafka, data from MDM needs to be transform to a key value pair with a custome json-like format as a Kafka events.
4. The organization is looking for an implementation of Apache Spark as the transformation tool. Apache spark will be hosted in Amazon EMR.
5. The requirement does not ask us to build a real-time stream data. Instead, the pipeline will be running once a day to Kafka.

### D. Objectives
1. Develop Apache kafka scripts to transform the data from MDM based on specific rquirements for each downstream system and load to Confluent kafka.
2. Ensure readability and modular programming design methodolodies are implemented.
3. Spark scripts need to be developed locally via any preferable IDEs i.e. PyCharm, VSC.
4. Unit testing is done locally in machine during the building/developmental stage to avoid any compute cost consumption.
5. The system will be batch dataflow pipeline that will be running once a day. (Depending on the requirements, you may need to run the pipeline hourly).
6. Estimate and optimize Spark computation requirements.

## Project Requirements and Resources

#### Stack
1. Apache Spark - Installed in machine
2. Java - Installed in machine (compatible with the Spark version)
3. Python - Installde in machine (compatible with the Spark version)
4. Confluent Kafka - As an endpoint / sink destination
5. Amazon S3 - Storage
6. Apache HIVE / AWS Glue Catalog - For metadata creation of the data stored in S3
7. IDE - optional

##### [Project Milestone & Guidelines](https://app.notion.com/p/PySpark-Transforming-Data-From-Hive-Distributed-Warehouse-To-Kafka-Events-Format-39a7d7cd41a6809eae92e3886cdb6754?source=copy_link)
A comprehensive checkpoint in a project timeline that marks a major event, phase completion, or key deliverables.
<br><br>

##### Project File Structure 
Modular code separation isolating ETL operations, business logic, session management, and configuration loading.
```bash
MDM_Spark_Project/
├── conf/
│   ├── sbdl.conf             # Application-level configurations (paths, tables, parameters)
│   └── spark.conf            # Spark Session parameters and executor properties
├── lib/
│   ├── __init__.py
│   ├── ConfigLoader.py       # Helper for loading runtime and environment configs
│   ├── DataLoader.py         # Data reader logic for source data ingestion
│   ├── Transformations.py    # Core PySpark business logic & schema transformations
│   ├── Utils.py              # Spark Session builder and utility functions
│   └── logger.py             # Custom Python logger wrapper for tracking ETL execution
├── test_data/
│   ├── accounts/
│   │   └── account_samples.csv
│   ├── parties/
│   │   └── party_samples.csv
│   ├── party_address/
│   │   └── address_samples.csv
│   └── results/
│       └── final_df.json     # Expected execution output for local verification
├── .env                      # Local environment variable definitions
├── .gitignore                # Excludes cache, logs, and OS system files
├── Jenkinsfile               # Automated CI/CD build & deployment pipeline
├── Pipfile                   # Pipenv environment specifications
├── Pipfile.lock              # Explicit package dependency versions
├── ReadMe.md                 # Project README for documentation
├── log4j.properties          # Apache Spark logging configuration
├── sbdl_main.py              # Main driver script / entry point
├── sbdl_submit.sh            # Production spark-submit bash wrapper script
└── test_pytest_sbdl.py       # PyTest suite for data transformation unit tests

----

## Key Components
# sbdl_main.py: Entry point for running the PySpark pipeline.
# lib/: Modular Python packages containing re-usable PySpark logic, data loaders, and configuration handlers.
# conf/: Decoupled config files separating application-level parameters from Spark cluster settings.
# test_data/: Sample inputs and output schemas for automated local testing and PyTest validation ('test pytest_sbdl.py').
# sbdl submit.sh & Jenkinsfile: Shell script and CI/CD pipeline definitions for automated deployment and cluster execution via spark-submit`.
```
<br>

#### Version Control
This project requires to build branches of repository layers to mimicks the actual working scenarios where data engineers work solely in their respective development-stage repo. Data engineers are expected to run unit testing locally before requesting a merge to the development stage. In real working scenarios, we can expect to have CI/CD automation which will auto merge the project repo to the next level of branches without needing to have human intervention.
The branch is as follow:

```txt
                          master
                            ^
                            |
                         release
                            ^
                            |
                       development
                            ^
                            |
                     feature-changes 
```
<br>

#### Automated CI/CD
In real working scenarios, DevOps implement automated CI/CD by adopting automation tools such as Jenkins, GitHub Actions or GitLab CI/CD. Data engineers do not build this automation tools. Instead, data engineers will somehow tweak the automation scripts depending on the requirements or applications that they build. In most times, DevOps develop the script to align with the desired outcomes or testing results and data engineers must ensure that their files, folders and scripts align with the pre-defined Jenkinsfile script. This project assumes that Jenkins is used as the CI/CD automation tool and therefore the creation of Jenkinsfile.

```Jenkinsfile
pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'pipenv --python python3 sync'
            }
        }

        stage('Test') {
            steps {
                sh 'pipenv run pytest'
            }
        }

        stage('Package') {
            steps {
                // This packages your local files into a zip archive on your machine
                sh 'zip -r sbdl.zip lib'
            }
        }

        stage('Local Simulate Deploy') {
            steps {
                // Simulates a deployment locally by printing a success message
                echo "Build completed successfully! Package 'sbdl.zip' is ready in the workspace."
            }
        }
    }
}
```
<br>

#### Password And Credentials
This project requires connection to the source (MDM hosted in S3 with the integration of Apache HIVE) and also the target destination (Confluent Kafka). In common cases where ETL decelopment are done in company-managed environment, data engineers are provided with pre-defined scripts that are already built in the project repositories (depending on the company size). You are rarely to develop your own config files from scratch. To mimic real working scenarios, this project requires building config files and store the credentials on a separate file where it will be passed automatically during the reading of the environment, programmatic fetching at runtime or native integration with orchestrators & park (i.e. Databricks or native Apache Spark config). Locally, the credentials are stored in .env where it will be included in .gitignore.

# Input & Ouput 

### A. Data Source Format
The MDM is stored in Amazon S3 where HIVE is integrated to build the metadata for the data stored in S3. Thus, Spark able to read the source via table format, querying the data via SparkSQL. 

**Source Input Sample**

Accounts Table

<img width="1350" height="393" alt="image" src="https://github.com/user-attachments/assets/a0b9d6b7-f1dd-4bff-93f2-bc0ad95c4df8" />
<br>

Party Table

<img width="664" height="466" alt="image" src="https://github.com/user-attachments/assets/edeb90f1-9767-4126-ad55-1dde9f630aa7" />
<br>

Party Address Table

<img width="1151" height="387" alt="image" src="https://github.com/user-attachments/assets/59ff956f-a925-4b2f-b082-68e958e28535" />


### B. Sink Format
Data ingested from source was then transformed into a complex JSON key value pair format before sinking it to Confluent Kafka. The sample output of the transformation is as follow:

```JSON
{
  "eventHeader": {
    "eventIdentifier": "c361a145-d2fc-434e-a608-9688caa6d22e",
    "eventType": "SBDL-Contract",
    "majorSchemaVersion": 1,
    "minorSchemaVersion": 0,
    "eventDateTime": "2022-09-06T20:49:03+0530"
  },
  "keys": [
    {
      "keyField": "contractIdentifier",
      "keyValue": "6982391060"
    }
  ],
  "payload": {
    "contractIdentifier": {
      "operation": "INSERT",
      "newValue": "6982391060"
    },
    "sourceSystemIdentifier": {
      "operation": "INSERT",
      "newValue": "COH"
    },
    "contactStartDateTime": {
      "operation": "INSERT",
      "newValue": "2018-03-24T13:56:45.000+05:30"
    },
    "contractTitle": {
      "operation": "INSERT",
      "newValue": [
        {
          "contractTitleLineType": "lgl_ttl_ln_1",
          "contractTitleLine": "Tiffany Riley"
        },
        {
          "contractTitleLineType": "lgl_ttl_ln_2",
          "contractTitleLine": "Matthew Davies"
        }
      ]
    },
    "taxIdentifier": {
      "operation": "INSERT",
      "newValue": {
        "taxIdType": "EIN",
        "taxId": "ZLCK91795330413525"
      }
    },
    "contractBranchCode": {
      "operation": "INSERT",
      "newValue": "ACXMGBA5"
    },
    "contractCountry": {
      "operation": "INSERT",
      "newValue": "Mexico"
    },
    "partyRelations": [
      {
        "partyIdentifier": {
          "operation": "INSERT",
          "newValue": "9823462810"
        },
        "partyRelationshipType": {
          "operation": "INSERT",
          "newValue": "F-N"
        },
        "partyRelationStartDateTime": {
          "operation": "INSERT",
          "newValue": "2019-07-29T06:21:32.000+05:30"
        },
        "partyAddress": {
          "operation": "INSERT",
          "newValue": {
            "addressLine1": "45229 Drake Route",
            "addressLine2": "13306 Corey Point",
            "addressCity": "Shanefort",
            "addressPostalCode": "77163",
            "addressCountry": "Canada",
            "addressStartDate": "2019-02-26"
          }
        }
      }
    ]
  }
}
```

# Spark Resource Requirements
Before we start discussing the resouces required for Spark in order to achieve optimum throughput while minimizing the cost, lets understand the architecture of Spark.
<br>

<img width="1699" height="664" alt="image" src="https://github.com/user-attachments/assets/ba01acc9-2c32-4fbf-9ae6-cde7db69d120" />

<br>

Spark consists of master node and executor nodes. In a production environment, where Spark lives in remote servers as clusters.

### A. Master nodes
master nodes do the following:
1. Create a Python worker process for PySpark
2. Create a JVM process for running the Spark driver process.
3. Py4J tunnel for communicating between Python and JVM

Within the master node, we have a driver that do the following:
1. Execution plan, Partitioning strategy, trigger jobs, allocate partitions and code to tasks and monitor the progress
2. Collect and distribute any broadcast data
3. Collect data at the driver caused by collect() or take() actions

Generally, data engineers adopt the practice of providing 2 CPU cores. If multithreading is required, then data engineers will the amount of cores needed for the job. For the memory, it is agreeable among many data engineers that each core will have at least 2 GB or RAM. The memory is then divided equally for data processing as well as data collecting, making the overall driver memory equivalent to 1 GB per each core. With this setup, the overall overhead memory for spark driver will be 400 MB (search the resource allocation in browser for more details). Sometimes you will receive 'overhead memory exceeded' error due to heavy data collection in the driver. Therefore, to be save, we will allocate 1 GB memory overhead to the project.

> spark.driver.cores = 2

> spark.driver.memory = 4 GB

> spark.driver.memory.overHead = 1 GB

### B. Executor nodes

Executor nodes do the heavy lifting work, reading the dataframe, transform, sort, shuffle or even partition of the data. As a starter, we will allocate 5 cores for each executor nodes. This will led to 10 GB of RAM. But you should also understand how Spark will use this 10 GB. Spark will take out 300MB for running the executor process itself. That's called reserved memory, which is fixed at 300MB per executor. Then you will give only 60% of the remaining for the executors. So you have only 5820 MB for the 5 cores to do data processing work. But 50% of this memory can be taken up by data caching. So you are left with 2910 MB for 5 CPU cores. So end of the day, you will have 582 MB per core. General thumb rule, we should provision our data partition, X. If you X number of partitions, then memory allocation should be 4X.

Let's assume that the data partition size is 128 MB. So 4X of 128 MB comes to 512MB. That fits in 582 MB per core. The spark.executor.memoryOverhead is also allocated the same as in the driver, 1 GB.

> spark.executor.cores = 5

> spark.executor.memory = 10G

> spark.executor.memoryOverhead = 1G

### C. Number of executors

Generally, number of executors are depends on data volume and the number of partitions that we have during the data processing and transformations. Assume that we have 10 partitions and we already have 5 cores for each executor. To run the job in parallel, having 2 executors are logical in this scenario. But it is not that easy to know the partition count. To tell how many partitions or how big is the data volume, the easiest method is to run your application in a QA environment and capture these details. We can monitor this in the execution plan. Refer to below visualisation:


<img width="1729" height="899" alt="image" src="https://github.com/user-attachments/assets/c6c5f3df-4a44-4d44-846c-d115b1f3f151" />

The upper measurements are actual calculation based on the execution plan. The lower measurements are the planned execution based on 800 shuffle partitions.

<br>
We configured the Spark memory to get 500 MB for each core. We also have 500 MB for data caching but we don't do data caching in this project so the actual capacity is 1GB per core. We have 800+400+480 = 1680 partitions for the first three stages. If we have 1680 cores, we can run the first three stages in parallel. When we reach stage 4, we will require only 800 cores because we have only 800 partitions. However, it is resource redundant if we allocate 1680 cores since it is for the first three stages and stage 4 and 5 only require 800 partitions. Thefore, we will implement 800 shuffle partitions which is equivalent to 800 cores. The executor comes with 5 CPU cores. So, to achieve maximum parallelism, the best is to get 160 executors.
<br><br>

> spark.executor.instances = 160

<br><br><br><br>
_License: Distributed under the MIT License. See [LICENSE](https://opensource.org/license/mit) for more information._
_Follow me on LinkedIn: https://www.linkedin.com/in/mohdnuriqhwan/_








































