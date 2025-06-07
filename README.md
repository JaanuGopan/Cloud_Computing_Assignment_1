# 📊 Cloud Computing Assignment: Student Performance Analysis with Hadoop MapReduce

This project analyzes student performance data using Hadoop MapReduce. Outputs are generated in both **normal** and **table-formatted** formats using multiple jobs.

---

## 🐳 Step 1: Set Up Hadoop in Docker

1. Create a `docker-compose.yml` file and paste the following:

    ```yaml
    version: "2"
    services:
      namenode:
        image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
        container_name: namenode
        environment:
          - CLUSTER_NAME=test
          - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
          - HDFS_CONF_dfs_replication=1
        ports:
          - "9870:9870"
          - "9000:9000"
        volumes:
          - namenode:/hadoop/dfs/name
        networks:
          - hadoop

      datanode:
        image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
        container_name: datanode
        environment:
          - CLUSTER_NAME=test
          - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
          - HDFS_CONF_dfs_replication=1
        ports:
          - "9864:9864"
        volumes:
          - datanode:/hadoop/dfs/data
        networks:
          - hadoop
        depends_on:
          - namenode

    volumes:
      namenode:
      datanode:

    networks:
      hadoop:
    ```

2. Start the cluster:
    ```bash
    docker-compose up -d
    ```

3. Check the Hadoop NameNode UI at: [http://localhost:9870](http://localhost:9870)

---

## 🗃️ Step 2: Dataset

Download the **test** "Student Performance" dataset from Kaggle and place it in your `~/Downloads/Dataset/` directory as `test.csv` and rename it as student_data.csv.
https://www.kaggle.com/datasets/neuralsorcerer/student-performance
```
~/Downloads/
└── Datasets/
    ├── test.csv -> rename it as student_data.csv
```
---

## 🚀 Step 3: Clone the Repository

```bash
git clone https://github.com/JaanuGopan/Cloud_Computing_Assignment_1.git
cd Cloud_Computing_Assignment_1
```

---

## ⚙️ Step 4: Build with Maven

```bash
mvn clean package
```

> Output: `target/CloudComputingAssignment-1.0-SNAPSHOT.jar`

---

## 📁 Step 5: Copy Files to Docker

### Copy JAR to `namenode`:
```bash
docker cp target/CloudComputingAssignment-1.0-SNAPSHOT.jar namenode:/assignment/
```

### Copy dataset:
```bash
docker cp ~/Downloads/Dataset/student_data.csv namenode:/assignment/input/student_data.csv
```

---

## 🛠️ Step 6: HDFS Setup

### Access the container:
```bash
docker exec -it namenode bash
```

### Inside the container:

```bash
hdfs dfs -mkdir -p /assignment/input
hdfs dfs -put /assignment/input/student_data.csv /assignment/input/
```

---

## 🏃 Step 7: Run MapReduce Jobs

### Run Normal Output Jobs:
```bash
hadoop jar /assignment/CloudComputingAssignment-1.0-SNAPSHOT.jar org.cloudcomputing.StudentPerformanceMapReduce /assignment/input/student_data.csv /assignment/output/normal
```

### Run Table-Formatted Output Jobs:
```bash
hadoop jar /assignment/CloudComputingAssignment-1.0-SNAPSHOT.jar org.cloudcomputing.StudentPerformanceMapReduceWithPivot /assignment/input/student_data.csv /assignment/output/table
```

---

## 📂 HDFS Output Structure

```
/assignment/output/
├── normal/
│   ├── job1_GenderSchoolType_GPAClass/
│   ├── job2_SchoolTypeLocale_InternetAccess/
│   ├── job3_FreeTimePartTime_GPAClass/
│   └── job4_InternetRelationship_GPAClass/
└── table/
    ├── job1_GenderSchoolType_GPAClass/
    ├── job2_SchoolTypeLocale_InternetAccess/
    ├── job3_FreeTimePartTime_Pivot/
    └── job4_InternetRelationship_GPAClass/
```

---

## 📤 Step 8: View Results

### 🔹 Normal Output:
```bash
hdfs dfs -cat /assignment/output/normal/job1_GenderSchoolType_GPAClass/part-r-00000
hdfs dfs -cat /assignment/output/normal/job2_SchoolTypeLocale_InternetAccess/part-r-00000
hdfs dfs -cat /assignment/output/normal/job3_FreeTimePartTime_GPAClass/part-r-00000
hdfs dfs -cat /assignment/output/normal/job4_InternetRelationship_GPAClass/part-r-00000
```

### 🔸 Table-Formatted Output:
```bash
hdfs dfs -cat /assignment/output/table/job1_GenderSchoolType_GPAClass/part-r-00000
hdfs dfs -cat /assignment/output/table/job2_SchoolTypeLocale_InternetAccess/part-r-00000
hdfs dfs -cat /assignment/output/table/job3_FreeTimePartTime_Pivot/part-r-00000
hdfs dfs -cat /assignment/output/table/job4_InternetRelationship_GPAClass/part-r-00000
```

---

