# Apache Spark: WordCount & K-Means Clustering

This project contains **Java** implementations of two fundamental MapReduce algorithms—**WordCount** and **K-Means Clustering**—using **Apache Spark 4.0**. It explores the performance benefits of Spark's in-memory processing compared to traditional disk-based Hadoop MapReduce.

## 📂 Project Overview

The project focuses on "Digital Intelligence & Scientific Discovery" by benchmarking Spark's efficiency.
* **WordCount:** Counts word frequencies in a text file and sorts them.
* **K-Means:** Clusters the **Iris dataset** into 3 groups using 5 iterations of the algorithm.
* **Performance:** Spark (~2.0s) significantly outperforms Hadoop MapReduce (~45s) on the same dataset due to in-memory caching.

## 🛠️ Prerequisites

* **Java:** JDK 11
* **Maven:** 3.8+ (for building the fat JAR)
* **Apache Spark:** 4.0.1
* **Hadoop/HDFS:** Configured and running (Optional if running locally, required for HDFS paths).

## 🚀 Installation & Build

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/yourusername/Apache-Spark-WordCount-KMeans.git](https://github.com/yourusername/Apache-Spark-WordCount-KMeans.git)
    cd Apache-Spark-WordCount-KMeans
    ```

2.  **Build the JAR:**
    This project uses the `maven-shade-plugin` to create a "fat JAR" containing all dependencies.
    ```bash
    mvn clean package
    ```
    *The output file `wordcount-1.0.jar` will be created in the `target/` directory.*

## 💻 Usage

### 1. Running WordCount
The WordCount job reads a text file and outputs the frequency of each word.

**Input:** `test.txt`
**Command:**
```bash
# Syntax: java -jar <jar-file> <input-path> <output-path>
java -cp target/wordcount-1.0.jar com.geekcap.javaworld.sparkexample.WordCount test.txt output/wordcount_results
Note: The code is hardcoded to use local[*] master, so spark-submit is not strictly required if the classpath is correct.2. Running K-Means ClusteringThe K-Means job clusters the Iris flower dataset.Input: iris.dataCommand:Bashjava -cp target/wordcount-1.0.jar com.geekcap.javaworld.sparkexample.SparkKMeans iris.data output/kmeans_results
📊 Results & BenchmarksWordCount OutputThe program successfully counts word occurrences. Example output from test.txt:Plaintext(spark, 7)
(hadoop, 5)
(big, 3)
(data, 3)
...
K-Means ConvergenceThe algorithm converges after 5 iterations. Final Centroids:Cluster 1: [5.006, 3.418, 1.464, 0.244]Cluster 2: [5.936, 2.770, 4.260, 1.326]Cluster 3: [6.850, 3.074, 5.742, 2.071]Performance Comparison (5 Iterations)ApproachRuntimeNotesSequential Java~0.15sFastest for tiny data; no overhead.Apache Spark~2.0sFast; uses cache() to keep data in RAM.Hadoop MR~45.0sSlow; reads/writes to disk every iteration.🔧 Troubleshooting & SolutionsDuring development, the following specific issues were encountered and resolved:Security Exception (Invalid Signature)Error: java.lang.SecurityException: Invalid signature file digestCause: The uber-jar included signature files (META-INF/*.RSA) from signed dependencies.Solution: Manually removed signatures from the JAR:Bashzip -d target/wordcount-1.0.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
NumberFormatException (Empty Strings)Error: java.lang.NumberFormatException: empty StringCause: The input dataset contained trailing newlines or empty rows.Solution: Added a .filter() transformation to the RDD pipeline to ignore empty lines before parsing.HDFS Connection RefusedError: Connection refused to localhost:9000.Solution: Restarted the Hadoop cluster services:Bashstart-dfs.sh
start-yarn.sh
