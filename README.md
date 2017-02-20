# Run TPCH Benchmark on Apache Kylin

This is a derived work from https://github.com/hortonworks/hive-testbench

This benchmark requires Apache Kylin 2.0 or later.

1. Prepare TPCH test data

   - Follow [the steps](https://github.com/hortonworks/hive-testbench) to setup test data in Hive.
     - For example, generate minimal data with ScaleFactor=2
       ```sh
       git clone https://github.com/hortonworks/hive-testbench.git
       cd hive-testbench/
       ./tpch-build.sh
       ./tpch-setup.sh 2
       ```

     - Verify TPCH tables existed in hive
       ```sh
       echo "use tpch_flat_orc_2; show tables;" | hive
       ```

2. Import Kylin model

   - Get this repository.
     ```sh
     git clone https://github.com/Kyligence/kylin-tpch.git
     ```

   - Make sure there is a running Kylin instance.

     ```sh
     export KYLIN_HOME=...
     ```

   - Run the `setup-kylin-model.sh` with the SAME SCALE FACTOR to generate hive data, for example:

     ```sh
     cd kylin-tpch
     ./setup-kylin-model.sh 2
     ```

   - The script also creates a few simple views on top of the original TPCH tables to allow Kylin pre-calculate some complex measures. The resulted E-R model topology is identical to the original TPCH model.

3. Build Kylin Cube

   -  In the Kylin web, click "System->Reload Metadata" to refresh the newly imported TPCH model.

   -  Select project "tpch" and find the 4 cubes below.
      - lineitem_cube
      - partsupp_cube
      - customer_cube
      - customer_vorder_cube

   -  `lineitem_cube` and `partsupp_cube` together covers 20 TPCH queries out of the total 22. Besides, `customer_vorder_cube` is for query 13 and `customer_cube` is for query 22.

   -  Build all cubes within the year range from 1992 to 1998.

   -  Verify by counting `v_lineitem` in Kylin. The row count must match the Hive table `lineitem`.

      ```sql
       select count(*) from v_lineitem
      ```

4. Run TPCH queries

   - Find the TPCH queries under the `query` folder. Run them via the Kylin web, or Kylin REST API / JDBC / ODBC interfaces.
   - The queries are slightly re-written to leverage the views created in Step 2 and work around limitations like [KYLIN-2341](https://issues.apache.org/jira/browse/KYLIN-2341).
   - The original queries (from hive-testbench) can be found at `queries/original-queries`. The original and the re-written queries should give the same result in Hive and Kylin respectively.
   - There is a tool to run all queries one by one. It can be found at `tools/query-tool.py`. For kylin, it prints query duration. For hive, it prints query duration and query results.
     * Install dependencies
     ```sh
     pip install requests
     ```
     * Run kylin query
     ```sh
     python tools/query-tool.py -s http://127.0.0.1:7070/kylin -u ADMIN -p KYLIN -d queries -o tpch -r 3 -t kylin
     ```
     * Run hive query
     ```sh
     python tools/query-tool.py -t hive -c 10
     ```



## Experiment Result

This benchmark was tested in two environments. One enviroment is HDP sandbox, another is a 4-node CDH cluster. The query speed is compared with Hive for reference.

| Cube Build Stats     | HDP Sandbox (SF=2)    | CDH 4-Node Cluster (SF=10) |
| -------------------- | --------------------- | -------------------------- |
| lineitem_cube        | 18.1 GB, 125 mins * 7 | 85.8 GB, 54.69 mins * 7    |
| partsupp_cube        | 1.6 GB, 27.02 mins    | 8.3 GB, 18.48 mins         |
| customer_vorder_cube | 283 MB, 16.88 mins    | 1.39 GB, 13.7 mins         |
| customer_cube        | 7 MB, 3.95 mins       | 36.98 MB, 3.97 mins        |

(The lineitem_cube was built incrementally. Each increment builds 1 years data.)

| Query Response Time | Kylin on HDP (SF=2) | Hive+Tez on HDP (SF=2) | Kylin on CDH (SF=10) | Hive+MR on CDH (SF=10) |
| ------------------- | ------------------- | ---------------------- | -------------------- | ---------------------- |
| query01             | 0.43 sec            | 13.79 sec              | 0.45 sec             | 43.45 sec              |
| query02             | 8.87 sec            | 17.04 sec              | 8.77 sec             | 3570 sec               |
| query03             | 5.2 sec             | 16.32 sec              | 4.84 sec             | 137.4 sec              |
| query04             | 0.71 sec            | 31.23 sec              | 2.69 sec             | 90.99 sec              |
| query05             | 0.33 sec            | 23.68 sec              | 0.47 sec             | 162.79 sec             |
| query06             | 0.34 sec            | 8.91 sec               | 0.39 sec             | 23.29 sec              |
| query07             | 0.17 sec            | 35.23 sec              | 0.24 sec             | 245.52 sec             |
| query08             | 7.42 sec            | 19.4 sec               | 8.82 sec             | 2248.5 sec             |
| query09             | 10.4 sec            | 30.75 sec              | 34.8 sec             | 19869 sec              |
| query10             | 21.1 sec            | 19.03 sec              | 11.91 sec            | 120.61 sec             |
| query11             | 3.42 sec            | 15.87 sec              | 2.09 sec             | 123.3 sec              |
| query12             | 7.66 sec            | 12.64 sec              | 5.73 sec             | 96.34 sec              |
| query13             | 20.44 sec           | 24.61 sec              | 104.77 sec           | 135.64 sec             |
| query14             | 1.1 sec             | 4.54 sec               | 1.56 sec             | 54.42 sec              |
| query15             | 1.96 sec            | 13.06 sec              | 17.02 sec            | 124.29 sec             |
| query16             | 3.69 sec            | 17.36 sec              | 10.16 sec            | 144.34 sec             |
| query17             | 5.49 sec            | 25.61 sec              | 10.19 sec            | 183.56 sec             |
| query18             | 22.43 sec           | 65.32 sec              | 74.88 sec            | 231.9 sec              |
| query19             | 21.9 sec            | 22.5 sec               | 138.81 sec           | 186.87 sec             |
| query20             | 6.46 sec            | 16.21 sec              | 42.02 sec            | 229.01 sec             |
| query21             | 58.6 sec            | 84.6 sec               | 136.3 sec            | 276.59 sec             |
| query22             | 23.21 sec           | 21.79 sec              | 102.81 sec           | 154.48 sec             |


**HDP Sandbox Configuration**

* HDP sandbox (version 2.4.0.0-169) VM on VirtualBox
* 4 vcores, 10 GB memory, ScaleFactor=2
* Because of the limited resource, in depth tuning is required to run through the benchmark
* More resource to Tez
  * hive.tez.java.opts -Xmx=512m
  * tez container heap 1G
* More resource to HBase
  - hbase region server heap 4G
* Kylin be easy on HBase
  * kylin.storage.hbase.coprocessor-timeout-seconds=3600
  * kylin.storage.hbase.max-hconnection-threads=5
  * kylin.storage.hbase.core-hconnection-threads=5
  * kylin.storage.hbase.coprocessor-mem-gb=30
  * kylin.storage.partition.aggr-spill-enabled=false
  * kylin.storage.hbase.compression-codec=snappy
  * kylin.query.scan-threshold=1000000000
* YARN config
  * 5 GB memory, 3 vcores
* MR config
  * mapreduce.job.reduce.slowstart.completedmaps=1
  * enable compression org.apache.hadoop.io.compress.SnappyCodec

**CDH 4-Node Cluster Configuration**

- Cloudera 5.8.3
- 1 Master (32 vcores, 192 GB memory), 3 Slave (32 vcores, 96 GB memory)
- ScaleFactor = 10
- YARN config
  * 200 GB memory, 128 vcores
- Kylin config
  * kylin.storage.hbase.compression-codec=snappy
  * kylin.query.scan-threshold=1000000000
  * kylin.storage.hbase.coprocessor-mem-gb=30
  * set kylin JVM -Xms4g -Xmx8g

