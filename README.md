# airflow-spark-operator-plugin

[![build passing](https://jenkins-public.personali.io/badge-icon?organization=personali&branch=master&project=airflow-spark-operator-plugin)](http://172.34.1.161:8080/view/Organization/job/personali/job/airflow-spark-operator-plugin/job/master/)

## Description

A plugin to Apache Airflow (Documentation: https://pythonhosted.org/airflow/, Source Code: https://github.com/apache/incubator-airflow) to allow you to run Apache Spark Commands as an Operator from Workflows


## TODO List

* Test extensively

## How do Deploy
 
1. Copy the spark_operator_plugin.py file into the Airflow Plugins directory

    * The Airflow Plugins Directory is defined in the airflow.cfg file as the variable "plugins_folder"
    
    * The Airflow Plugins Directory is, by default, ${AIRFLOW_HOME}/plugins
    
    * You may have to create the Airflow Plugins Directory folder as it is not created by default
 
    * quick way of doing this:
    
        $ cd {AIRFLOW_PLUGINS_FOLDER}
        $ wget https://raw.githubusercontent.com/rssanders3/airflow-zip-operator-plugin/master/zip_operator_plugin.py
 
2. Restart the Airflow Services

3. Create or Deploy DAGs which utilize the Operator

4. You're done!


## Spark Submit Operator

### Operator Definition

class **airflow.operators.SparkSubmitOperator**(application_file, main_class=None, master=None, conf=None, deploy_mode=None, other_spark_options=None, application_args=None, xcom_push=False, env=None, output_encoding='utf-8', *args, **kwargs)

Bases: **airflow.operators.BashOperator**

An operator which executes the spark-submit command through Airflow. This operator accepts all the desired arguments and assembles the spark-submit command which is then executed by the BashOperator.  

Parameters:

* **main_class** (string) - The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
* **master** (string) - The master value for the cluster. (e.g. spark://23.195.26.187:7077 or yarn-client)
* **conf** (string) - Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap “key=value” in quotes. (templated)
* **deploy_mode** (string) - Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (default: client) 
* **other_spark_options** (string) - Other options you would like to pass to the spark submit command that isn't covered by the current options. (e.g. --files /path/to/file.xml) (templated)
* **application_file** (string) - Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an hdfs:// path or a file:// path that is present on all nodes.
* **application_args** (string) - Arguments passed to the main method of your main class, if any. (templated)
* **xcom_push**  (bool) – If xcom_push is True, the last line written to stdout will also be pushed to an XCom when the bash command completes.
* **env** (dict) – If env is not None, it must be a mapping that defines the environment variables for the new process; these are used instead of inheriting the current process environment, which is the default behavior. (templated)


### Prerequisites

The executors need to have access to the spark-submit command on the local commandline shell. Spark libraries will need to be installed.


## Steps done by the Operator

1. Accept all the required input
2. Assemble the spark-submit command
3. Execute the spark-submit command on the executor node


### How to use the Operator

There are some examples on how to use the operator under example_dags.

Import the SparkSubmitOperator using the following line:

    ```
    from airflow.operators import SparkSubmitOperator
    ```


## Livy Spark Operator

### Operator Definition

class **airflow.operators.LivySparkOperator**(spark_script, session_kind="spark", http_conn_id=None, poll_interval=30, *args, **kwargs)

Bases: **airflow.models.BaseOperator**

Operator to facilitate interacting with the Livy Server which executes Apache Spark code via a REST API.

Parameters:

* **spark_script** (string) - Scala, Python or R code to submit to the Livy Server (templated)
* **session_kind** (string) - Type of session to setup with Livy. This will determine which type of code will be accepted. Possible values include "spark" (executes Scala code), "pyspark" (executes Python code) or "sparkr" (executes R code).
* **http_conn_id** (string) - The http connection to run the operator against.
* **poll_interval** (integer) - The polling interval to use when checking if the code in spark_script has finished executing. In seconds. (default: 30 seconds)


### Prerequisites

1. The Livy Server needs to be setup on the desired server.
    
    * Livy Source Code: https://github.com/cloudera/livy

2. Add an entry to the Connections list that points to the Livy Server
 
    1. Open the Airflow WebServer
    2. Navigate to Admin -> Connections
    3. Create a new connection
        
      * Set the Conn Id as some unique value to identify it (example: livy_http_conn) and use this value as the http_conn_id
      * Set the Conn Type as "http"
      * Set the host
      * Set the port (default for livy is 8998)


## Steps done by the Operator

1. Accept all the required inputs
2. Establish an HTTP Connection with the Livy Server via the information provided in the http_conn_id
3. Create a dedicated Livy Spark Session for the execution of the Spark script provided
4. Submit the Spark script code
5. Poll to see if the Spark script has completed running
6. Print the logs and the output of the Spark script
7. Close the Livy Spark Session


### How to use the Operator

There are some examples on how to use the operator under example_dags.

Import the LivySparkOperator using the following line:

    ```
    from airflow.operators import LivySparkOperator
    ```
