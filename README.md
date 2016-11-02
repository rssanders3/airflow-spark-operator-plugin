# airflow-spark-operator-plugin

## Description

A plugin to Apache Airflow to allow you to run Spark Submit Commands as an Operator


## Operator Definition

class **airflow.operators.SparkSubmitOperator**(application_file, main_class=None, master=None, conf=None, deploy_mode=None, other_spark_options=None, application_args=None, xcom_push=False, env=None, output_encoding='utf-8', *args, **kwargs)

Bases: airflow.operators.BashOperator

An operator which executes the spark-submit command through Airflow. This operator accepts all the desired arguments and assembles the spark-submit command which is then executed by the BashOperator.  

Parameters:

* **main_class** (string) - The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
* **master** (string) - The master value for the cluster. (e.g. spark://23.195.26.187:7077 or yarn-client)
* **conf** (string) - Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap “key=value” in quotes. (templated)
* **deploy_mode** (string) - Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client) (default: client) 
* **other_spark_options** (string) - Other options you would like to pass to the spark submit command that isn't covered by the current options. (e.g. --files /path/to/file.xml) (templated)
* **application_file** (string) -  Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an hdfs:// path or a file:// path that is present on all nodes.
* **application_args** (string) - Arguments passed to the main method of your main class, if any. (templated)
* **xcom_push**  (bool) – If xcom_push is True, the last line written to stdout will also be pushed to an XCom when the bash command completes.
* **env** (dict) – If env is not None, it must be a mapping that defines the environment variables for the new process; these are used instead of inheriting the current process environment, which is the default behavior. (templated)


## Prerequisites

The executors need to have access to the spark-submit command on the local commandline shell. Spark libraries will need to be installed.


## How do Deploy
 
1. Copy the spark_operator_plugin.py file into the Airflow Plugins directory

    * The Airflow Plugins Directory is defined in the airflow.cfg file as the variable "plugins_folder"
    
    * The Airflow Plugins Directory is, by default, ${AIRFLOW_HOME}/plugins
    
    * You may have to create the Airflow Plugins Directory folder as it is not created by default
 
2. Restart the Airflow Services

3. Your done!


## How to use the Operator

There are some examples on how to use the operator under example_dags.

Import the SparkSubmitOperator using the following line:

    ```
    from airflow.operators import SparkSubmitOperator
    ```
