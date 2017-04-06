# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.plugins_manager import AirflowPlugin
from airflow.hooks import HttpHook
from airflow.models import BaseOperator
from airflow.operators import BashOperator
from airflow.utils import apply_defaults
import logging
import textwrap
import time
import json


class SparkSubmitOperator(BashOperator):
    """
   An operator which executes the spark-submit command through Airflow. This operator accepts all the desired
   arguments and assembles the spark-submit command which is then executed by the BashOperator.

   :param application_file: Path to a bundled jar including your application
        and all dependencies. The URL must be globally visible inside of
        your cluster, for instance, an hdfs:// path or a file:// path
        that is present on all nodes.
   :type application_file: string
   :param main_class: The entry point for your application
        (e.g. org.apache.spark.examples.SparkPi)
   :type main_class: string
   :param master: The master value for the cluster.
        (e.g. spark://23.195.26.187:7077 or yarn-client)
   :type master: string
   :param conf: Arbitrary Spark configuration property in key=value format.
        For values that contain spaces wrap “key=value” in quotes.
   :type conf: string
   :param deploy_mode: Whether to deploy your driver on the worker nodes
        (cluster) or locally as an external client (default: client)
   :type deploy_mode: string
   :param other_spark_options: Other options you would like to pass to
        the spark submit command that isn't covered by the current
        options. (e.g. --files /path/to/file.xml)
   :type other_spark_options: string
   :param application_args: Arguments passed to the main method of your
        main class, if any.
   :type application_args: string
   :param xcom_push: If xcom_push is True, the last line written to stdout
       will also be pushed to an XCom when the bash command completes.
   :type xcom_push: bool
   :param env: If env is not None, it must be a mapping that defines the
       environment variables for the new process; these are used instead
       of inheriting the current process environment, which is the default
       behavior. (templated)
   :type env: dict
   :type output_encoding: output encoding of bash command
   """

    template_fields = ('conf', 'other_spark_options', 'application_args', 'env')
    template_ext = []
    ui_color = '#e47128'  # Apache Spark's Main Color: Orange

    @apply_defaults
    def __init__(
            self,
            application_file,
            main_class=None,
            master=None,
            conf=None,
            deploy_mode=None,
            other_spark_options=None,
            application_args=None,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):
        self.bash_command = ""
        self.env = env
        self.output_encoding = output_encoding
        self.xcom_push_flag = xcom_push
        super(SparkSubmitOperator, self).__init__(bash_command=self.bash_command, xcom_push=xcom_push, env=env, output_encoding=output_encoding, *args, **kwargs)
        self.application_file = application_file
        self.main_class = main_class
        self.master = master
        self.conf = conf
        self.deploy_mode = deploy_mode
        self.other_spark_options = other_spark_options
        self.application_args = application_args

    def execute(self, context):
        logging.info("Executing SparkSubmitOperator.execute(context)")

        self.bash_command = "spark-submit "
        if self.is_not_null_and_is_not_empty_str(self.main_class):
            self.bash_command += "--class " + self.main_class + " "
        if self.is_not_null_and_is_not_empty_str(self.master):
            self.bash_command += "--master " + self.master + " "
        if self.is_not_null_and_is_not_empty_str(self.deploy_mode):
            self.bash_command += "--deploy-mode " + self.deploy_mode + " "
        if self.is_not_null_and_is_not_empty_str(self.conf):
            self.bash_command += "--conf " + self.conf + " "
        if self.is_not_null_and_is_not_empty_str(self.other_spark_options):
            self.bash_command += self.other_spark_options + " "

        self.bash_command += self.application_file + " "

        if self.is_not_null_and_is_not_empty_str(self.application_args):
            self.bash_command += self.application_args + " "

        logging.info("Finished assembling bash_command in SparkSubmitOperator: " + str(self.bash_command))

        logging.info("Executing bash execute statement")
        super(SparkSubmitOperator, self).execute(context)

        logging.info("Finished executing SparkSubmitOperator.execute(context)")

    @staticmethod
    def is_not_null_and_is_not_empty_str(value):
        return value is not None and value != ""


class LivySparkOperator(BaseOperator):
    """
   Operator to facilitate interacting with the Livy Server which executes Apache Spark code via a REST API.

   :param spark_script: Scala, Python or R code to submit to the Livy Server (templated)
   :type spark_script: string
   :param session_kind: Type of session to setup with Livy. This will determine which type of code will be accepted. Possible values include "spark" (executes Scala code), "pyspark" (executes Python code) or "sparkr" (executes R code).
   :type session_kind: string
   :param http_conn_id: The http connection to run the operator against
   :type http_conn_id: string
   :param poll_interval: The polling interval to use when checking if the code in spark_script has finished executing. In seconds. (default: 30 seconds)
   :type poll_interval: integer
   """

    template_fields = ['spark_script']  # todo : make sure this works
    template_ext = ['.py', '.R', '.r']
    ui_color = '#34a8dd'  # Clouderas Main Color: Blue

    acceptable_response_codes = [200, 201]

    @apply_defaults
    def __init__(
            self,
            spark_script,
            session_kind="spark",  # spark, pyspark, or sparkr
            http_conn_id='http_default',
            poll_interval=30,
            *args, **kwargs):
        super(LivySparkOperator, self).__init__(*args, **kwargs)

        self.spark_script = spark_script
        self.session_kind = session_kind
        self.http_conn_id = http_conn_id
        self.poll_interval = poll_interval

        self.http = HttpHook("GET", http_conn_id=self.http_conn_id)

    def execute(self, context):
        logging.info("Executing LivySparkOperator.execute(context)")

        logging.info("Validating arguments...")
        self._validate_arguments()
        logging.info("Finished validating arguments")

        logging.info("Creating a Livy Session...")
        session_id = self._create_session()
        logging.info("Finished creating a Livy Session. (session_id: " + str(session_id) + ")")

        logging.info("Submitting spark script...")
        statement_id, overall_statements_state = self._submit_spark_script(session_id=session_id)
        logging.info("Finished submitting spark script. (statement_id: " + str(statement_id) + ", overall_statements_state: " + str(overall_statements_state) + ")")

        poll_for_completion = (overall_statements_state == "running")

        if poll_for_completion:
            logging.info("Spark job did not complete immediately. Starting to Poll for completion...")

        while overall_statements_state == "running":  # todo: test execution_timeout
            logging.info("Sleeping for " + str(self.poll_interval) + " seconds...")
            time.sleep(self.poll_interval)
            logging.info("Finished sleeping. Checking if Spark job has completed...")
            statements = self._get_session_statements(session_id=session_id)

            is_all_complete = True
            for statement in statements:
                if statement["state"] == "running":
                    is_all_complete = False

            if is_all_complete:
                overall_statements_state = "available"

            logging.info("Finished checking if Spark job has completed. (overall_statements_state: " + str(overall_statements_state) + ")")

        if poll_for_completion:
            logging.info("Finished Polling for completion.")

        logging.info("Session Logs:\n" + str(self._get_session_logs(session_id=session_id)))

        for statement in self._get_session_statements(session_id):
            logging.info("Statement '" + str(statement["id"]) + "' Output:\n" + str(statement["output"]))

        logging.info("Closing session...")
        response = self._close_session(session_id=session_id)
        logging.info("Finished closing session. (response: " + str(response) + ")")

        logging.info("Finished executing LivySparkOperator.execute(context)")

    def _validate_arguments(self):
        if self.session_kind is None or self.session_kind == "":
            raise Exception(
                "session_kind argument is invalid. It is empty or None. (value: '" + str(self.session_kind) + "')")
        elif self.session_kind not in ["spark", "pyspark", "sparkr"]:
            raise Exception(
                "session_kind argument is invalid. It should be set to 'spark', 'pyspark', or 'sparkr'. (value: '" + str(
                    self.session_kind) + "')")

    def _get_sessions(self):
        method = "GET"
        endpoint = "sessions"
        response = self._http_rest_call(method=method, endpoint=endpoint)

        if response.status_code in self.acceptable_response_codes:
            return response.json()["sessions"]
        else:
            raise Exception("Call to get sessions didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _get_session(self, session_id):
        sessions = self._get_sessions()
        for session in sessions:
            if session["id"] == session_id:
                return session

    def _get_session_logs(self, session_id):
        method = "GET"
        endpoint = "sessions/" + str(session_id) + "/log"
        response = self._http_rest_call(method=method, endpoint=endpoint)
        return response.json()

    def _create_session(self):
        method = "POST"
        endpoint = "sessions"

        data = {
            "kind": self.session_kind
        }

        response = self._http_rest_call(method=method, endpoint=endpoint, data=data)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            session_id = response_json["id"]
            session_state = response_json["state"]

            if session_state == "starting":
                logging.info("Session is starting. Polling to see if it is ready...")

            session_state_polling_interval = 10
            while session_state == "starting":
                logging.info("Sleeping for " + str(session_state_polling_interval) + " seconds")
                time.sleep(session_state_polling_interval)
                session_state_check_response = self._get_session(session_id=session_id)
                session_state = session_state_check_response["state"]
                logging.info("Got latest session state as '" + session_state + "'")

            return session_id
        else:
            raise Exception("Call to create a new session didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _submit_spark_script(self, session_id):
        method = "POST"
        endpoint = "sessions/" + str(session_id) + "/statements"

        logging.info("Executing Spark Script: \n" + str(self.spark_script))

        data = {
            'code': textwrap.dedent(self.spark_script)
        }

        response = self._http_rest_call(method=method, endpoint=endpoint, data=data)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            return response_json["id"], response_json["state"]
        else:
            raise Exception("Call to create a new statement didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _get_session_statements(self, session_id):
        method = "GET"
        endpoint = "sessions/" + str(session_id) + "/statements"
        response = self._http_rest_call(method=method, endpoint=endpoint)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            statements = response_json["statements"]
            return statements
        else:
            raise Exception("Call to get the session statement response didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _close_session(self, session_id):
        method = "DELETE"
        endpoint = "sessions/" + str(session_id)
        return self._http_rest_call(method=method, endpoint=endpoint)

    def _http_rest_call(self, method, endpoint, data=None, headers=None, extra_options=None):
        if not extra_options:
            extra_options = {}
        logging.debug("Performing HTTP REST call... (method: " + str(method) + ", endpoint: " + str(endpoint) + ", data: " + str(data) + ", headers: " + str(headers) + ")")
        self.http.method = method
        response = self.http.run(endpoint, json.dumps(data), headers, extra_options=extra_options)

        logging.debug("status_code: " + str(response.status_code))
        logging.debug("response_as_json: " + str(response.json()))

        return response


# Defining the plugin class
class SparkOperatorPlugin(AirflowPlugin):
    name = "spark_operator_plugin"
    operators = [SparkSubmitOperator, LivySparkOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
