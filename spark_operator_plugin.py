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
from airflow.operators import BashOperator
from airflow.utils.decorators import apply_defaults
import logging


class SparkSubmitOperator(BashOperator):
    """
   Execute a spark-submit command.
   :param bash_command: The command, set of commands or reference to a
       bash script (must be '.sh') to be executed.
   :type bash_command: string
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

    template_fields = ('conf', 'other_spark_options', 'application_args', 'env')  # todo: review exactly what this is and how it can be used
    template_ext = []
    ui_color = '#e47128'

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
        super(SparkSubmitOperator, self).__init__(bash_command=None, *args, **kwargs)
        self.application_file = application_file
        self.main_class = main_class
        self.master = master
        self.conf = conf
        self.deploy_mode = deploy_mode
        self.other_spark_options = other_spark_options
        self.application_args = application_args
        self.env = env
        self.output_encoding = output_encoding
        self.xcom_push_flag = xcom_push

        self.bash_command = ""

    def execute(self, context):
        logging.info("Executing SparkSubmitOperator")
        self.bash_command = "spark-submit "
        if self.is_not_null_and_is_not_empty(self.main_class):
            self.bash_command += "--class " + self.main_class + " "
        if self.is_not_null_and_is_not_empty(self.master):
            self.bash_command += "--master " + self.master + " "
        if self.is_not_null_and_is_not_empty(self.deploy_mode):
            self.bash_command += "--deploy-mode " + self.deploy_mode + " "
        if self.is_not_null_and_is_not_empty(self.conf):
            self.bash_command += "--conf " + self.conf + " "
        if self.is_not_null_and_is_not_empty(self.other_spark_options):
            self.bash_command += self.other_spark_options + " "

        self.bash_command += self.application_file + " "

        if self.is_not_null_and_is_not_empty(self.application_args):
            self.bash_command += self.application_args + " "

        logging.info("Finished assembling bash_command in SparkSubmitOperator: " + str(self.bash_command))

        logging.info("Executing bash execute statement")
        super(SparkSubmitOperator, self).execute(context)

    @staticmethod
    def is_not_null_and_is_not_empty(value):
        return value is not None and value != ""


# Defining the plugin class
class SparkOperatorPlugin(AirflowPlugin):
    name = "spark_operator_plugin"
    operators = [SparkSubmitOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []