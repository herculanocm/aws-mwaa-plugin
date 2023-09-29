import json
import urllib3
import time
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class PowerBIDataflowRefreshOperator(BaseOperator):
    """
    Airflow operator to refresh a Power BI dataflow using the Power BI REST API.
    """
    template_fields = ('dataflow_id','workspace_id', 'token')
    
    @apply_defaults
    def __init__(
        self,
        dataflow_id,
        workspace_id,
        refresh_endpoint='refreshes',
        transactions_endpoint='transactions',
        api_version='v1.0',
        token=None,
        polling_interval=10,  # Time in seconds to wait before polling for refresh status
        max_polling_attempts=120,  # Maximum number of attempts to poll for refresh status
        # format_string = '%Y-%m-%dT%H:%M:%S.%fZ',
        format_string = '%Y-%m-%dT%H:%M:%S',
        *args, **kwargs
    ):
        """
        Initialize the operator.

        :param dataflow_id: The ID of the Power BI dataflow to be refreshed.
        :type dataflow_id: str
        :param workspace_id: The ID of the Power BI workspace where the dataflow belongs.
        :type workspace_id: str
        :param refresh_endpoint: The refresh endpoint for Power BI dataflows.
        :type refresh_endpoint: str
        :param api_version: The Power BI REST API version to use (default: 'v1.0').
        :type api_version: str
        :param token: The Power BI access token. If not provided, it will be fetched from Airflow connection.
        :type token: str
        :param polling_interval: Time in seconds to wait before polling for refresh status.
        :type polling_interval: int
        :param max_polling_attempts: Maximum number of attempts to poll for refresh status.
        :type max_polling_attempts: int
        """
        super(PowerBIDataflowRefreshOperator, self).__init__(*args, **kwargs)
        self.dataflow_id = dataflow_id
        self.workspace_id = workspace_id
        self.refresh_endpoint = refresh_endpoint
        self.api_version = api_version
        self.token = token
        self.polling_interval = polling_interval
        self.max_polling_attempts = max_polling_attempts
        self.transactions_endpoint = transactions_endpoint
        self.format_string = format_string

    def _get_refresh_status(self):
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        
        # Construct the API endpoint
        base_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{self.workspace_id}/'
        refresh_endpoint_url = f'{base_url}dataflows/{self.dataflow_id}/{self.refresh_endpoint}'
        print(f"Refresh dataflow url: {refresh_endpoint_url} - headers: {headers}")

        # Send the GET request to fetch the refresh status
        http = urllib3.PoolManager()
        self.log.info(f"Refresh dataflow url: {refresh_endpoint_url}")

        body_data = {
            "notifyOption": "MailOnFailure"
        }

        json_data = json.dumps(body_data)
        response = http.request(
            'POST', 
            refresh_endpoint_url, 
            body=json_data,
            headers=headers
            )
        self.log.info(f"status: {response.status}, data: {response.data}")
        return response.status

    def _get_transaction_list(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        body_data = {
            "notifyOption": "MailOnFailure"
        }
        json_data = json.dumps(body_data)

        base_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{self.workspace_id}/'
        transactions_endpoint_url = f'{base_url}dataflows/{self.dataflow_id}/{self.transactions_endpoint}'
        self.log.info(f"Refresh dataflow url: {transactions_endpoint_url} - headers: {headers}")
        
        http = urllib3.PoolManager()

        
        response = http.request(
            'GET', 
            transactions_endpoint_url, 
            body=json_data,
            headers=headers
            )
        self.log.info(f"status: {response.status}, data: {response.data}")
        return response


    def execute(self, context):
        self.log.info(f"Calling API Dataflow ID: {self.dataflow_id}")
        status_code = self._get_refresh_status()

        if status_code == 200:
            self.log.info(f"Power BI dataflow refresh completed for Dataflow ID: {self.dataflow_id}")
        else:
            raise Exception(f"Power BI dataflow refresh failed for Dataflow ID: {self.dataflow_id}")

        for attempt in range(1, self.max_polling_attempts + 1):
            time.sleep(self.polling_interval)


            try:
                response = self._get_transaction_list()
                if response.status < 200 or response.status > 299:
                    self.log.warning(f"Error checking Power BI refresh status: {response.status}")
                else:               
                    arr = json.loads(response.data.decode("utf-8")).get("value")
                    # max_value = datetime.strptime('1999-12-01T02:54:54.86Z', self.format_string)
                    max_value = datetime.strptime('2023-09-07T21:29:32', self.format_string)
                    max_index = -1

                    for index, item in enumerate(arr):
                        _date = datetime.strptime(item["startTime"][0:19], self.format_string)
                        if _date > max_value:
                            max_value = _date
                            max_index = index

                    ultima_data = arr[max_index]
                    refresh_status = ultima_data.get("status")
                    
                    self.log.info(f"Refresh attempt {attempt}/{self.max_polling_attempts} - Status: {refresh_status}")
                    if refresh_status == "Success":
                        self.log.info("Power BI dataset refresh completed.")
                        return

            except Exception as e:
                self.log.warning(f"Error checking Power BI refresh status: {e}")

        raise ValueError("Power BI dataset refresh did not complete within the specified attempts.")
    