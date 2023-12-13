import json
import urllib3
import time
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Airflow operator to refresh a Power BI dataset using the Power BI REST API.
    """
    template_fields = ('dataset_id','workspace_id', 'token')
    
    @apply_defaults
    def __init__(
        self,
        dataset_id,
        workspace_id,
        refresh_endpoint='refreshes',
        api_version='v1.0',
        token=None,
        *args, **kwargs
    ):
        """
        Initialize the operator.

        :param dataset_id: The ID of the Power BI dataset to be refreshed.
        :type dataset_id: str
        :param workspace_id: The ID of the Power BI workspace where the dataset belongs.
        :type workspace_id: str
        :param refresh_endpoint: The refresh endpoint for Power BI datasets.
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
        super(PowerBIDatasetRefreshOperator, self).__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.workspace_id = workspace_id
        self.refresh_endpoint = refresh_endpoint
        self.api_version = api_version
        self.token = token

    def execute(self, context):
        max_polling_attempts = 10
        poll_interval = 5
        # format_string = '%Y-%m-%dT%H:%M:%S.%fZ'
        format_string = '%Y-%m-%dT%H:%M:%SZ'
        default_ts = '1999-12-01T02:54:54.86Z'

        self.log.info(f"Calling API Dataflow ID: {self.dataset_id}")
    
        headers = {
            'Authorization': f'Bearer {self.token}'
        }

        base_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{self.workspace_id}/'
        endpoint_url = f'{base_url}datasets/{self.dataset_id}/{self.refresh_endpoint}'

        http = urllib3.PoolManager()
        self.log.info(f"Refresh dataflow url: {endpoint_url}")

        try:
            response = http.request("POST", endpoint_url, headers=headers)
            if response.status < 200 or response.status > 299:
                self.log.error(f"Error triggering Power BI dataset refresh: {response.status}")
                raise Exception(f"Error triggering Power BI dataset refresh: {response.status}")
        except Exception as e:
            self.log.error(f"Error triggering Power BI dataset refresh: {e}")
            raise
        
        for attempt in range(1, max_polling_attempts + 1):
            time.sleep(poll_interval)

            try:
                response = http.request("GET", endpoint_url, headers=headers)
                if response.status < 200 or response.status > 299:
                    self.log.warning(f"Error checking Power BI refresh status: {response.status}")
                else:
                    arr = json.loads(response.data.decode("utf-8")).get("value")
                    max_value = datetime.strptime(default_ts[0:18], format_string)
                    max_index = -1

                    for index, item in enumerate(arr):
                        _dd = item["startTime"]
                        _date = datetime.strptime(_dd[0:18], format_string)
                        # _date = datetime.strptime(_dd, '%Y-%m-%dT%H:%M:%S.%fZ')
                        if _date > max_value:
                            max_value = _date
                            max_index = index
                    ultima_data = arr[max_index]
                    refresh_status = ultima_data.get("status")
                    self.log.info(f"Refresh attempt {attempt}/{max_polling_attempts} - Status: {refresh_status}")
                    if refresh_status == "Completed":
                        self.log.info("Power BI dataset refresh completed.")
                        return
            except Exception as e:
                self.log.warning(f"Error checking Power BI refresh status: {e}")
                
        raise ValueError("Power BI dataset refresh did not complete within the specified attempts.")
        
