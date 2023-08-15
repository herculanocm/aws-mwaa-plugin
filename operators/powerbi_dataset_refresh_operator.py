import json
import urllib3
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class PowerBIDatasetRefreshOperator(BaseOperator):
    """
    Airflow operator to refresh a Power BI dataset using the Power BI REST API.
    """
    #template_fields = ('dataset_id',)
    
    @apply_defaults
    def __init__(
        self,
        dataset_id,
        workspace_id,
        refresh_endpoint='refreshes',
        api_version='v1.0',
        token=None,
        polling_interval=30,  # Time in seconds to wait before polling for refresh status
        max_polling_attempts=30,  # Maximum number of attempts to poll for refresh status
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
        self.polling_interval = polling_interval
        self.max_polling_attempts = max_polling_attempts
        self.poll_interval = 5

    def _get_refresh_status(self):
        
        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        
        # Construct the API endpoint
        base_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{self.workspace_id}/'
        endpoint_url = f'{base_url}datasets/{self.dataset_id}/{self.refresh_endpoint}'
        print(f"Refresh dataflow url: {endpoint_url} - headers: {headers}")

        # Send the GET request to fetch the refresh status
        http = urllib3.PoolManager()
        self.log.info(f"Refresh dataflow url: {endpoint_url}")


        response = http.request(
            'POST', 
            endpoint_url, 
            headers=headers
            )
        self.log.info(f"status: {response.status}, data: {response.data}")
        return response.status

    def execute(self, context):
        self.log.info(f"Calling API Dataflow ID: {self.dataset_id}")
        status_code = self._get_refresh_status()

        if status_code >= 200 and status_code <= 299:
            self.log.info(f"Power BI dataset refresh completed for dataset ID: {self.dataset_id}")
        else:
            raise Exception(f"Power BI dataset refresh failed for dataset ID: {self.dataset_id}")