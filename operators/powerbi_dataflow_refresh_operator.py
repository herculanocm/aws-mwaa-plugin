import json
import urllib3
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class PowerBIDataflowRefreshOperator(BaseOperator):
    """
    Airflow operator to refresh a Power BI dataflow using the Power BI REST API.
    """
    template_fields = ('dataflow_id',)
    
    @apply_defaults
    def __init__(
        self,
        dataflow_id,
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

    def _get_refresh_status(self):
        # Fetch token from connection if not provided
        if not self.token:
            conn = self.get_hook('power_bi_conn_id')
            self.token = conn.access_token

        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        
        # Construct the API endpoint
        base_url = f'https://api.powerbi.com/{self.api_version}/myorg/groups/{self.workspace_id}/'
        endpoint_url = f'{base_url}dataflows/{self.dataflow_id}/{self.refresh_endpoint}'

        # Send the GET request to fetch the refresh status
        http = urllib3.PoolManager()
        response = http.request('GET', endpoint_url, headers=headers)
        return json.loads(response.data)

    def execute(self, context):
        # Initiate the dataflow refresh
        super(PowerBIDataflowRefreshOperator, self).execute(context)

        # Wait for the refresh to complete
        attempts = 0
        while attempts < self.max_polling_attempts:
            refresh_status = self._get_refresh_status()
            if refresh_status.get('status', {}).get('value') == 'Succeeded':
                self.log.info(f"Power BI dataflow refresh completed for Dataflow ID: {self.dataflow_id}")
                break
            elif refresh_status.get('status', {}).get('value') == 'Failed':
                self.log.error(f"Power BI dataflow refresh failed for Dataflow ID: {self.dataflow_id}")
                raise Exception(f"Power BI dataflow refresh failed for Dataflow ID: {self.dataflow_id}")
            else:
                self.log.info(f"Waiting for Power BI dataflow refresh to complete. Attempt {attempts+1}/{self.max_polling_attempts}")
                attempts += 1
                time.sleep(self.polling_interval)