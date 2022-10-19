import requests
from time import sleep


class Alma_Api_Client:
    def __init__(self, api_key):
        self.API_KEY = api_key
        self.BASE_URL = "https://api-na.hosted.exlibrisgroup.com"
        self.HEADERS = {
            "Authorization": f"apikey {self.API_KEY}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _call_get_api(self, api, parameters={}):
        # parameters: optional dictionary
        get_url = self.BASE_URL + api
        response = requests.get(get_url, headers=self.HEADERS, params=parameters)
        # Actually, a Python dictionary...
        api_data = response.json()
        # Add a few response elements caller can use
        api_data["api_response"] = {
            "headers": response.headers,
            "status_code": response.status_code,
            "request_url": response.url,
        }
        return api_data

    def _call_post_api(self, api, data, parameters={}):
        post_url = self.BASE_URL + api
        response = requests.post(
            post_url, headers=self.HEADERS, json=data, params=parameters
        )
        # Actually, a Python dictionary...
        api_data = response.json()
        # Add a few response elements caller can use
        api_data["api_response"] = {
            "headers": response.headers,
            "status_code": response.status_code,
            "request_url": response.url,
        }
        return api_data

    def _call_delete_api(self, api, parameters, data):
        delete_url = self.BASE_URL + api + parameters
        response = requests.delete(delete_url, headers=self.HEADERS, data=data)
        # Success is HTTP 204, "No Content"
        if response.status_code != 204:
            # TODO: Real error handling
            print(delete_url)
            print(response.status_code)
            print(response.headers)
            print(response.text)
            # exit(1)
        return response

    def create_item(self, bib_id, holding_id, data, parameters={}):
        api = f"/almaws/v1/bibs/{bib_id}/holdings/{holding_id}/items"
        return self._call_post_api(api, data, parameters)

    def get_items(self, bib_id, holding_id, parameters={}):
        api = f"/almaws/v1/bibs/{bib_id}/holdings/{holding_id}/items"
        return self._call_get_api(api, parameters)

    def get_integration_profiles(self, parameters={}):
        # Caller can pass search parameters, but must deal with possible
        # multiple matches.
        api = "/almaws/v1/conf/integration-profiles"
        return self._call_get_api(api, parameters)

    def get_jobs(self, parameters={}):
        # Caller normally will pass parameters, but they're not required.
        # Caller must deal with possible multiple matches.
        api = "/almaws/v1/conf/jobs"
        return self._call_get_api(api, parameters)

    def run_job(self, job_id, data={}, parameters={}):
        # Tells Alma to queue / run a job; does *not* wait for completion.
        # Caller must provide job_id outside of parameters.
        # Running a scheduled job requires empty data {}; not sure about other jobs
        api = f"/almaws/v1/conf/jobs/{job_id}"
        return self._call_post_api(api, data, parameters)

    def wait_for_completion(self, job_id, instance_id, seconds_to_poll=15):
        # Running a job just queues it to run; Alma assigns an instance id.
        # This method allows the caller to wait until the given instance of
        # the job has completed.
        api = f"/almaws/v1/conf/jobs/{job_id}/instances/{instance_id}"
        # progress value (0-100) can't be used as it remains 0 if FAILED.
        # Use status instead; values from
        # https://developers.exlibrisgroup.com/alma/apis/docs/xsd/rest_job_instance.xsd/
        status = "NONE"  # Fake value until API is called.
        while status in [
            "NONE",
            "QUEUED",
            "PENDING",
            "INITIALIZING",
            "RUNNING",
            "FINALIZING",
        ]:
            instance = self._call_get_api(api)
            status = instance["status"]["value"]
            print(status)
            sleep(seconds_to_poll)
        return instance

    def get_fees(self, user_id, parameters={}):
        api = f"/almaws/v1/users/{user_id}/fees"
        return self._call_get_api(api, parameters)

    def get_analytics_report(self, parameters={}):
        # Docs say to URL-encode report name (path);
        # they lie, or request lib is doing it automatically?
        # path: /shared/University of California Los Angeles (UCLA) 01UCS_LAL/Reports/RAMS/Cataloging Statistics
        api = "/almaws/v1/analytics/reports"
        return self._call_get_api(api, parameters)

    def get_analytics_path(self, path, parameters={}):
        api = f"/almaws/v1/analytics/paths/{path}"
        return self._call_get_api(api, parameters)

    def get_vendor(self, parameters={}):
        api = "/almaws/v1/acq/vendors"
        return self._call_get_api(api, parameters)
