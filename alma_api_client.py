import requests
from time import sleep


class AlmaAPIClient:
    def __init__(self, api_key: str) -> None:
        self.API_KEY = api_key
        self.BASE_URL = "https://api-na.hosted.exlibrisgroup.com"

    def _get_headers(self, format: str = "json") -> dict:
        return {
            "Authorization": f"apikey {self.API_KEY}",
            "Accept": f"application/{format}",
            "Content-Type": f"application/{format}",
        }

    def _get_api_data(self, response: requests.Response, format: str = "json") -> dict:
        """Return dictionary with response content and selected response headers.

        If format is not json, the (presumably) XML content is in api_data["content"],
        as a byte array.
        """
        try:
            if format == "json":
                api_data: dict = response.json()
            else:
                api_data = {"content": response.content}
        except requests.exceptions.JSONDecodeError:
            # Some responses return nothing, which can't be decoded...
            api_data = {}
        # Add a few response elements caller can use
        api_data["api_response"] = {
            "headers": response.headers,
            "status_code": response.status_code,
            "request_url": response.url,
        }
        return api_data

    def _call_get_api(
        self, api: str, parameters: dict = None, format: str = "json"
    ) -> dict:
        if parameters is None:
            parameters = {}
        get_url = self.BASE_URL + api
        headers = self._get_headers(format)
        response = requests.get(get_url, headers=headers, params=parameters)
        api_data: dict = self._get_api_data(response, format)
        return api_data

    def _call_post_api(
        self, api: str, data: dict, parameters: dict = None, format: str = "json"
    ) -> dict:
        if parameters is None:
            parameters = {}
        post_url = self.BASE_URL + api
        headers = self._get_headers(format)
        # TODO: Non-JSON POST?
        response = requests.post(
            post_url, headers=headers, json=data, params=parameters
        )
        api_data: dict = self._get_api_data(response, format)
        return api_data

    def _call_put_api(
        self, api: str, data: str, parameters: dict = None, format: str = "json"
    ) -> dict:
        if parameters is None:
            parameters = {}
        headers = self._get_headers(format)
        put_url = self.BASE_URL + api
        # Handle both XML (required by update_bib) and default JSON
        if format == "xml":
            response = requests.put(
                put_url, headers=headers, data=data, params=parameters
            )
        else:
            # json default
            response = requests.put(
                put_url, headers=headers, json=data, params=parameters
            )
        api_data: dict = self._get_api_data(response, format)
        return api_data

    def _call_delete_api(
        self, api: str, parameters: dict = None, format: str = "json"
    ) -> dict:
        if parameters is None:
            parameters = {}
        delete_url = self.BASE_URL + api
        headers = self._get_headers(format)
        response = requests.delete(delete_url, headers=headers, params=parameters)
        # Success is HTTP 204, "No Content"
        if response.status_code != 204:
            # TODO: Real error handling
            print(delete_url)
            print(response.status_code)
            print(response.headers)
            print(response.text)
            # exit(1)
        api_data: dict = self._get_api_data(response, format)
        return api_data

    def create_item(
        self, bib_id: str, holding_id: str, data: dict, parameters: dict = None
    ) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/bibs/{bib_id}/holdings/{holding_id}/items"
        return self._call_post_api(api, data, parameters)

    def get_items(self, bib_id: str, holding_id: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/bibs/{bib_id}/holdings/{holding_id}/items"
        return self._call_get_api(api, parameters)

    def get_integration_profiles(self, parameters: dict = None) -> dict:
        # Caller can pass search parameters, but must deal with possible
        # multiple matches.
        if parameters is None:
            parameters = {}
        api = "/almaws/v1/conf/integration-profiles"
        return self._call_get_api(api, parameters)

    def get_jobs(self, parameters: dict = None) -> dict:
        # Caller normally will pass parameters, but they're not required.
        # Caller must deal with possible multiple matches.
        if parameters is None:
            parameters = {}
        api = "/almaws/v1/conf/jobs"
        return self._call_get_api(api, parameters)

    def run_job(self, job_id, data: dict = None, parameters: dict = None) -> dict:
        # Tells Alma to queue / run a job; does *not* wait for completion.
        # Caller must provide job_id outside of parameters.
        # Running a scheduled job requires empty data {}; not sure about other jobs
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/conf/jobs/{job_id}"
        return self._call_post_api(api, data, parameters)

    def wait_for_completion(
        self, job_id: str, instance_id: str, seconds_to_poll: int = 15
    ) -> dict:
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

    def get_fees(self, user_id: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/users/{user_id}/fees"
        return self._call_get_api(api, parameters)

    def get_analytics_report(self, parameters: dict = None) -> dict:
        # Docs say to URL-encode report name (path);
        # request lib is doing it automatically.
        if parameters is None:
            parameters = {}
        api = "/almaws/v1/analytics/reports"
        return self._call_get_api(api, parameters)

    def get_analytics_path(self, path: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/analytics/paths/{path}"
        return self._call_get_api(api, parameters)

    def get_vendors(self, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = "/almaws/v1/acq/vendors"
        return self._call_get_api(api, parameters)

    def get_vendor(self, vendor_code: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/acq/vendors/{vendor_code}"
        return self._call_get_api(api, parameters)

    def get_bib(self, mms_id: str, parameters: dict = None) -> dict:
        """Return dictionary response, with Alma bib record (in Alma XML format),
        in "content" element.
        """
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/bibs/{mms_id}"
        return self._call_get_api(api, parameters, format="xml")

    def update_bib(self, mms_id: str, data: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/bibs/{mms_id}"
        return self._call_put_api(api, data, parameters, format="xml")

    def get_holding(
        self, mms_id: str, holding_id: str, parameters: dict = None
    ) -> dict:
        """Return dictionary response, with Alma holding record (in Alma XML format),
        in "content" element.
        """
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/bibs/{mms_id}/holdings/{holding_id}"
        return self._call_get_api(api, parameters, format="xml")

    def update_holding(
        self, mms_id: str, holding_id: str, data: str, parameters: dict = None
    ) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/bibs/{mms_id}/holdings/{holding_id}"
        return self._call_put_api(api, data, format="xml")

    def get_set_members(self, set_id: str, parameters: dict = None) -> None:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/conf/sets/{set_id}/members"
        return self._call_get_api(api, parameters)

    def create_user(self, user: dict, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = "/almaws/v1/users"
        return self._call_post_api(api, user, parameters)

    def delete_user(self, user_id: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/users/{user_id}"
        return self._call_delete_api(api, parameters)

    def get_user(self, user_id: str, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/users/{user_id}"
        return self._call_get_api(api, parameters)

    def update_user(self, user_id: str, user: dict, parameters: dict = None) -> dict:
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/users/{user_id}"
        return self._call_put_api(api, user, parameters)

    def get_general_configuration(self) -> dict:
        """Return general configuration info.
        Useful for checking production / sandbox via environment_type.
        """
        api = "/almaws/v1/conf/general"
        return self._call_get_api(api)

    def get_code_tables(self) -> dict:
        """Return list of code tables.  This specific API is undocumented."""
        api = "/almaws/v1/conf/code-tables"
        return self._call_get_api(api)

    def get_code_table(self, code_table: str, parameters: dict = None) -> dict:
        """Return specific code table, via name from get_code_tables()."""
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/conf/code-tables/{code_table}"
        return self._call_get_api(api, parameters)

    def get_mapping_tables(self) -> dict:
        """Return list of mapping tables.  This specific API is undocumented."""
        api = "/almaws/v1/conf/mapping-tables"
        return self._call_get_api(api)

    def get_mapping_table(self, mapping_table: str, parameters: dict = None) -> dict:
        """Return specific mapping table, via name from get_mapping_tables()."""
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/conf/code-tables/{mapping_table}"
        return self._call_get_api(api, parameters)

    def get_libraries(self) -> dict:
        """Return all libraries."""
        api = "/almaws/v1/conf/libraries"
        return self._call_get_api(api)

    def get_library(self, library_code: str) -> dict:
        """Return data for a single library, via code.
        Doesn't provide more details than each entry in get_libaries().
        """
        api = f"/almaws/v1/conf/libraries/{library_code}"
        return self._call_get_api(api)

    def get_circulation_desks(self, library_code: str, parameters: dict = None) -> dict:
        """Return data about circ desks in a single library, via code."""
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/conf/libraries/{library_code}/circ-desks/"
        return self._call_get_api(api, parameters)

    def get_funds(self, parameters: dict = None) -> dict:
        """Return data about all funds matching search in parameters."""
        if parameters is None:
            parameters = {}
        api = "/almaws/v1/acq/funds"
        return self._call_get_api(api, parameters)

    def get_fund(self, fund_id: str, parameters: dict = None) -> dict:
        """Return data about a specific fund."""
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/acq/funds/{fund_id}"
        return self._call_get_api(api, parameters)

    def update_fund(self, fund_id: str, fund: dict, parameters: dict = None) -> dict:
        """Update a specific fund."""
        if parameters is None:
            parameters = {}
        api = f"/almaws/v1/acq/funds/{fund_id}"
        return self._call_put_api(api, fund, parameters)
