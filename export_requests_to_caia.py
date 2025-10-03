import argparse
import tomllib
from datetime import datetime
from alma_api_client import APIError, APIResponse, AlmaAPIClient


def _get_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    :return: Parsed arguments for program as a Namespace object.
    """
    parser = argparse.ArgumentParser(description="Update bookplates in Alma.")
    parser.add_argument(
        "--production",
        action="store_true",
        help="Use production Alma API key. Default is sandbox.",
    )
    parser.add_argument(
        "--config_file",
        type=str,
        default="secret_config.toml",
        help="Path to config file with API keys",
    )
    parser.add_argument(
        "--log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    return parser.parse_args()


def _get_config(config_file_name: str) -> dict:
    """Returns configuration for this program, loaded from TOML file.

    :param config_file_name: Path to the configuration file.
    :return: Configuration dictionary.
    """
    with open(config_file_name, "rb") as f:
        config = tomllib.load(f)
    return config


def _get_profile_id(api_response: APIResponse) -> str:
    """Return the Alma profile id from an Alma API response.

    :param api_response: An Alma `APIResponse` containing integration profile data.
    :raises: `ValueError`, if the response has other than one single profile
    or no id is found in the profile data.
    :return profile_id: The matching integration profile id.
    """
    api_data = api_response.api_data
    total_record_count = api_data.get("total_record_count", 0)
    if total_record_count == 1:
        profiles = api_data.get("integration_profile", [])
        # 1 profile (trust the count), get its id.
        profile_id = profiles[0].get("id", "")
        if profile_id:
            return profile_id
        else:
            raise ValueError("Profile id missing from profile")
    else:
        raise ValueError(f"Expected 1 profile, found: {total_record_count}")


def _get_job_id(api_response: APIResponse, job_name: str) -> str:
    """Return the Alma job id from an Alma API response, matching the expected job name
    in case multiple jobs were found.

    :param api_response: An Alma `APIResponse` containing job data.
    :raises: `ValueError`, if no id is found in the job data.
    :return job_id: The matching job id.
    """
    # Initialize, in case there are no jobs in the API data at all.
    job_id = ""
    # Profiles can have many jobs associated with them.
    # Find first job which matches the expected job name.
    # This is fragile since the job name could be changed in Alma, but the best option available.
    for job in api_response.api_data.get("job", []):
        if job.get("name", "") == job_name:
            job_id = job.get("id", "")
            break
    # Found one, return it.
    if job_id:
        return job_id
    else:
        raise ValueError(f"No job found in API data for {job_name}")


def main():
    """Entry point for the script.

    This script runs an Alma job associated with the Caiasoft remote storage integration
    profile, "Send data from Alma to Caiasoft via SFTP".  When there are appropriate requests
    in Alma, this job causes data for them to be exported and uploaded to Caia's SFTP server
    (connection handled within Alma, not by this script).

    The script does not wait for the job to complete.  Experiments show that checking the job
    info after completion, like /almaws/v1/conf/jobs/S6378570160006533/instances/25701065310006533,
    does not contain useful data, though there may be events associated with the job we could
    check for.
    """
    args = _get_arguments()
    config = _get_config(args.config_file)

    if args.production:
        alma_api_key = config["alma_api_keys"]["DIIT_SCRIPTS"]
    else:  # default to sandbox
        print("This job should not be run in the SANDBOX environment;")
        print("behavior is unknown, but could upload duplicate / wrong")
        print("requests to Caia. Exiting now.")
        exit()

    client = AlmaAPIClient(alma_api_key)

    # Initialize variables to make type-checker happy.
    profile_id = ""
    job_id = ""

    # Hard-coded integration profile code, should be unique;
    # not using hard-coded profile id, in case that's subject to change.
    profile_code = "Caiasoft"
    profile_parameters = {"q": f"code~{profile_code}"}
    # If one single profile is not found, no point in continuing. Catch exceptions
    # to print error messages, then exit.
    try:
        # No APIError raised if nothing found (no 400 or 404), just a response with
        # {'total_record_count': 0}.
        api_response = client.get_integration_profiles(parameters=profile_parameters)
        profile_id = _get_profile_id(api_response)
    except APIError as e:
        # Print detailed error messages from API response.
        print(e.error_messages)
        exit()
    except ValueError as e:
        print(e)
        exit()

    # An empty profile_id, if the profile wasn't found and somehow not detected already,
    # can retrieve multiple irrelevant jobs, leading to problems.
    assert profile_id != ""

    # We've got one real profile id, so try to find the job we need.
    job_parameters = {
        "category": "FULFILLMENT",
        "profile_id": profile_id,
        "type": "SCHEDULED",
    }
    try:
        api_response = client.get_jobs(parameters=job_parameters)
        # Jobs cannot be filtered on name via API, so use
        # job name as secondary check.
        job_name = "Send Requests to Remote Storage"
        job_id = _get_job_id(api_response, job_name)
    except APIError as e:
        # Print detailed error messages from API response.
        print(e.error_messages)
        exit()
    except ValueError as e:
        print(e)
        exit()

    # Still paranoid.
    assert job_id != ""

    # We've got a real job id, so try to run it.
    now = datetime.now().strftime("%c")
    print(f"Running job {job_id} at {now}")
    # Run the job, but no need to wait for completion.
    # TODO: Consider implementing API call for "Retrieve Job Instance Event Details",
    # which can provide useful info when the job does complete.
    try:
        run_parameters = {"op": "run"}
        client.run_job(job_id, parameters=run_parameters)
    except APIError as e:
        # Print detailed error messages from API response.
        print(e.error_messages)
        exit()


if __name__ == "__main__":
    main()
