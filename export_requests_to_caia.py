#!/usr/bin/env python3
"""
Finds and runs an Alma job to export remote storage requests.
See LICENSE.txt in this repository.
"""
from datetime import datetime
from alma_api_client import AlmaAPIClient
from alma_api_keys import API_KEYS


def get_profile_id(profile_name: str, alma_client: type[AlmaAPIClient]) -> str:
    """
    Given an Alma integration profile name (or word from the name),
        return the profile id configured for remote storage.
    """
    profile_id: str = None
    parameters: dict = {"q": "name~Caiasoft"}
    profiles = alma_client.get_integration_profiles(parameters)
    # Find the first profile which is REMOTE_STORAGE
    for profile in profiles["integration_profile"]:
        if profile["type"]["value"] == "REMOTE_STORAGE":
            profile_id = profile["id"]
            break
    return profile_id


def get_job_id(profile_id: str, alma_client: type[AlmaAPIClient]) -> str:
    """
    Given an Alma integration profile id, return the job id for sending remote storage requests.
    """
    job_id: str = None
    parameters: dict = {"type": "SCHEDULED", "profile_id": profile_id}
    jobs = alma_client.get_jobs(parameters)
    # Find the first job which is FULFILLMENT
    for job in jobs["job"]:
        if job["category"]["value"] == "FULFILLMENT":
            job_id = job["id"]
            break
    return job_id


def run_job(job_id: str, alma_client: type[AlmaAPIClient]) -> None:
    """
    Given an Alma job id, run the job.
    """
    data = {}
    params = {"op": "run"}
    alma_client.run_job(job_id, data, params)


def main():
    alma_client = AlmaAPIClient(API_KEYS["CAIA_INTERNAL"])
    # Change profile_name as needed for search to work in your Alma instance
    profile_name = "Caiasoft"
    profile_id = get_profile_id(profile_name, alma_client)
    if profile_id:
        job_id = get_job_id(profile_id, alma_client)
    if job_id:
        now = datetime.now().strftime("%c")
        print(f"Running job {job_id} at {now}")
        run_job(job_id)


if __name__ == "__main__":
    main()
