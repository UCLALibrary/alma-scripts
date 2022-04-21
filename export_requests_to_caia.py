#!/usr/bin/env python3
"""
Finds and runs an Alma job to export remote storage requests.
See LICENSE.txt in this repository.
"""
import json
import requests # 3rd party module, requires installation
import time

# UCLA-specific API key management
from alma_api_keys import API_KEYS

BASE_URL = 'https://api-na.hosted.exlibrisgroup.com'
# UCLA-specific API key
API_KEY = API_KEYS['CAIA_INTERNAL']
HEADERS = {'Authorization': f'apikey {API_KEY}',
           'Accept': 'application/json',
           'Content-Type': 'application/json'}

def call_get_api(api, parameters):
	get_url = BASE_URL + api + parameters
	response = requests.get(get_url, headers=HEADERS)
	if (response.status_code != 200):
		#TODO: Real error handling
		print(response.status_code)
		print(response.headers)
		print(response.text)
		exit(1)
	return response

def call_post_api(api, parameters, data):
	post_url = BASE_URL + api + parameters
	response = requests.post(post_url, headers=HEADERS, data=data)
	if (response.status_code != 200):
		#TODO: Real error handling
		print(post_url)
		print(response.status_code)
		print(response.headers)
		print(response.text)
		exit(1)
	return response

def get_profile_id(profile_name):
	""" 
	Given an Alma integration profile name (or word from the name), return the profile id configured for remote storage.
	"""
	profile_id = None
	api = '/almaws/v1/conf/integration-profiles'
	parameters = f'/?q=name~{profile_name}'
	response = call_get_api(api, parameters)
	profiles = json.loads(response.text)
	# Find the first profile which is REMOTE_STORAGE
	for profile in profiles['integration_profile']:
		if (profile['type']['value'] == 'REMOTE_STORAGE'):
			profile_id = profile['id']
			break
	return profile_id

def get_job_id(profile_id):
	""" 
	Given an Alma integration profile id, return the job id for sending remote storage requests.
	"""
	job_id = None
	api = '/almaws/v1/conf/jobs'
	parameters = f'?type=SCHEDULED&profile_id={profile_id}'
	response = call_get_api(api, parameters)
	jobs = json.loads(response.text)
	# Find the first job which is FULFILLMENT
	for job in jobs['job']:
		if(job['category']['value'] == 'FULFILLMENT'):
			job_id = job['id']
			break
	return job_id

def run_job(job_id):
	"""
	Given an Alma job id, run the job.
	"""
	api = '/almaws/v1/conf/jobs'
	parameters = f'/{job_id}/?op=run'
	# Per docs, send an empty JSON object as data to run a scheduled job.
	# https://developers.exlibrisgroup.com/blog/Working-with-the-Alma-Jobs-API/
	data = '{}'
	call_post_api(api, parameters, data)

def main():
	# Change profile_name as needed for search to work in your Alma instance
	profile_name = 'Caiasoft'
	profile_id = get_profile_id(profile_name)
	if (profile_id):
		job_id = get_job_id(profile_id)
	if (job_id):
		now = time.asctime( time.localtime(time.time()) )
		print(f'Running job {job_id} at {now}')
		run_job(job_id)

if __name__ == '__main__':
	main()
