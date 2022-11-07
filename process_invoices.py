#!/usr/bin/env python3
from alma_api_client import AlmaAPIClient
from alma_api_keys import API_KEYS
from datetime import datetime
from invoice import Invoice
from sftp_credentials import ALMA, PAC
import argparse
import os
import sys  # for exit() while testing
import pprint as pp
import pysftp
import re
import xml.etree.ElementTree as ET


def get_invoice_profile_id():
    params = {"q": "code~UCLA_INVOICES", "type": "PAYMENT"}
    profile_id = None
    profiles = client.get_integration_profiles(params)
    # This one is unique, but caller is responsible for finding the right ID
    if profiles["total_record_count"] == 1:
        profile_id = profiles["integration_profile"][0]["id"]
    else:
        raise ValueError("Multiple profiles found")
    print(f"profile_id: {profile_id}")
    return profile_id


def get_invoice_job_id(profile_id):
    params = {"type": "SCHEDULED", "profile_id": profile_id}
    jobs = client.get_jobs(params)
    # Invoice/ERP profile has several jobs; must find right one by description
    # jobs is a dictionary, with 'job' list of dictionaries and 'total_record_count' (int)
    job_id = None
    for job in jobs["job"]:
        if job["description"] == "Exports invoices to ERP system":
            job_id = job["id"]
    if job_id is not None:
        print(f"job_id: {job_id}")
    else:
        raise ValueError("Job not found")
    return job_id


def run_job(job_id, run_job=True):
    # Run the invoice export job
    # Returns the instance_id of the running job
    # Per docs, send an empty JSON object as data to run a scheduled job.
    # https://developers.exlibrisgroup.com/blog/Working-with-the-Alma-Jobs-API/
    if run_job:
        data = {}
        params = {"op": "run"}
        response = client.run_job(job_id, data, params)
        # Running a job returns a link with the job instance at end
        instance_id = response["additional_info"]["link"].split("/")[-1]
    else:
        # Real, completed instance for testing in the sandbox
        instance_id = "5905109070006533"

    print(f"instance_id: {instance_id}")
    return instance_id


def get_invoice_counters(response):
    # List of dictionaries
    counter_data = response["counter"]
    message_map = {
        "Number of Invoices processed": "Processed",
        "Number of Invoices failed": "Failed",
        "Number of invoices finished successfully": "Successful",
    }
    counters = {}
    for counter in counter_data:
        alma_message = counter["type"]["value"]
        label = message_map[alma_message]
        value = int(counter["value"])
        counters[label] = value
    return counters


def retrieve_alma_file(instance_id):
    # Alma-generated filename starts with instance_id, ends with .xml
    pattern = re.compile("^" + instance_id + "-.*\.xml$")
    # Local filename: today's YYYYMMDD.xml

    with pysftp.Connection(ALMA["server"], username=ALMA["user"]) as sftp:
        print("Connected")
        sftp.cwd("alma/erp")

        files = sftp.listdir()
        for file in files:
            # We only care about the file created by the specified job instance.
            match = pattern.match(file)
            if match:
                local_file = datetime.today().strftime("%Y%m%d") + ".xml"
                print(f"{file} found - downloading as {local_file}")
                sftp.get(file, local_file)
                # Back up the file on the SFTP server
                sftp.rename(file, file + ".BAK")
                break
            else:
                print(f"Skipping {file}")

        print(sftp.listdir())
    return local_file


def upload_pac_file(pac_file):
    # PAC requires files be uploaded with the same name; ours have dates for archiving
    pac_sftp_file = "LIBRY-APINTRFC"
    with pysftp.Connection(
        PAC["server"], username=PAC["user"], password=PAC["password"]
    ) as sftp:
        print("Connected")
        sftp.put(pac_file, pac_sftp_file, confirm=True)
        # Get full directory listing
        for line in sftp.listdir_attr():
            print(line)


def _get_pac_filename():
    # Daily files, named like: LIBRY-APINTRFC.YYYYMMDD
    # where YYYYMMDD is today's date.
    today = datetime.strftime(datetime.now(), "%Y%m%d")
    file_name = f"LIBRY-APINTRFC.{today}"
    return file_name


def _write_invoice_to_file(pac_invoice, pac_file):
    with open(pac_file, "a") as f:
        f.writelines(pac_invoice)


# For testing only, modify invoice number to reflect test batch
def _inject_test_number(invoice, test_batch):
    invoice.data["invoice_number"] += test_batch
    invoice.data["pac_invoice_number"] = invoice._format_invoice_number()
    invoice.data["pac_lines"] = invoice._get_pac_lines()


def create_pac_invoices(xml_file, dump_dict):
    PROD = True
    pac_file = _get_pac_filename()
    if os.path.exists(pac_file):
        os.remove(pac_file)
    root = ET.parse(xml_file).getroot()
    # Namespace
    ns = {"alma": "http://com/exlibris/repository/acq/invoice/xmlbeans"}
    # Loop through Alma XML data to build pac_invoice dictionary
    for alma_invoice in root.findall(".//alma:invoice", ns):
        try:
            invoice = Invoice(alma_invoice, ns)
            #####_inject_test_number(invoice, '-2')

            if dump_dict:
                invoice.dump()

            if PROD:
                if invoice.is_valid():
                    _write_invoice_to_file(invoice.get_pac_format(), pac_file)
            else:
                # TODO: Changes to is_valid()
                invoice.is_valid()
            # TODO: Real logging
            print(invoice.data["validation_message"])

        except Exception as ex:
            bad_invoice_number = alma_invoice.findtext("alma:invoice_number", None, ns)
            print(ex)
            print(f"ERROR: Bad invoice {bad_invoice_number}")

    return pac_file


def get_xml_from_alma():
    global client
    client = AlmaAPIClient(API_KEYS["DIIT_SCRIPTS"])
    profile_id = get_invoice_profile_id()
    job_id = get_invoice_job_id(profile_id)
    instance_id = run_job(job_id)
    # Wait for job to finish
    response = client.wait_for_completion(job_id, instance_id)
    # Eventually, get counter messages (invoices processed etc.) and times.
    counters = get_invoice_counters(response)
    pp.pprint(response)

    # If no invoices exported, no file is created; otherwise file is
    # {instance_id}-some_data.xml
    xml_file = retrieve_alma_file(instance_id)
    return xml_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dump_invoice", help="Dump invoice as dictionary", action="store_true"
    )
    parser.add_argument(
        "-s", "--skip_upload", help="Skip PAC upload", action="store_true"
    )
    parser.add_argument("-x", "--xml_file", help="XML file to process", default=None)
    args = parser.parse_args()

    # If xml_file is passed via command-line, use it;
    # otherwise, extract Alma invoices and retrieve xml_file from server.
    if args.xml_file is None:
        xml_file = get_xml_from_alma()
    else:
        xml_file = args.xml_file

    # Creates PAC file and returns its name
    pac_file = create_pac_invoices(xml_file, args.dump_invoice)

    # Upload PAC file to UCLA ITS sftp server
    if args.skip_upload:
        print(f"{pac_file} NOT uploaded")
    else:
        if os.path.exists(pac_file):
            upload_pac_file(pac_file)
        else:
            print(f"{pac_file} does not exist")


if __name__ == "__main__":
    main()
