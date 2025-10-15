import warnings  # for paramiko TripleDES deprecation
import argparse
import os
import re
import tomllib

import xml.etree.ElementTree as ET
from alma_api_client import APIError, APIResponse, AlmaAPIClient
from datetime import datetime
from invoice import Invoice
from pprint import pprint

# Ignore this deprecation warning otherwise printed on pysftp import.
warnings.filterwarnings("ignore", message="TripleDES has been moved")
import pysftp  # noqa


def _get_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    :return: Parsed arguments for program as a Namespace object.
    """
    parser = argparse.ArgumentParser(description="Process daily Alma invoices.")
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
        "-d", "--dump_invoices", help="Dump invoices as dictionary", action="store_true"
    )
    parser.add_argument(
        "-s", "--skip_upload", help="Skip PAC upload", action="store_true"
    )
    parser.add_argument("-x", "--xml_file", help="XML file to process", default=None)
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


def _get_instance_id(api_response: APIResponse) -> str:
    """Return the Alma instance id from an Alma API response, representing the specific
    instance of a running job.

    :param api_response: An Alma `APIResponse` containing job data.
    :return instance_id: The instance id.
    """
    # The instance id is embedded in an API link, and in a textual message,
    # both found in `additional_info`.  Use the link, ignore the message.
    api_link = api_response.api_data.get("additional_info", {}).get("link", "")

    # The instance id is the final element in the link, a URI like
    # 'https://domain/almaws/v1/conf/jobs/job_id/instances/instance_id'
    # api_link is always a string, and even splitting "" is safe; this will return ""
    # in that case.
    instance_id = api_link.split("/")[-1]
    return instance_id


def export_invoices(alma_api_key: str) -> str:
    """Use the Alma API to run the job to export invoices.

    :param alma_api_key: An Alma API key with permission to run jobs.
    :return:
    """
    client = AlmaAPIClient(alma_api_key)

    # Initialize variables to make type-checker happy.
    profile_id = ""
    job_id = ""

    # Hard-coded integration profile code, should be unique;
    # not using hard-coded profile id, in case that's subject to change.
    profile_code = "UCLA_INVOICES"
    # This profile has multiple components; we must have the PAYMENT one.
    profile_parameters = {"q": f"code~{profile_code}", "type": "PAYMENT"}
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
        # Print error from _get_profile_id().
        print(e)
        exit()

    # An empty profile_id, if the profile wasn't found and somehow not detected already,
    # can retrieve multiple irrelevant jobs, leading to problems.
    assert profile_id != ""

    # We've got one real profile id, so try to find the job we need.
    job_parameters = {
        "profile_id": profile_id,
        "type": "SCHEDULED",
    }
    try:
        api_response = client.get_jobs(parameters=job_parameters)
        # Jobs cannot be filtered on name via API, so use
        # job name as secondary check.
        job_name = "ERP export using profile UCLA Invoices PA"
        job_id = _get_job_id(api_response, job_name)
    except APIError as e:
        # Print detailed error messages from API response.
        print(e.error_messages)
        exit()
    except ValueError as e:
        # Print error from _get_job_id()
        print(e)
        exit()

    # Still paranoid.
    assert job_id != ""

    # We've got a real job id, so try to run it.
    now = datetime.now().strftime("%c")
    print(f"Running job {profile_id=}, {job_id=} at {now}")
    # Run the job, capturing response so we can monitor for completion.
    # TODO: Consider implementing API call for "Retrieve Job Instance Event Details",
    # which can provide useful info when the job does complete.
    # TODO: Consider adding wait_for_completion flag to client.run_job()?
    try:
        run_parameters = {"op": "run"}
        api_response = client.run_job(job_id, parameters=run_parameters)
        instance_id = _get_instance_id(api_response)
    except APIError as e:
        # Print detailed error messages from API response.
        print(e.error_messages)
        exit()

    print(f"Waiting for job to finish: {instance_id=}")
    try:
        api_response = client.wait_for_completion(job_id, instance_id)
        # The response has useful info.
        pprint(api_response.api_data, width=132)
    except APIError as e:
        # Print detailed error messages from API response.
        print(e.error_messages)
        exit()

    # The instance id is used in the invoice file name.
    return instance_id


def retrieve_alma_file(instance_id: str, config: dict) -> str:
    """Retrieve Alma invoice file from SFTP server.

    :param instance_id: Alma id for the job instance which generated the file.
    This is used in the file name.
    """
    # Alma-generated filename starts with instance_id, ends with .xml
    pattern = re.compile("^" + instance_id + "-.*\\.xml$")

    alma_sftp_server = config["sftp"]["alma"]["server"]
    alma_sftp_user = config["sftp"]["alma"]["user"]
    with pysftp.Connection(alma_sftp_server, username=alma_sftp_user) as sftp:
        # If no Alma invoice file, this will be returned as the filename.
        local_file: str = ""
        print("Connected")
        sftp.cwd("alma/erp")
        files = sftp.listdir()
        for file in files:
            # We only care about the file created by the specified job instance.
            match = pattern.match(file)
            if match:
                # Local filename: today's YYYYMMDD.xml
                local_file = datetime.today().strftime("%Y%m%d") + ".xml"
                print(f"{file} found - downloading as {local_file}")
                sftp.get(file, local_file)
                # Back up the file on the SFTP server
                sftp.rename(file, file + ".BAK")
                break
            else:
                print(f"Skipping {file}")

    return local_file


def _get_pac_filename() -> str:
    """Get the local name of the PAC invoice file, date stamped
    for our archives.

    :return pac_filename: The name of our PAC invoice file.
    """
    # Daily files, named like: LIBRY-APINTRFC.YYYYMMDD
    # where YYYYMMDD is today's date.
    today = datetime.strftime(datetime.now(), "%Y%m%d")
    pac_filename = f"LIBRY-APINTRFC.{today}"
    return pac_filename


def _write_invoice_to_file(pac_invoice, pac_filename) -> None:
    """Append the data for a PAC invoice to a file.

    :param pac_invoice: The data for one invoice, in PAC form.
    :param pac_filename: The name of the PAC invoice file.
    """
    with open(pac_filename, "a") as f:
        f.writelines(pac_invoice)


def create_pac_invoices(
    xml_file: str, dump_invoices: bool, production_mode: bool = False
):
    """Create PAC (UCLA accounting) invoices from Alma XML invoices.

    :param xml_file: Name of the XML file containing Alma invoices.
    :param dump_invoices: Whether to dump invoices (to stdout) as Python dictionaries for debugging.
    :param production_mode: Whether to run this in production mode. By default, this is False,
    meaning invoices will be converted and validated, but not written to file or uploaded to PAC.
    """
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

            if dump_invoices:
                invoice.dump()

            if production_mode:
                if invoice.is_valid():
                    _write_invoice_to_file(invoice.get_pac_format(), pac_file)
            else:
                # TODO: Cleanup, but requires changing internals of Invoice()
                invoice.is_valid()
            # Whether production or not, print message set by invoice.is_valid().
            print(invoice.data["validation_message"])

        # TODO: What actual exceptions are relevant?
        # Presumably xml.etree.ElementTree.ParseError could be raised in the actual
        # Invoice() parsing; that all needs review and refactoring.
        # For now (20251014/akohler) this is acceptable.
        except Exception as ex:
            bad_invoice_number = alma_invoice.findtext("alma:invoice_number", None, ns)
            print(ex)
            print(f"ERROR: Bad invoice {bad_invoice_number}")

    return pac_file


def upload_pac_file(pac_file: str, config: dict) -> None:
    """Upload the converted invoice file to the campus PAC system.

    :param pac_file: The name of the PAC invoice file.
    """
    # PAC requires files be uploaded with the same name every day;
    # ours have dates for archiving.
    pac_sftp_file = "LIBRY-APINTRFC"
    pac_sftp_server = config["sftp"]["pac"]["server"]
    pac_sftp_user = config["sftp"]["pac"]["user"]
    pac_sftp_password = config["sftp"]["pac"]["password"]
    with pysftp.Connection(
        pac_sftp_server, username=pac_sftp_user, password=pac_sftp_password
    ) as sftp:
        print("Connected")
        sftp.put(pac_file, pac_sftp_file, confirm=True)
        # Get full directory listing to see what's present.
        for line in sftp.listdir_attr():
            print(line)


def main():
    args = _get_arguments()
    config = _get_config(args.config_file)

    if args.production:
        alma_api_key = config["alma_api_keys"]["DIIT_SCRIPTS"]
        production_mode = True
    else:  # default to sandbox
        print("Using SANDBOX")
        alma_api_key = config["alma_api_keys"]["SANDBOX"]
        # Override skip_upload: data from Alma Sandbox should not go to PAC.
        args.skip_upload = False
        production_mode = False

    # If xml_file is passed via command-line, use it;
    # otherwise, extract Alma invoices and retrieve xml_file from server.
    if args.xml_file is None:
        instance_id = export_invoices(alma_api_key)
        xml_file = retrieve_alma_file(instance_id, config)
    else:
        xml_file = args.xml_file

    # Creates PAC file and returns its name
    if xml_file:
        pac_file = create_pac_invoices(
            xml_file, dump_invoices=args.dump_invoices, production_mode=production_mode
        )
        # Upload PAC file to UCLA ITS sftp server
        if args.skip_upload:
            print(f"{pac_file} NOT uploaded")
        else:
            if os.path.exists(pac_file):
                upload_pac_file(pac_file, config)
            else:
                print(f"{pac_file} does not exist")
    else:
        print("NO XML FILE TO PROCESS")


if __name__ == "__main__":
    main()
