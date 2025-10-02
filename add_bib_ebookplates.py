import csv
import copy
import argparse
import logging
import tomllib
from pathlib import Path
from datetime import datetime
from alma_api_client import AlmaAPIClient, AlmaAnalyticsClient, APIError
from pymarc import Field, Record, Subfield, Indicators


def _get_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    :return: Parsed arguments as a Namespace object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spac_mappings_file",
        type=str,
        required=True,
        help="Path to the SPAC mappings .csv file",
    )
    parser.add_argument(
        "--environment",
        choices=["sandbox", "production", "test"],
        required=True,
        help="Alma environment (sandbox or production), or 'test' for a small test set.",
    )
    parser.add_argument(
        "--config_file",
        type=str,
        default="secret_config.toml",
        help="Path to TOML config file with API keys",
    )
    parser.add_argument(
        "--start_index", type=int, help="Start processing report data at this index"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit the number of records processed"
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


def _configure_logging(log_level: str):
    """Returns a logger for the current application.
    A unique log filename is created using the current time, and log messages
    will use the name in the 'logger' field.

    :param log_level: Log level to use
    """
    name = Path(__file__).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging_file = Path("logs", f"{name}_{timestamp}.log")  # Log to `logs/` dir
    logging_file.parent.mkdir(parents=True, exist_ok=True)  # Make `logs/` dir, if none
    logging.basicConfig(
        filename=logging_file,
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    # always suppress urllib3 logs with lower level than WARNING
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_fund_code_report(analytics_api_key: str) -> list:
    """Get the report of MMS IDs and fund codes from Alma Analytics.

    :param analytics_api_key: API key for Alma Analytics
    :return: List of dicts with report data
    """
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Acquisitions/Reports/API/MMS ID by SPAC"
    )
    aac.set_report_path(report_path)
    try:
        report = aac.get_report()
    except APIError as e:
        logging.error(f"AlmaAnalyticsClient returned an error: {e.error_messages}")
        exit()
    return report


def get_report_ebookplates(report: list, input_file: str) -> list:
    """Add SPAC ebookplate info to each item in the report.

    :param report: List of dicts with report data
    :param input_file: Path to the SPAC mappings .csv file
    :return: New list of dicts with SPAC info added
    """
    # copy SPAC mappings into list of dicts for looping over
    spac_mappings = []
    with open(input_file, newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        for line in reader:
            # remove leading/trailing whitespace from all values
            line = {k: v.strip() for k, v in line.items()}
            # check the FUND column for commas, indicating multiple funds
            if "," in line["FUND"]:
                # split on commas and add a new line for each fund
                funds = line["FUND"].split(", ")
                for fund in funds:
                    current_line = copy.deepcopy(line)
                    current_line["FUND"] = fund
                    spac_mappings.append(current_line)
            else:
                spac_mappings.append(line)

    # create new list of dicts for items to avoid changing as we iterate over report
    new_report = []
    for item in report:
        for line in spac_mappings:
            if line["FUND"] == item["Fund Code"]:
                current_item = copy.deepcopy(item)
                current_item["spac_code"] = line["SPAC"]
                current_item["spac_name"] = line["NAME"]
                current_item["spac_url"] = line["URL"]
                new_report.append(current_item)

    return new_report


def is_new_966(old_record: Record, spac_code: str) -> bool:
    """Check all 966 fields in a record to see if a new 966 field is needed.

    :param old_record: pymarc Record object
    :param spac_code: SPAC code to check for in existing 966 fields
    :return: True if no existing 966 field matches the SPAC code, False otherwise
    """
    for field_966 in old_record.get_fields("966"):
        # match only subfield a
        if spac_code in field_966.get_subfields("a"):
            return False
    return True


def needs_bookplate_update(
    old_field: Field, spac_code: str, spac_name: str, spac_url: str
) -> bool:
    """Check if a 966 field matches the SPAC code, but needs an update to URL or name.

    :param old_field: pymarc Field object for the existing 966 field
    :param spac_code: SPAC code to check for in existing 966 field
    :param spac_name: SPAC name to check for in existing 966 field
    :param spac_url: SPAC URL to check for in existing 966 field
    :return: True if the existing 966 field needs an update, False otherwise
    """
    # First, match on subfield a. If no match, this field doesn't need updating.
    # get_subfields returns a list, we expect only one $a,b,c per 966 field
    if spac_code != old_field.get_subfields("a")[0]:
        return False
    # If the new URL is an empty string, check if $c exists. If it does, update is needed.
    elif (not spac_url) and (old_field.get_subfields("c")):
        return True
    # If the new URL is not empty, check if it matches the existing $c. If not, update is needed.
    elif spac_url:
        # if we have a URL but no $c subfield, update is needed
        if not old_field.get_subfields("c"):
            return True
        # otherwise, compare the URL in the 966 field to the new URL
        if spac_url != old_field.get_subfields("c")[0]:
            return True
    # Now check if the bookplate text needs updating
    if spac_name != old_field.get_subfields("b")[0]:
        return True
    # Otherwise, no update is needed
    return False


def add_new_966(record: Record, spac_code: str, spac_name: str, spac_url: str) -> None:
    """Add a new 966 field to a pymarc record, with SPAC and bookplate data.

    :param record: pymarc Record object
    :param spac_code: SPAC code to add in subfield a
    :param spac_name: SPAC name to add in subfield b
    :param spac_url: SPAC URL to add in subfield c (if not empty)
    """
    subfields = []
    subfields.append(Subfield(code="a", value=spac_code))
    subfields.append(Subfield(code="b", value=spac_name))
    subfields.append(Subfield(code="9", value="LOCAL"))
    if spac_url:
        subfields.append(Subfield(code="c", value=spac_url))
    record.add_field(
        Field(
            tag="966",
            indicators=Indicators("", ""),
            subfields=subfields,
        )
    )


def update_existing_966(field_966: Field, spac_name: str, spac_url: str) -> None:
    """Update the URL and bookplate text in an existing 966 field.

    :param field_966: pymarc Field object for the existing 966 field
    :param spac_name: SPAC name to update in subfield b
    :param spac_url: SPAC URL to update in subfield c (if not empty)
    """
    # update $b for bookplate text
    field_966.delete_subfield("b")
    field_966.add_subfield("b", spac_name)
    # update $c for URL
    field_966.delete_subfield("c")
    # if spac_url is an empty string, don't add $c back in
    if spac_url:
        field_966.add_subfield("c", spac_url)


def add_bookplates(
    report_data: list, alma_api_key: str, spac_mappings_file: str
) -> None:
    """Main function to add ebookplates to bib records based on fund codes.

    :param report_data: List of dicts with report data
    :param alma_api_key: API key for Alma
    :param spac_mappings_file: Path to the SPAC mappings .csv file
    """

    logging.info(f"Beginning processing {len(report_data)} bib e-bookplates")

    report_with_ebookplates = get_report_ebookplates(report_data, spac_mappings_file)

    client = AlmaAPIClient(alma_api_key)

    # initialize counters
    total_bibs_updated = 0
    total_bibs_skipped = 0
    total_bibs_errored = 0

    for report_index, item in enumerate(report_with_ebookplates):
        mms_id = item["MMS Id"]
        spac_code = item["spac_code"]
        spac_name = item["spac_name"]
        spac_url = item["spac_url"]
        bib_was_updated = False

        # get bib from Alma
        try:
            alma_bib = client.get_bib_record(bib_id=mms_id)
        except APIError as e:
            logging.error(
                f"AlmaAPIClient returned an error "
                f"while finding MMS ID {mms_id}, index {report_index}: {e.error_messages}"
            )
            total_bibs_errored += 1
            continue
        except Exception:
            # if we get an Exception other than APIError, halt the script.
            # Report is sorted by MMS ID, so we can use this to resume later if needed.
            logging.error(
                f"Unexpected response for MMS ID {mms_id}, index {report_index}. Exiting. ",
                f"Error message: {Exception}",
            )
            exit()

        # Get pymarc record from Alma bib
        pymarc_record = alma_bib.marc_record

        # If pymarc_record is None, log error and skip to next record
        if pymarc_record is None:
            logging.error(f"No MARC record found for MMS ID {mms_id}. Skipping.")
            total_bibs_errored += 1
            continue

        if is_new_966(pymarc_record, spac_code):
            add_new_966(pymarc_record, spac_code, spac_name, spac_url)
            logging.debug(
                f"Added new bookplate to bib. MMS ID: {mms_id}, SPAC Name: {spac_name}"
            )
            bib_was_updated = True
        else:
            for field_966 in pymarc_record.get_fields("966"):
                if needs_bookplate_update(field_966, spac_code, spac_name, spac_url):
                    update_existing_966(field_966, spac_name, spac_url)
                    logging.debug(
                        f"Updated bookplate. MMS ID: {mms_id}, SPAC Name: {spac_name}",
                    )
                    bib_was_updated = True

        if bib_was_updated:
            alma_bib.marc_record = pymarc_record
            client.update_bib_record(bib_id=mms_id, bib_record=alma_bib)
            total_bibs_updated += 1
        else:
            total_bibs_skipped += 1
            logging.debug(f"Skipping MMS ID {mms_id}. No 966 updates needed.")

        # every 1% of records, log progress
        total_bibs_processed = (
            total_bibs_updated + total_bibs_skipped + total_bibs_errored
        )
        # Take 1%, round down, add 1 to avoid 0 when length < 100
        progress_interval = (len(report_with_ebookplates) // 100) + 1
        if total_bibs_processed % progress_interval == 0:
            logging.info(
                f"Processed {total_bibs_processed} bibs. Last MMS ID: {mms_id}"
            )

    logging.info("Finished adding ebookplates.")
    logging.info(f"{total_bibs_updated} bibs updated.")
    logging.info(f"{total_bibs_skipped} bibs skipped with no 966 updates needed.")
    logging.info(f"{total_bibs_errored} bibs skipped due to errors.")


def main() -> None:
    """Main function to run the script."""
    args = _get_arguments()
    config = _get_config(args.config_file)
    _configure_logging(args.log_level)

    # initialize variables for report_data and API keys
    report_data = []
    analytics_api_key = ""
    alma_api_key = ""

    if args.environment == "test":
        # test data for sandbox environment
        # these MMS IDs are real, but fund codes are fake to align with test SPAC mappings file
        # test file is in repo at tests/data/sample_SPAC_mappings.csv
        report_data = [
            # case 1: SPAC1, with URL
            {"MMS Id": "9911656853606533", "Fund Code": "FUND2A"},
            # case 2: SPAC3, no URL
            {"MMS Id": "9990572683606533", "Fund Code": "FUND3"},
        ]
        alma_api_key = config["alma_api_keys"]["SANDBOX"]

    elif args.environment == "sandbox":
        # use production analytics key for sandbox environment, since sandbox doesn't have analytics
        analytics_api_key = config["alma_api_keys"]["DIIT_ANALYTICS"]
        alma_api_key = config["alma_api_keys"]["SANDBOX"]
        report_data = get_fund_code_report(analytics_api_key)

    elif args.environment == "production":
        analytics_api_key = config["alma_api_keys"]["DIIT_ANALYTICS"]
        alma_api_key = config["alma_api_keys"]["DIIT_SCRIPTS"]
        report_data = get_fund_code_report(analytics_api_key)

    # if a start index is provided, slice the report to start at that index
    if args.start_index is not None:
        report_data = report_data[args.start_index :]

    # if a limit is provided, slice the report to that limit
    if args.limit is not None:
        report_data = report_data[: args.limit]

    spac_mappings_file = args.spac_mappings_file
    add_bookplates(report_data, alma_api_key, spac_mappings_file)


if __name__ == "__main__":
    main()
