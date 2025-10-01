import csv
import argparse
import logging
import tomllib
from pathlib import Path
from datetime import datetime
from alma_api_client import AlmaAPIClient, AlmaAnalyticsClient, APIError
from pymarc import Field


def _get_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    :return: Parsed arguments for program as a Namespace object.
    """
    parser = argparse.ArgumentParser(description="Update bookplates in Alma.")
    parser.add_argument(
        "--spac_mappings_file", type=str, help="Path to the SPAC mappings .csv file"
    )
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
    parser.add_argument(
        "--start_index", type=int, help="Start processing report data at this index"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit the number of records to process"
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


def get_mms_report(analytics_api_key: str) -> list:
    """Get the report of MMS IDs and current 966 contents from Alma Analytics.

    :param analytics_api_key: Alma Analytics API key.
    :return: Alma Analytics report with MMS IDs and current 966 contents.
    """
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Cataloging/Reports/API/MMS IDs for 966 updates"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def get_spac_mappings(input_file: str) -> list:
    """Get SPAC mappings from a CSV file. Filter out any lines without a valid URL.

    :param input_file: Path to the SPAC mappings CSV file.
    :return: List of SPAC mappings.
    """
    spac_mappings = []
    with open(input_file, newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        for line in reader:
            # remove leading/trailing whitespace from all values
            line = {k: v.strip() for k, v in line.items()}
            # first, check if the line has a valid URL. If not, skip it.
            if not line["URL"]:
                continue
            elif line["URL"][:4] != "http":
                continue
            else:
                current_line = {
                    "SPAC": line["SPAC"],
                    "NAME": line["NAME"],
                    "URL": line["URL"],
                }
                spac_mappings.append(current_line)
    return spac_mappings


def needs_bookplate_update(old_field: Field, spac_mappings: list) -> bool:
    """Check if a 966 field matches a SPAC code. If so, assume it needs updating.

    :param old_field: The old 966 Pymarc Field to check.
    :param spac_mappings: List of SPAC mappings.
    :return: True if the 966 field needs updating, False otherwise.
    """
    # check if the SPAC code in the 966 field is in the list of SPAC mappings
    for spac_mapping in spac_mappings:
        if spac_mapping["SPAC"] == old_field.get_subfields("a")[0]:
            return True
    return False


def get_spac_info(spac_mappings: list, field_966: Field) -> dict:
    """Get the SPAC name and URL from the mappings to update a 966.

    :param spac_mappings: List of SPAC mappings.
    :param field_966: The 966 Pymarc Field to update.
    :return: Dictionary with SPAC name and URL.
    """
    for spac_mapping in spac_mappings:
        if spac_mapping["SPAC"] == field_966.get_subfields("a")[0]:
            return {"spac_name": spac_mapping["NAME"], "spac_url": spac_mapping["URL"]}
    return {"spac_name": "", "spac_url": ""}


def update_existing_966(field_966: Field, spac_name: str, spac_url: str):
    """Update the URL and bookplate text in an existing 966 field.

    :param field_966: The 966 Pymarc Field to update.
    :param spac_name: The new SPAC name.
    :param spac_url: The new SPAC URL.
    """
    # update $b for bookplate text
    field_966.delete_subfield("b")
    field_966.add_subfield("b", spac_name)
    # update $c for URL
    field_966.delete_subfield("c")
    # if spac_url is an empty string, don't add $c back in
    if spac_url:
        field_966.add_subfield("c", spac_url)


def update_bookplates(report: list, spac_mappings: list, client: AlmaAPIClient):
    """Update bookplates in a list of bibs.

    :param report: Alma Analytics report with MMS IDs to update.
    :param spac_mappings: List of SPAC mappings.
    :param client: Alma API client.
    """
    # initialize counters
    total_bibs_updated = 0
    total_bibs_skipped = 0
    total_bibs_errored = 0

    for report_index, item in enumerate(report):
        logging.info(f"Beginning processing {len(report)} bib e-bookplates")
        mms_id = item["MMS Id"]
        bib_was_updated = False

        try:
            alma_bib = client.get_bib_record(bib_id=mms_id)
        except APIError as e:
            logging.error(
                f"AlmaAPIClient returned an error "
                f"while finding MMS ID {mms_id}, index {report_index}: {e}"
            )
            total_bibs_errored += 1
            continue
        except Exception:
            # if we get an Exception other than APIError, halt the script.
            # Report is sorted by MMS ID, so we can use this to resume later if needed.
            logging.error(
                f"Unexpected response for MMS ID {mms_id}, index {report_index}. Exiting."
            )
            exit()

        pymarc_record = alma_bib.marc_record
        if not pymarc_record:
            logging.error(
                f"Error converting MMS ID {mms_id}, index {report_index} to Pymarc."
            )
            total_bibs_errored += 1
            continue
        for field_966 in pymarc_record.get_fields("966"):
            if needs_bookplate_update(field_966, spac_mappings):
                # get the SPAC name and URL from the mappings
                spac_info = get_spac_info(spac_mappings, field_966)
                spac_name = spac_info["spac_name"]
                spac_url = spac_info["spac_url"]
                update_existing_966(field_966, spac_name, spac_url)
                logging.info(
                    f"Updated bookplate. MMS ID: {mms_id}, SPAC Name: {spac_name}",
                )
                bib_was_updated = True

        if bib_was_updated:
            alma_bib.marc_record = pymarc_record
            client.update_bib_record(bib_id=mms_id, bib_record=alma_bib)
            total_bibs_updated += 1
        else:
            # this case shouldn't happen, since report is limited to records that need updating
            # log it in case it does
            total_bibs_skipped += 1
            logging.info(f"Skipping MMS ID {mms_id}. No 966 updates needed.")

        # every 1% of records, log progress
        # Take 1%, round down, add 1 to avoid 0 when length < 100
        progress_interval = (len(report) // 100) + 1
        if report_index % progress_interval == 0:
            logging.info(f"Processed {report_index} bibs. Last MMS ID: {mms_id}")

    logging.info("Finished adding ebookplates.")
    logging.info(f"{total_bibs_updated} bibs updated.")
    logging.info(f"{total_bibs_skipped} bibs skipped with no 966 updates needed.")
    logging.info(f"{total_bibs_errored} bibs skipped due to errors.")


def main():
    """Entry point for the script."""
    args = _get_arguments()
    config = _get_config(args.config_file)
    _configure_logging(args.log_level)

    if args.production:
        logging.info("Using production Alma API key")
        alma_api_key = config["alma_api_keys"]["DIIT_SCRIPTS"]
    else:  # default to sandbox
        logging.info("Using sandbox Alma API key")
        alma_api_key = config["alma_api_keys"]["SANDBOX"]

    # analytics only available in prod environment
    analytics_api_key = config["alma_api_keys"]["DIIT_ANALYTICS"]
    report = get_mms_report(analytics_api_key)

    # if a start index is provided, slice the report to start at that index
    if args.start_index:
        report = report[args.start_index :]
    # if a limit is provided, slice the report to that limit
    if args.limit:
        report = report[: args.limit]

    client = AlmaAPIClient(alma_api_key)
    spac_mappings = get_spac_mappings(args.spac_mappings_file)

    update_bookplates(report, spac_mappings, client)


if __name__ == "__main__":
    main()
