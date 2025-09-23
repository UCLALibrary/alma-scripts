import argparse
import logging
from alma_api_client import (
    AlmaAPIClient,
    AlmaAnalyticsClient,
)
from pymarc import Field
from datetime import datetime
from retry.api import retry_call
from pathlib import Path
import json
import csv
import tomllib

# for error handling
from requests.exceptions import ConnectTimeout


def _get_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    :return: Parsed arguments for program as a Namespace object."""
    parser = argparse.ArgumentParser(
        description="Remove bookplates from Alma holdings records."
    )
    parser.add_argument(
        "--production",
        action="store_true",
        default=False,
        help="Run script using production API keys",
    )
    parser.add_argument(
        "--config_file",
        type=str,
        default="secret_config.toml",
        help="Path to the configuration file with API keys",
    )
    parser.add_argument(
        "--log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    parser.add_argument(
        "--start_index",
        type=int,
        default=0,
        help="Start index for the bookplates report",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of records to process",
    )
    parser.add_argument(
        "--report_file",
        type=str,
        default=None,
        help="Path to local report data file, to use instead of fetching from Alma Analytics",
    )
    return parser.parse_args()


def _get_config(config_file_name: str) -> dict:
    """Returns configuration for this program, loaded from TOML file.

    :param config_file_name: Path to the configuration file.
    :return: Configuration dictionary."""

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


def _write_to_file(data: list, file_path: str | Path):
    """Write data to a file.

    :param data: Data to write
    :param file_path: Path to the file
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _load_report_data_from_file(file_path: str) -> list:
    """Load the report data from a file.

    :param file_path: Path to the file
    :return: Report data
    """
    with open(file_path, "r", encoding="utf-8") as f:
        if file_path.endswith(".json"):
            return json.load(f)
        elif file_path.endswith(".csv"):
            return list(csv.DictReader(f))
        else:
            raise ValueError(f"Report must be either JSON or CSV: {file_path}")


def get_bookplates_report(analytics_api_key: str) -> list:
    """Get the bookplates report from Alma Analytics.

    :param analytics_api_key: Alma Analytics API key
    :return: Bookplates report data
    """
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Cataloging/Reports/API/Combined Bookplates to Remove"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def needs_966_removed(field_966: Field, bookplates_to_leave: list) -> bool:
    """Determine if a 966 field needs to be removed.

    :param field_966: 966 field to test
    :param bookplates_to_leave: List of bookplates to leave
    :return: True if the 966 field needs to be removed, False otherwise
    """
    # only remove 966s that don't contain any of the SPACs in bookplates_to_leave in $a
    subfields = field_966.get_subfields("a")
    for term in bookplates_to_leave:
        for subfield in subfields:
            if term in subfield:
                logging.info(f"Found {term} in 966 field")
                return False
    return True


def needs_856_removed(field_856: Field) -> bool:
    """Determine if a 856 field needs to be removed.

    :param field_856: 856 field to test
    :return: True if the 856 field needs to be removed, False otherwise
    """
    # only remove 856s that contain "Bookplate" in $3
    subfields = field_856.get_subfields("3")
    for subfield in subfields:
        if "Bookplate" in subfield:
            return True
    return False


def remove_bookplates(
    report_data: list,
    client: AlmaAPIClient,
    bookplates_to_leave: list,
    start_index: int = 0,
    limit: int = 0,
):
    """Remove bookplates from Alma holdings records.

    :param report_data: Bookplate report data
    :param client: Alma API client
    :param bookplates_to_leave: List of bookplates to leave
    :param start_index: Start index for the report data
    :param limit: Limit the number of records to process
    """
    # slice the report data to specified start index and limit
    report_data = report_data[start_index:]
    if limit > 0:
        report_data = report_data[:limit]

    logging.info(f"Processing {len(report_data)} bookplates")
    errored_holdings_count = 0
    updated_holdings_count = 0
    skipped_holdings_count = 0
    errored_holdings = []
    for index, item in enumerate(report_data):
        logging.info(f"Current report index: {index + start_index}")
        mms_id = item["MMS Id"]
        holding_id = item["Holding Id"]
        try:
            alma_holding_record = retry_call(
                client.get_holding_record,
                fargs=(mms_id, holding_id),
                tries=3,
                delay=20,
                backoff=2,
            )
        except ConnectTimeout as e:
            logging.error(
                f"Error finding MMS ID {mms_id}, Holding ID {holding_id}: {e}"
            )
            errored_holdings_count += 1
            errored_holdings.append({"MMS Id": mms_id, "Holding Id": holding_id})
            continue
        alma_holding_xml = alma_holding_record.alma_xml
        # make sure we got a valid holding record
        # TODO: use built-in error handling here, when it's added to the new Alma API client
        if (
            b"is not valid" in alma_holding_xml
            or b"INTERNAL_SERVER_ERROR" in alma_holding_xml
            or b"Search failed" in alma_holding_xml
            or alma_holding_xml is None
        ):
            logging.error(
                f"Error finding MMS ID {mms_id}, Holding ID {holding_id}. Skipping this record."
            )
            errored_holdings_count += 1
            errored_holdings.append({"MMS Id": mms_id, "Holding Id": holding_id})

        else:
            # convert to Pymarc to handle fields and subfields
            pymarc_record = alma_holding_record.marc_record
            if not pymarc_record:
                logging.error(
                    f"Error converting MMS ID {mms_id}, Holding ID {holding_id} to Pymarc."
                )
                errored_holdings_count += 1
                errored_holdings.append({"MMS Id": mms_id, "Holding Id": holding_id})
                continue
            # get initial bytes for comparison later
            initial_bytes = pymarc_record.as_marc()
            # examine fields to see if any need to be removed
            pymarc_966_fields = pymarc_record.get_fields("966")
            pymarc_856_fields = pymarc_record.get_fields("856")
            if not pymarc_966_fields and not pymarc_856_fields:
                logging.info(
                    f"No 966 or 856 found for MMS ID {mms_id}, Holding ID {holding_id}"
                )
            else:
                for field_966 in pymarc_966_fields:
                    if needs_966_removed(field_966, bookplates_to_leave):
                        pymarc_record.remove_field(field_966)
                        logging.info(
                            f"Removing 966 bookplate from MMS ID {mms_id}, "
                            f"Holding ID {holding_id} ($a: {field_966.get_subfields('a')})"
                        )
                    else:
                        logging.info(
                            f"Not removing 966 bookplate from MMS ID {mms_id}, "
                            f"Holding ID {holding_id} ($a: {field_966.get_subfields('a')})",
                        )
                for field_856 in pymarc_856_fields:
                    if needs_856_removed(field_856):
                        pymarc_record.remove_field(field_856)
                        logging.info(
                            f"Removing 856 bookplate from MMS ID {mms_id}, "
                            f"Holding ID {holding_id} ($z: {field_856.get_subfields('z')})"
                        )
                    else:
                        logging.info(
                            f"Not removing 856 bookplate from MMS ID {mms_id}, "
                            f"Holding ID {holding_id} ($z: {field_856.get_subfields('z')})",
                        )

            # check if any changes were made,
            # using bytes comparison to avoid object identity issues
            updated_bytes = pymarc_record.as_marc()
            if updated_bytes == initial_bytes:
                logging.info(
                    f"No changes made to MMS ID {mms_id}, Holding ID {holding_id}"
                )
                skipped_holdings_count += 1
            else:
                # add pymarc updates to holding record
                alma_holding_record.marc_record = pymarc_record
                # update holding record
                try:
                    retry_call(
                        client.update_holding_record,
                        fargs=(mms_id, alma_holding_record),
                        tries=3,
                        delay=20,
                        backoff=2,
                    )
                # deal with possible ConnectTimeout error
                except ConnectTimeout as e:
                    logging.error(
                        f"Error updating MMS ID {mms_id}, Holding ID {holding_id}: {e}"
                    )
                    errored_holdings_count += 1
                    errored_holdings.append(
                        {"MMS Id": mms_id, "Holding Id": holding_id}
                    )
                else:
                    logging.info(f"Updated MMS ID {mms_id}, Holding ID {holding_id}")
                    updated_holdings_count += 1
    logging.info("Finished Bookplate Updates")
    logging.info(f"Total Holdings Updated: {updated_holdings_count}")
    logging.info(f"Total Holdings Skipped: {skipped_holdings_count}")
    logging.info(f"Total Holdings Errored: {errored_holdings_count}")
    if errored_holdings:
        # write errored holdings to file
        output_filename = (
            f"errored_holdings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        output_path = Path("logs", output_filename)  # write to logs/ dir
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"Errored holdings written to `{output_path}`")
        _write_to_file(errored_holdings, output_path)


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

    if args.report_file:
        logging.info(f"Using local report data from {args.report_file}")
        report_data = _load_report_data_from_file(args.report_file)
    else:
        logging.info("Getting bookplate report data from Alma Analytics")
        report_data = get_bookplates_report(analytics_api_key)

    client = AlmaAPIClient(alma_api_key)

    # bookplates to leave in 966 field (FTVA SPACs)
    bookplates_to_leave_966 = [
        "AFC",
        "AFI",
        "AHA",
        "AM",
        "AMA",
        "AMAS",
        "AMI",
        "AMP",
        "BBA",
        "CFS",
        "CSC",
        "DEN",
        "DGA",
        "DLX",
        "ERO",
        "FNF",
        "GKC",
        "HEA",
        "HHF",
        "HLC",
        "HRC",
        "ICC",
        "IWF",
        "JCC",
        "JUN",
        "LAI",
        "LAR",
        "MCC",
        "MIC",
        "MP",
        "MPC",
        "MTC",
        "OUT",
        "PHI",
        "PPB",
        "PPI",
        "QRC",
        "RA",
        "RAS",
        "RHE",
        "SOD",
        "STC",
        "SUN",
        "TV",
        "UTV",
        "WBA",
        "WEL",
        "WIF",
    ]

    remove_bookplates(
        report_data, client, bookplates_to_leave_966, args.start_index, args.limit
    )


if __name__ == "__main__":
    main()
