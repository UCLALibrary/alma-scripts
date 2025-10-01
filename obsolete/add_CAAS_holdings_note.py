import argparse
import logging
import json
import csv
from pathlib import Path
from datetime import datetime
import tomllib
from alma_api_client import AlmaAPIClient, AlmaAnalyticsClient, APIError
from pymarc import Field


def _get_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    :return: Parsed arguments for program as a Namespace object.
    """
    parser = argparse.ArgumentParser(
        description="Add CAAS reading room note to Alma holdings (852 $z)."
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
        help="Start index for the report data",
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
    """Load configuration from a TOML file.

    :param config_file_name: Path to the configuration file.
    :return: Configuration dictionary.
    """
    with open(config_file_name, "rb") as f:
        config = tomllib.load(f)
    return config


def _configure_logging(log_level: str) -> None:
    """Configure application logging to a timestamped file in `logs/`.

    :param log_level: Log level to use.
    """
    name = Path(__file__).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging_file = Path("logs", f"{name}_{timestamp}.log")
    logging_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=logging_file,
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


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


def get_holdings_report(analytics_api_key: str) -> list:
    """Fetch holdings to update from Alma Analytics.

    :param analytics_api_key: Alma Analytics API key.
    :return: Report data with `MMS Id` and `Holding Id` for each row.
    """
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Collections/Reports/CAAS Holdings for Display Note"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def get_subfield_position(field: Field, subfield_code: str) -> int | None:
    """Return 0-based position of the first subfield with the given code,
    or None if not found.

    :param field: The Pymarc Field to search.
    :param subfield_code: The subfield code to search for.
    :return: The 0-based position of the first subfield with the given code,
        or None if not found.
    """
    found = False
    pos = -1

    for subfield in field:
        pos += 1
        if subfield.code == subfield_code:
            found = True
            break
    return pos if found else None


def update_holdings(client: AlmaAPIClient, report_data: list) -> None:
    """Update holdings with the CAAS reading room note.

    :param client: Alma API client.
    :param report_data: Report data with `MMS Id` and `Holding Id` for each row.
    """
    logging.info(f"Found {len(report_data)} holdings to update.")
    errored_holdings_count = 0
    updated_holdings_count = 0

    for item in report_data:
        mms_id = item["MMS Id"]
        holding_id = item["Holding Id"]

        try:
            alma_holding = client.get_holding_record(
                bib_id=mms_id, holding_id=holding_id
            )
        except APIError as e:
            logging.error(
                f"Error finding MMS ID {mms_id}, Holding ID {holding_id}: {e}"
            )
            errored_holdings_count += 1
            continue

        pymarc_record = alma_holding.marc_record
        if not pymarc_record:
            logging.error(
                f"Problem getting MARC record for MMS ID {mms_id}, Holding ID {holding_id}"
            )
            errored_holdings_count += 1
            continue

        # Should only be one 852 field in input data
        pymarc_852 = pymarc_record.get_fields("852")[0]
        public_note = "Reading Room Use ONLY."  # Add this note to the 852 $z subfield
        position = get_subfield_position(pymarc_852, "z")
        pymarc_852.add_subfield(
            "z",
            public_note,
            position,
        )

        # Post update to Alma
        alma_holding.marc_record = pymarc_record
        client.update_holding_record(bib_id=mms_id, holding_record=alma_holding)
        updated_holdings_count += 1
        logging.info(
            f"Added CAAS reading room note to MMS ID {mms_id}, Holding ID {holding_id}"
        )

    logging.info(f"Finished updating {updated_holdings_count} holdings.")
    logging.info(f"Encountered {errored_holdings_count} errors.")


def main():
    """Entry-point for the script using modern structure."""
    args = _get_arguments()
    config = _get_config(args.config_file)
    _configure_logging(args.log_level)

    # Load API key
    if args.production:
        logging.info("Using production Alma API key")
        alma_api_key = config["alma_api_keys"]["DIIT_SCRIPTS"]
    else:
        logging.info("Using sandbox Alma API key")
        alma_api_key = config["alma_api_keys"]["SANDBOX"]

    # Load report data, either from file or via Alma Analytics
    if args.report_file:
        logging.info(f"Using local report data from {args.report_file}")
        report_data = _load_report_data_from_file(args.report_file)
    else:
        logging.info("Getting bookplate report data from Alma Analytics")
        # analytics only available when using production API key
        analytics_api_key = config["alma_api_keys"]["DIIT_ANALYTICS"]
        report_data = get_holdings_report(analytics_api_key)

    # If a start index is provided, slice the report to start at that index
    if args.start_index:
        report_data = report_data[args.start_index :]
    # If a limit is provided, slice the report to that limit
    if args.limit:
        report_data = report_data[: args.limit]

    client = AlmaAPIClient(alma_api_key)

    update_holdings(client, report_data)


if __name__ == "__main__":
    main()
