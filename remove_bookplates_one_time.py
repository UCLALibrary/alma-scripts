import argparse
import logging
from alma_api_keys import API_KEYS
from alma_api_client import (
    AlmaAPIClient,
    get_pymarc_record_from_bib,
    prepare_bib_for_update,
    AlmaAnalyticsClient,
)
from pymarc import Field
from datetime import datetime
from retry.api import retry_call
import json

# for error handling
from requests.exceptions import ConnectTimeout


def get_bookplates_report(analytics_api_key: str) -> list:
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
    # only remove 966s that don't contain any of the SPACs in bookplates_to_leave in $a
    subfields = field_966.get_subfields("a")
    for term in bookplates_to_leave:
        for subfield in subfields:
            if term in subfield:
                logging.info(f"Found {term} in 966 field")
                return False
    return True


def needs_856_removed(field_856: Field) -> bool:
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
    limit: int = None,
):
    # slice the report data to specified start index and limit
    report_data = report_data[start_index:]
    if limit:
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
                client.get_holding,
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
        alma_holding = alma_holding_record.get("content")
        # make sure we got a valid bib
        if (
            b"is not valid" in alma_holding
            or b"INTERNAL_SERVER_ERROR" in alma_holding
            or b"Search failed" in alma_holding
            or alma_holding is None
        ):
            logging.error(
                f"Error finding MMS ID {mms_id}, Holding ID {holding_id}. Skipping this record."
            )
            errored_holdings_count += 1
            errored_holdings.append({"MMS Id": mms_id, "Holding Id": holding_id})

        else:
            # convert to Pymarc to handle fields and subfields
            pymarc_record = get_pymarc_record_from_bib(alma_holding)
            pymarc_966_fields = pymarc_record.get_fields("966")
            pymarc_856_fields = pymarc_record.get_fields("856")
            if not pymarc_966_fields and not pymarc_856_fields:
                logging.info(
                    f"No 966 or 856 found for MMS ID {mms_id}, Holding ID {holding_id}"
                )
                skipped_holdings_count += 1
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

                # check if any changes were made
                if pymarc_record == get_pymarc_record_from_bib(alma_holding):
                    logging.info(
                        f"No changes made to MMS ID {mms_id}, Holding ID {holding_id}"
                    )
                    skipped_holdings_count += 1
                else:
                    # convert back to Alma Holding and send update
                    new_alma_holding = prepare_bib_for_update(
                        alma_holding, pymarc_record
                    )
                    # deal with possible ConnectTimeout error
                    try:
                        retry_call(
                            client.update_holding,
                            fargs=(mms_id, holding_id, new_alma_holding),
                            tries=3,
                            delay=20,
                            backoff=2,
                        )
                        client.update_holding(mms_id, holding_id, new_alma_holding)
                    except ConnectTimeout as e:
                        logging.error(
                            f"Error updating MMS ID {mms_id}, Holding ID {holding_id}: {e}"
                        )
                        errored_holdings_count += 1
                        errored_holdings.append(
                            {"MMS Id": mms_id, "Holding Id": holding_id}
                        )
                    else:

                        logging.info(
                            f"Updated MMS ID {mms_id}, Holding ID {holding_id}"
                        )
                        updated_holdings_count += 1
    logging.info("Finished Bookplate Updates")
    logging.info(f"Total Holdings Updated: {updated_holdings_count}")
    logging.info(f"Total Holdings Skipped: {skipped_holdings_count}")
    logging.info(f"Total Holdings Errored: {errored_holdings_count}")
    if errored_holdings:
        logging.info(f"Errored Holdings: {errored_holdings}")
        # write errored holdings to file
        output_filename = (
            f"errored_holdings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_filename, "w") as f:
            json.dump(errored_holdings, f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "environment",
        choices=["sandbox", "production"],
        help="Alma environment (sandbox or production)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    parser.add_argument(
        "--start-index",
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
        "--local-report-data-path",
        type=str,
        default=None,
        help="Path to local report data file, to use instead of fetching from analytics",
    )
    args = parser.parse_args()

    logging_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"remove_bookplates_{logging_datetime}"
    logging.basicConfig(filename=f"{base_filename}.log", level=args.log_level)
    # always suppress urllib3 logs with lower level than WARNING
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if args.environment == "sandbox":
        alma_api_key = API_KEYS["SANDBOX"]
        # analytics only available in prod environment
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["DIIT_SCRIPTS"]

    if args.local_report_data_path:
        logging.info(f"Using local report data from {args.local_report_data_path}")
        with open(args.local_report_data_path, "r") as f:
            report_data = json.load(f)

    else:
        logging.info("Getting bookplate report data")
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
