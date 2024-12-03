import argparse
import logging
from alma_api_keys import API_KEYS
from alma_api_client import (
    AlmaAPIClient,
    get_pymarc_record_from_bib,
    prepare_bib_for_update,
)
from alma_analytics_client import AlmaAnalyticsClient
from pymarc import Field


def get_856_report(analytics_api_key: str) -> list:
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Cataloging/Reports/API/856 Bookplates to Remove"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def get_966_report(analytics_api_key: str) -> list:
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Cataloging/Reports/API/966 Bookplates to Remove"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="Alma environment (sandbox or production)")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    args = parser.parse_args()

    logging.basicConfig(filename="remove_bookplates_one_time.log", level=args.log_level)
    # always suppress urllib3 logs with lower level than WARNING
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if args.environment == "sandbox":
        alma_api_key = API_KEYS["SANDBOX"]
        # analytics only available in prod environment
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["DIIT_SCRIPTS"]

    logging.info("Getting 966 report data")
    report_data_966 = get_966_report(analytics_api_key)
    logging.info("Getting 856 report data")
    report_data_856 = get_856_report(analytics_api_key)

    client = AlmaAPIClient(alma_api_key)

    bookplates_to_leave_966 = [
        "AFC",
        "AFI",
        "AHA",
        "AMA",
        "AM",
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
        "MPC",
        "MTC",
        "OUT",
        "PHI",
        "PPB",
        "PPI",
        "RAS",
        "SOD",
        "STC",
        "SUN",
        "UTV",
        "WBA",
        "WEL",
        "WIF",
    ]

    logging.info(f"Processing {len(report_data_966)} 966 bookplates")
    errored_holdings_count = 0
    updated_holdings_count = 0
    skipped_holdings_count = 0

    for index, item in enumerate(report_data_966):
        mms_id = item["MMS Id"]
        holding_id = item["Holding Id"]

        alma_holding = client.get_holding(mms_id, holding_id).get("content")
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

        else:
            # convert to Pymarc to handle fields and subfields
            pymarc_record = get_pymarc_record_from_bib(alma_holding)
            pymarc_966s = pymarc_record.get_fields("966")
            if not pymarc_966s:
                logging.info(
                    f"No 966 found for MMS ID {mms_id}, Holding ID {holding_id}"
                )
                skipped_holdings_count += 1
            else:
                for field_966 in pymarc_966s:
                    if needs_966_removed(field_966, bookplates_to_leave_966):
                        pymarc_record.remove_field(field_966)
                        logging.info(
                            f"Removing 966 bookplate from MMS ID {mms_id}, Holding ID {holding_id}"
                        )
                    else:
                        logging.info(
                            f"Not removing 966 bookplate from MMS ID {mms_id}, ",
                            f"Holding ID {holding_id}",
                        )
                # convert back to Alma Holding and send update
                new_alma_holding = prepare_bib_for_update(alma_holding, pymarc_record)
                client.update_holding(mms_id, holding_id, new_alma_holding)
                updated_holdings_count += 1
    logging.info("966 Bookplates Updated")
    logging.info(f"Total Holdings Updated: {updated_holdings_count}")
    logging.info(f"Total Holdings Skipped: {skipped_holdings_count}")
    logging.info(f"Total Holdings Errored: {errored_holdings_count}")

    logging.info(f"Processing {len(report_data_856)} 856 bookplates")
    errored_holdings_count = 0
    updated_holdings_count = 0
    skipped_holdings_count = 0
    for index, item in enumerate(report_data_856):
        mms_id = item["MMS Id"]
        holding_id = item["Holding Id"]

        alma_holding = client.get_holding(mms_id, holding_id).get("content")
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
        else:
            # convert to Pymarc to handle fields and subfields
            pymarc_record = get_pymarc_record_from_bib(alma_holding)
            pymarc_856s = pymarc_record.get_fields("856")
            if not pymarc_856s:
                logging.info(
                    f"No 856 found for MMS ID {mms_id}, Holding ID {holding_id}"
                )
                skipped_holdings_count += 1
            else:
                for field_856 in pymarc_856s:
                    if needs_856_removed(field_856):
                        pymarc_record.remove_field(field_856)
                        logging.info(
                            f"Removing 856 bookplate from MMS ID {mms_id}, Holding ID {holding_id}"
                        )
                    else:
                        logging.info(
                            f"Not removing 856 bookplate from MMS ID {mms_id}, ",
                            f"Holding ID {holding_id}",
                        )
                # convert back to Alma Holding and send update
                new_alma_holding = prepare_bib_for_update(alma_holding, pymarc_record)
                client.update_holding(mms_id, holding_id, new_alma_holding)
                updated_holdings_count += 1
    logging.info("856 Bookplates Updated")
    logging.info(f"Total Holdings Updated: {updated_holdings_count}")
    logging.info(f"Total Holdings Skipped: {skipped_holdings_count}")
    logging.info(f"Total Holdings Errored: {errored_holdings_count}")
    logging.info("Script Complete")


def needs_966_removed(field_966: Field, bookplates_to_leave: list) -> bool:
    # only remove 966s that don't contain any of the SPACs in bookplates_to_leave
    for term in bookplates_to_leave:
        if term in field_966.format_field():
            return False
    return True


def needs_856_removed(field_856: Field) -> bool:
    # only remove 856s that contain "Bookplate"
    if "Bookplate" in field_856.format_field():
        return True
    return False


if __name__ == "__main__":
    main()
