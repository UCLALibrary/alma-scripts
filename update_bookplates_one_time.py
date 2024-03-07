import csv
import argparse
import logging
from alma_api_keys import API_KEYS
from alma_api_client import AlmaAPIClient
from alma_analytics_client import AlmaAnalyticsClient
from alma_marc import get_pymarc_record_from_bib, prepare_bib_for_update
from pymarc import Field

logging.basicConfig(filename="update_bookplates_one_time.log", level=logging.DEBUG)


def get_mms_report(analytics_api_key: str) -> list:
    """Get the report of MMS IDs and current 966 contents from Alma Analytics."""
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
    """Get SPAC mappings from a CSV file. Filter out any lines without a valid URL."""
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
    """Check if a 966 field matches a SPAC code. If so, assume it needs updating."""
    # check if the SPAC code in the 966 field is in the list of SPAC mappings
    for spac_mapping in spac_mappings:
        if spac_mapping["SPAC"] == old_field.get_subfields("a")[0]:
            return True
    return False


def get_spac_info(spac_mappings: list, field_966: Field) -> dict:
    """Get the SPAC name and URL from the mappings to update a 966."""
    for spac_mapping in spac_mappings:
        if spac_mapping["SPAC"] == field_966.get_subfields("a")[0]:
            return {"spac_name": spac_mapping["NAME"], "spac_url": spac_mapping["URL"]}
    return {"spac_name": "", "spac_url": ""}


def update_existing_966(field_966: Field, spac_name: str, spac_url: str) -> None:
    """Update the URL and bookplate text in an existing 966 field."""
    # update $b for bookplate text
    field_966.delete_subfield("b")
    field_966.add_subfield("b", spac_name)
    # update $c for URL
    field_966.delete_subfield("c")
    # if spac_url is an empty string, don't add $c back in
    if spac_url:
        field_966.add_subfield("c", spac_url)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "spac_mappings_file", help="Path to the SPAC mappings .csv file"
    )
    parser.add_argument(
        "environment",
        help="Alma environment (sandbox or production), or 'debug' for a small test set.",
    )
    parser.add_argument(
        "--start-index", type=int, help="Start processing report data at this index"
    )
    args = parser.parse_args()

    if args.environment == "debug":
        # test data for sandbox environment
        report = [
            {"MMS Id": "9911656853606533"},
            {"MMS Id": "9990572683606533"},
        ]
        alma_api_key = API_KEYS["SANDBOX"]

    elif args.environment == "sandbox":
        # use production analytics key for sandbox environment, since sandbox doesn't have analytics
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["SANDBOX"]
        report = get_mms_report(analytics_api_key)

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["DIIT_SCRIPTS"]
        report = get_mms_report(analytics_api_key)

    # if a start index is provided, slice the report to start at that index
    if args.start_index:
        report = report[args.start_index :]

    print(f"Beginning processing {len(report)} bib e-bookplates")
    print()

    client = AlmaAPIClient(alma_api_key)

    spac_mappings = get_spac_mappings(args.spac_mappings_file)

    # initialize counters
    total_bibs_updated = 0
    total_bibs_skipped = 0
    total_bibs_errored = 0
    report_index = 0

    for item in report:

        mms_id = item["MMS Id"]
        bib_was_updated = False

        # get bib from Alma
        alma_bib = client.get_bib(mms_id).get("content")
        # check for error in bib response, usually due to invalid MMS ID
        if b"errorsExist" in alma_bib:
            logging.info(
                f"Got an error finding bib record for MMS ID {mms_id}. Skipping this record."
            )
            total_bibs_errored += 1
            continue
        # if we get a bad response, halt the script
        # report is sorted by MMS ID, so we can use this to resume later if needed
        if not alma_bib:
            logging.error(
                f"Unexpected response for MMS ID {mms_id}, index {report_index}. Exiting."
            )
            exit()

        # convert to Pymarc to handle fields and subfields
        pymarc_record = get_pymarc_record_from_bib(alma_bib)
        for field_966 in pymarc_record.get_fields("966"):
            if needs_bookplate_update(field_966, spac_mappings):
                # get the SPAC name and URL from the mappings
                spac_info = get_spac_info(spac_mappings, field_966)
                spac_name = spac_info["spac_name"]
                spac_url = spac_info["spac_url"]
                update_existing_966(field_966, spac_name, spac_url)
                logging.debug(
                    f"Updated bookplate. MMS ID: {mms_id}, SPAC Name: {spac_name}",
                )
                bib_was_updated = True

        if bib_was_updated:
            new_alma_bib = prepare_bib_for_update(alma_bib, pymarc_record)
            client.update_bib(mms_id, new_alma_bib)
            total_bibs_updated += 1
        else:
            # this case shouldn't happen, since report is limited to records that need updating
            # log it in case it does
            total_bibs_skipped += 1
            logging.info(f"Skipping MMS ID {mms_id}. No 966 updates needed.")

        # every 5% of records, log progress
        if report_index % (len(report) / 20) == 0:
            logging.info(f"Processed {report_index} bibs.")

        report_index += 1

    print()
    print(
        "Finished adding ebookplates. ",
        f"{total_bibs_updated} bibs updated. ",
        f"{total_bibs_skipped} bibs skipped with no 966 updates needed. ",
        f"{total_bibs_errored} bibs skipped due to errors.",
    )


if __name__ == "__main__":
    main()
