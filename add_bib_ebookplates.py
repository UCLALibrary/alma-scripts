import csv
import copy
import argparse
import logging
from alma_api_keys import API_KEYS
from alma_api_client import AlmaAPIClient
from alma_analytics_client import AlmaAnalyticsClient
from alma_marc import get_pymarc_record_from_bib, prepare_bib_for_update
from pymarc import Field, Record, Subfield

logging.basicConfig(filename="add_bib_ebookplates.log", level=logging.DEBUG)


def get_fund_code_report(analytics_api_key: str) -> list:
    """Get the report of MMS IDs and fund codes from Alma Analytics."""
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Acquisitions/Reports/API/MMS ID by SPAC"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def get_report_ebookplates(report: list, input_file: str) -> list:
    """Add SPAC ebookplate info to each item in the report."""
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

    # sanity check - same number of items before and after SPAC mapping?
    if len(report) != len(new_report):
        quit(
            """Mapping length mismatch. The mapping file may contain duplicate fund
            codes, or may be missing fund codes. Please check inputs."""
        )
    return new_report


def is_new_966(old_record: Record, spac_code: str, spac_name: str) -> bool:
    """Check all 966 fields in a record to see if a new 966 field is needed."""
    for field_966 in old_record.get_fields("966"):
        # match only subfield a
        if spac_code in field_966.get_subfields("a"):
            return False
    return True


def needs_bookplate_update(
    old_field: Field, spac_code: str, spac_name: str, spac_url: str
) -> bool:
    """Check if a 966 field matches the SPAC code, but needs an update to URL or name."""
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


def add_new_966(record: Record, spac_code: str, spac_name: str, spac_url: str) -> None:
    """Add a new 966 field to a pymarc record, with SPAC and bookplate data."""
    subfields = []
    subfields.append(Subfield(code="a", value=spac_code))
    subfields.append(Subfield(code="b", value=spac_name))
    subfields.append(Subfield(code="9", value="LOCAL"))
    if spac_url:
        subfields.append(Subfield(code="c", value=spac_url))
    record.add_field(
        Field(
            tag="966",
            indicators=[" ", " "],
            subfields=subfields,
        )
    )


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
    args = parser.parse_args()

    if args.environment == "debug":
        # test data for sandbox environment
        # these MMS IDs are real, but fund codes are fake to align with test SPAC mappings file
        report_data = [
            # case 1: SPAC1, with URL
            {"MMS Id": "9911656853606533", "Fund Code": "FUND2A"},
            # case 2: SPAC3, no URL
            {"MMS Id": "9990572683606533", "Fund Code": "FUND3"},
        ]
        alma_api_key = API_KEYS["SANDBOX"]

    elif args.environment == "sandbox":
        # use production analytics key for sandbox environment, since sandbox doesn't have analytics
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["SANDBOX"]
        report_data = get_fund_code_report(analytics_api_key)

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["DIIT_SCRIPTS"]
        report_data = get_fund_code_report(analytics_api_key)

    print(f"Beginning processing {len(report_data)} bib e-bookplates")
    print()

    report_with_ebookplates = get_report_ebookplates(
        report_data, args.spac_mappings_file
    )

    client = AlmaAPIClient(alma_api_key)

    # initialize counters
    total_bibs_updated = 0
    total_bibs_skipped = 0
    total_bibs_errored = 0

    for item in report_with_ebookplates:
        mms_id = item["MMS Id"]
        spac_code = item["spac_code"]
        spac_name = item["spac_name"]
        spac_url = item["spac_url"]
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

        # convert to Pymarc to handle fields and subfields
        pymarc_record = get_pymarc_record_from_bib(alma_bib)

        if is_new_966(pymarc_record, spac_code, spac_name):
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
            new_alma_bib = prepare_bib_for_update(alma_bib, pymarc_record)
            client.update_bib(mms_id, new_alma_bib)
            total_bibs_updated += 1
        else:
            total_bibs_skipped += 1
            logging.debug(f"Skipping MMS ID {mms_id}. No 966 updates needed.")

        # every 5% of records, log progress
        total_bibs_processed = (
            total_bibs_updated + total_bibs_skipped + total_bibs_errored
        )
        if total_bibs_processed % (len(report_with_ebookplates) / 20) == 0:
            logging.info(f"Processed {total_bibs_processed} bibs.")

    print()
    print(
        "Finished adding ebookplates. ",
        f"{total_bibs_updated} bibs updated. ",
        f"{total_bibs_skipped} bibs skipped with no 966 updates needed. ",
        f"{total_bibs_errored} bibs skipped due to errors.",
    )


if __name__ == "__main__":
    main()
