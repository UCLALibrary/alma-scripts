import csv
import copy
import argparse
from alma_api_keys import API_KEYS
from alma_api_client import AlmaAPIClient
from alma_analytics_client import AlmaAnalyticsClient
from alma_marc import get_pymarc_record_from_bib, prepare_bib_for_update
from pymarc import Field, Record, Subfield


def get_fund_code_report(analytics_api_key: str) -> list:
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Acquisitions/Reports/API/MMS ID by SPAC"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def get_ebookplates(report: list, input_file: str) -> list:
    # copy SPAC mappings into list of dicts for looping over
    spac_mappings = []
    with open(input_file, newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        for line in reader:
            spac_mappings.append(line)

    # create new list of dicts for items to avoid changing as we iterate over report
    new_report = []
    for item in report:
        for line in spac_mappings:
            if line["Alma Fund Code"] == item["Fund Code"]:
                current_item = copy.deepcopy(item)
                current_item["spac_code"] = line["SPAC_CODE"]
                current_item["spac_name"] = line["SPAC_NAME"]
                current_item["spac_url"] = line["E-bookplate link"]
                new_report.append(current_item)

    # sanity check - same number of items before and after SPAC mapping?
    if len(report) != len(new_report):
        quit(
            """Mapping length mismatch. The mapping file may contain duplicate fund
            codes, or may be missing fund codes. Please check inputs."""
        )
    return new_report


def insert_ebookplates(alma_api_key: str, report: list) -> tuple[int, int, int]:
    client = AlmaAPIClient(alma_api_key)
    # keep track of # of bibs updated for later reporting
    total_updated = 0
    total_skipped = 0
    total_errored = 0

    for item in report:
        mms_id = item["MMS Id"]
        spac_name = item["spac_name"]
        spac_code = item["spac_code"]
        # spac_url will be an empty string for non-bookplate SPACs
        spac_url = item["spac_url"]

        # get bib from Alma
        alma_bib = client.get_bib(mms_id).get("content")
        # make sure we got a valid bib
        if b"is not valid" in alma_bib:
            total_errored += 1
            print(
                f"Got an error finding bib record for MMS ID {mms_id}. Skipping this record."
            )
            continue

        # convert to Pymarc to handle fields and subfields
        pymarc_record = get_pymarc_record_from_bib(alma_bib)

        # first check for existing 966, matching all of $a, $b, and $c
        if is_not_duplicate_966(pymarc_record, spac_code):
            # TODO: determine behavior for existing 966 with different $b or $c
            subfields = []
            # always have spac code in $a and name in $b, plus $9LOCAL to avoid overwrite
            subfields.append(Subfield(code="a", value=spac_code))
            subfields.append(Subfield(code="b", value=spac_name))
            subfields.append(Subfield(code="9", value="LOCAL"))
            # add $c if it exists
            if spac_url:
                subfields.append(Subfield(code="c", value=spac_url))
            pymarc_record.add_field(
                Field(
                    tag="966",
                    # alma_marc.prepare_bib_for_update needs indicators explicitly set
                    indicators=[" ", " "],
                    subfields=subfields,
                )
            )
            # repackage Alma bib and send update
            new_alma_bib = prepare_bib_for_update(alma_bib, pymarc_record)
            client.update_bib(mms_id, new_alma_bib)
            # print(f"Added SPAC to bib. MMS ID: {mms_id}, SPAC Name: {spac_name}")
            total_updated += 1

        # print extra info if a duplicate 966 is found
        else:
            print(
                f"Skipped bib with existing 966 SPAC. MMS ID: {mms_id}, SPAC Code: {spac_code}"
            )
            total_skipped += 1

    return total_updated, total_skipped, total_errored


def is_not_duplicate_966(
    old_record: Record, spac_code: str, spac_name: str, spac_url: str
) -> bool:
    for field_966 in old_record.get_fields("966"):
        if (
            spac_code in field_966.get_subfields("a")
            and spac_name in field_966.get_subfields("b")
            and spac_url in field_966.get_subfields("c")
        ):
            return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "spac_mappings_file", help="Path to the SPAC mappings .csv file"
    )
    parser.add_argument("environment", help="Alma environment (sandbox or production)")
    args = parser.parse_args()

    if args.environment == "sandbox":
        # test data for sandbox environment
        report_data = [
            # case 1: Bob Barker fund, no URL
            {
                "MMS Id": "9911656853606533",
                "Fund Code": "2LW003",
                "Transaction Date": "2022-04-15T00:00:00",
                "Transaction Item Type": "EXPENDITURE",
                "Invoice-Number": "9300014049",
            },
            # case 2: Edgar Bowers fund, with URL
            {
                "MMS Id": "9990572683606533",
                "Fund Code": "2SC002",
                "Transaction Date": "2022-04-15T00:00:00",
                "Transaction Item Type": "EXPENDITURE",
                "Invoice-Number": "9300014049",
            },
        ]
        alma_api_key = API_KEYS["SANDBOX"]

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["DIIT_SCRIPTS"]
        report_data = get_fund_code_report(analytics_api_key)
    print(f"Beginning processing {len(report_data)} bib e-bookplates")
    print()

    report_with_ebookplates = get_ebookplates(report_data, args.spac_mappings_file)
    total_updated, total_skipped, total_errored = insert_ebookplates(
        alma_api_key, report_with_ebookplates
    )

    print()
    print(
        "Finished adding ebookplates. ",
        f"{total_updated} bibs updated. ",
        f"{total_skipped} bibs skipped due to duplicate 966s.",
        f"{total_errored} bibs skipped due to errors.",
    )


if __name__ == "__main__":
    main()
