import csv
import copy
import argparse
from alma_api_keys import API_KEYS
from alma_api_client import AlmaAPIClient
from alma_analytics_client import AlmaAnalyticsClient
from alma_marc import get_pymarc_record_from_bib, prepare_bib_for_update
from pymarc import Field, Record


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
            if line["Alma Fund Code"] == item["Fund Ledger Code"]:
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


def insert_ebookplates(alma_api_key: str, report: list) -> None:
    # using Sandbox key only for now
    client = AlmaAPIClient(alma_api_key)
    for item in report:
        mms_id = item["MMS Id"]
        spac_name = item["spac_name"]
        # spac_url will be an empty string for some SPACs - this is ok!
        spac_url = item["spac_url"]
        # placeholder text for now - this will eventually vary with each SPAC
        spac_image = "fake image url"

        # get bib from Alma
        alma_bib = client.get_bib(mms_id).get("content")
        # use pymarc to add new 967 field
        pymarc_record = get_pymarc_record_from_bib(alma_bib)

        # first check for existing 967, matching on $a
        if is_not_duplicate_967(pymarc_record, spac_name):
            # if we have a spac_url, add it in $b - otherwise only use $a and $c
            if spac_url:
                pymarc_record.add_field(
                    Field(
                        tag="967",
                        # alma_marc.prepare_bib_for_update needs indicators explicitly set
                        indicators=[" ", " "],
                        subfields=["a", spac_name, "b", spac_url, "c", spac_image],
                    )
                )
            else:
                pymarc_record.add_field(
                    Field(
                        tag="967",
                        # alma_marc.prepare_bib_for_update needs indicators explicitly set
                        indicators=[" ", " "],
                        subfields=["a", spac_name, "c", spac_image],
                    )
                )
            # repackage Alma bib and send update
            new_alma_bib = prepare_bib_for_update(alma_bib, pymarc_record)
            client.update_bib(mms_id, new_alma_bib)


def is_not_duplicate_967(old_record: Record, spac_name: str) -> bool:
    for field_967 in old_record.get_fields("967"):
        if spac_name in field_967.get_subfields("a"):
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
            {
                "MMS Id": "9996854839106533",
                "Fund Ledger Code": "4IS001",
                "Transaction Date": "2022-04-15T00:00:00",
                "Transaction Item Type": "EXPENDITURE",
                "Invoice-Number": "9300014049",
            }
        ]
        alma_api_key = API_KEYS["SANDBOX"]
        # next 3 lines can move out of conditional once API keys are finalized
        report_with_ebookplates = get_ebookplates(report_data, args.spac_mappings_file)
        insert_ebookplates(alma_api_key, report_with_ebookplates)
        print(f"Processed {len(report_data)} bib e-bookplates")

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        report_data = get_fund_code_report(analytics_api_key)


if __name__ == "__main__":
    main()
