import argparse
from alma_api_keys import API_KEYS
from alma_api_client import AlmaAPIClient
from alma_analytics_client import AlmaAnalyticsClient
from alma_marc import get_pymarc_record_from_bib, prepare_bib_for_update


def get_holdings_report(analytics_api_key: str) -> list:
    # analytics only available in prod environment
    aac = AlmaAnalyticsClient(analytics_api_key)
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Collections/Reports/CAAS Holdings for Display Note"
    )
    aac.set_report_path(report_path)
    report = aac.get_report()
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("environment", help="Alma environment (sandbox or production)")
    args = parser.parse_args()

    if args.environment == "sandbox":
        # test data for sandbox environment
        report_data = [
            {"MMS Id": "9924282823606533", "Holding Id": "22534807520006533"}
        ]
        alma_api_key = API_KEYS["SANDBOX"]

    elif args.environment == "production":
        analytics_api_key = API_KEYS["DIIT_ANALYTICS"]
        alma_api_key = API_KEYS["DIIT_SCRIPTS"]
        report_data = get_holdings_report(analytics_api_key)

    client = AlmaAPIClient(alma_api_key)
    print(f"Found {len(report_data)} holdings to update.")
    errored_holdings_count = 0
    updated_holdings_count = 0

    for item in report_data:
        mms_id = item["MMS Id"]
        holding_id = item["Holding Id"]

        alma_holding = client.get_holding(mms_id, holding_id).get("content")
        # make sure we got a valid bib
        if b"is not valid" in alma_holding:
            print(
                f"Error finding MMS ID {mms_id}, Holding ID {holding_id}. Skipping this record."
            )
            errored_holdings_count += 1
        else:
            # convert to Pymarc to handle fields and subfields
            pymarc_record = get_pymarc_record_from_bib(alma_holding)
            pymarc_852 = pymarc_record.get_fields("852")[0]
            pymarc_852.add_subfield(code="z", value="Reading Room Use ONLY.")
            # convert back to Alma Holding and send update
            new_alma_holding = prepare_bib_for_update(alma_holding, pymarc_record)
            client.update_holding(mms_id, holding_id, new_alma_holding)
            updated_holdings_count += 1
    print(f"Finished updating {updated_holdings_count} holdings.")
    print(f"Encountered {errored_holdings_count} errors.")


if __name__ == "__main__":
    main()
