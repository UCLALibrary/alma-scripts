import argparse
import csv
from alma_api_client import AlmaAPIClient
from alma_api_keys import API_KEYS
from pprint import pprint


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "fau_mapping_file", help="Path to the FAU -> COA mapping TSV file"
    )
    parser.add_argument(
        "--environment",
        choices=["SANDBOX", "PRODUCTION"],
        default="SANDBOX",
        help="Alma environment",
    )
    parser.add_argument(
        "--dry_run",
        help="Dry run: do not update Alma",
        action="store_true",
    )
    args = parser.parse_args()

    fau_mapping_file = args.fau_mapping_file
    with open(fau_mapping_file, newline="") as f:
        funds_to_process = []
        fund_reader = csv.DictReader(f, delimiter="\t")
        for line in fund_reader:
            # Remove leading / trailing spaces from messy input data
            line = {k: v.strip() for k, v in line.items()}
            funds_to_process.append(line)

    if args.environment == "PRODUCTION":
        api_key = API_KEYS["DIIT_SCRIPTS"]
    else:
        api_key = API_KEYS["SANDBOX"]
    client = AlmaAPIClient(api_key)

    for fund in funds_to_process:
        fund_id = fund["Fund Id"]
        fund_code = fund["Fund Code"]
        fund_name = fund["Fund Name"]
        fund_fau = fund["Fund External Id (Current)"]
        fund_coa = fund["Fund External Id (New)"]

        # 2024-04-23: We have fund ids in our TSV file now, for precise retrieval.
        alma_fund = get_alma_fund_by_id(client, fund_id)
        # we care about: name, code, external_id; maybe show id and fiscal_period["desc"] in logs
        if alma_fund:
            update_message = (
                f"Updating {fund_code:15} / {fund_name:50}: "
                f"changing {fund_fau} to {fund_coa}"
            )
            if args.dry_run:
                print(f"DRY RUN: {update_message}")
            else:
                print(update_message)
                # Remove api_response we embedded on retrieval
                del alma_fund["api_response"]
                # Replace FAU with COA in Alma fund external_id
                alma_fund["external_id"] = fund_coa
                updated_fund = client.update_fund(fund_id, alma_fund)
                if updated_fund["api_response"]["status_code"] != 200:
                    print("PROBLEM updating fund?")
                    pprint(updated_fund, width=132)
        else:
            print(f"ERROR: No Alma active fund found for {fund_code} / {fund_name}")


def get_alma_fund_by_id(client: AlmaAPIClient, fund_id: str) -> dict:
    alma_fund = client.get_fund(fund_id)
    if alma_fund.get("errorsExist"):
        errors = alma_fund.get("errorList").get("error")
        for error in errors:
            error_code = error.get("errorCode")
            error_message = error.get("errorMessage")
            print(f"{error_code} : {error_message}")
        return None
    else:
        return alma_fund


# def get_alma_fund_by_code(client: AlmaAPIClient, fund_code: str) -> dict:
#     # Need an Alma fund_id to retrieve single fund via API; get that eventually in TSV file?
#     # For now, search active funds by code and retrieve the (assumed) 1 match.
#     parameters = {
#         "q": f"fund_code~{fund_code}",
#         "status": "ACTIVE",
#         "mode": "ALL",
#     }
#     alma_fund = client.get_funds(parameters)
#     # For now, if no match (or multiples), return None
#     if alma_fund["total_record_count"] == 1:
#         return alma_fund.get("fund")[0]
#     else:
#         return None


if __name__ == "__main__":
    main()
