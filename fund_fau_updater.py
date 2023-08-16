import csv
import sys
from alma_api_client import AlmaAPIClient
from alma_api_keys import API_KEYS
from pprint import pprint

# This is WIP, not ready for production use.
# Updating Alma to use COA instead of FAU will happen in 2024? 2025?
# This is proof of concept only, for now.


def main() -> None:
    tsv_file = sys.argv[1]
    with open(tsv_file, newline="") as f:
        funds_to_process = []
        fund_reader = csv.DictReader(f, delimiter="\t")
        for line in fund_reader:
            funds_to_process.append(line)

    client = AlmaAPIClient(API_KEYS["SANDBOX"])
    for fund in funds_to_process:
        # Fund Name	Fund Code	Fund External Id (Current)
        # Fund Name (CoA)	Fund Code (CoA)	Fund External Id (New)
        fund_code = fund["Fund Code"]
        fund_name = fund["Fund Name"]
        fund_fau = fund["Fund External Id (Current)"]
        fund_coa = fund["Fund External Id (New)"]

        alma_fund = get_alma_fund_by_code(client, fund_code)
        # we care about: name, code, external_id; maybe show id and fiscal_period["desc"] in logs
        if alma_fund:
            print(
                f"Updating {fund_code} / {fund_name}: changing {fund_fau} to {fund_coa}"
            )
            fund_id = alma_fund["id"]
            # Replace FAU with COA in Alma fund external_id
            alma_fund["external_id"] = fund_coa
            updated_fund = client.update_fund(fund_id, alma_fund)
            if updated_fund["api_response"]["status_code"] != 200:
                print("PROBLEM updating fund?")
                pprint(updated_fund, width=132)
        else:
            print(f"ERROR: No Alma active fund found for {fund_code} / {fund_name}")


def get_alma_fund_by_code(client: AlmaAPIClient, fund_code: str) -> dict:
    # Need an Alma fund_id to retrieve single fund via API; get that eventually in TSV file?
    # For now, search active funds by code and retrieve the (assumed) 1 match.
    parameters = {
        "q": f"fund_code~{fund_code}",
        "status": "ACTIVE",
        "mode": "ALL",
    }
    alma_fund = client.get_funds(parameters)
    # For now, if no match (or multiples), return None
    if alma_fund["total_record_count"] == 1:
        return alma_fund.get("fund")[0]
    else:
        return None


if __name__ == "__main__":
    main()

    # # Get up to 100 records per request, until all records have been retrieved.
    # records_per_request = 100
    # offset = 0
    # parameters = {
    #     "mode": "ALL",
    #     "status": "ACTIVE",
    #     "limit": records_per_request,
    #     "offset": offset,
    # }
    # # First batch
    # data = client.get_funds(parameters)
    # total_records = data["total_record_count"]
    # # Fund data is in a list called "fund"
    # records: list = data["fund"]
    # while len(records) < total_records:
    #     print(f"{total_records=}, {len(records)}")
    #     offset += records_per_request
    #     parameters = {
    #         "mode": "ALL",
    #         "status": "ACTIVE",
    #         "limit": records_per_request,
    #         "offset": offset,
    #     }
    #     data = client.get_funds(parameters)
    #     records.extend(data["fund"])
    # print(f"{total_records=}, {len(records)}")

    # pprint(records)
