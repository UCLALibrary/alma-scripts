from csv import DictWriter
from alma_api_keys import API_KEYS
from alma_api_client import AlmaAPIClient


def get_active_vendor_codes(alma_client: type[AlmaAPIClient]) -> list:
    vendor_codes: list = []
    vendor_total: int = -1
    # Max limit (records per batch) is 100
    limit: int = 100
    offset: int = 0
    parameters: dict = {}
    # For control during testing; 9999999 to disable.
    # Otherwise, make this a multiple of limit.
    testing_total: int = 9_999_999

    # Need to do at least once, to get total number of records.
    # No do...while in Python :(
    while True:
        # Get current batch of data (limit# records,
        # starting at 0-based offset#).
        print(f"Fetching up to #{limit + offset}")
        parameters = {"status": "active", "limit": limit, "offset": offset}
        vendor_data = alma_client.get_vendors(parameters)

        vendor_total = vendor_data["total_record_count"]
        new_codes = [vendor["code"] for vendor in vendor_data["vendor"]]
        vendor_codes.extend(new_codes)
        offset += limit
        if offset >= vendor_total or offset >= testing_total:
            break

    print(f"{vendor_total = }")
    print(f"{len(vendor_codes)} records checked")

    return vendor_codes


def get_contact_name(vendor: dict) -> str:
    # Returns the first contact found, if any.
    # API does not include "primary" designator found via
    # "Contact People" tab on Alma vendor record.
    contact_name: str = ""
    if vendor.get("contact_person"):
        first_name = vendor["contact_person"][0]["first_name"]
        last_name = vendor["contact_person"][0]["last_name"]
        contact_name = f"{first_name} {last_name}"
    return contact_name


def get_email_address(vendor: dict) -> str:
    # Returns the preferred email, if any.
    email_address: str = ""
    if vendor.get("contact_info"):
        emails = vendor["contact_info"]["email"]
        for email in emails:
            if email["preferred"]:
                email_address = email["email_address"]
            break  # don't bother looking for more
    return email_address


def get_vendor_data(alma_client: type[AlmaAPIClient], vendor_codes: list) -> list:
    # Returns a list of dictionaries, one for each vendor code
    vendor_data: list = []
    for vendor_code in vendor_codes:
        vendor = alma_client.get_vendor(vendor_code)
        # Only get data for those with finance code (VCK)
        if vendor.get("financial_sys_code"):
            print(f"Getting data for {vendor_code = }")
            # Keys reflect column headings wanted
            contact_name = get_contact_name(vendor)
            email_address = get_email_address(vendor)
            vd = {
                "vendor_code": vendor_code,
                # "raw_contact": vendor.get("contact_person"),
                "Vendor Name": vendor["name"],
                "Contact Name": contact_name,
                "Contact Email Address": email_address,
                "VCK": vendor["financial_sys_code"],
            }
            vendor_data.append(vd)
        else:
            print(f"Skipping {vendor_code = }: no VCK")
    return vendor_data


def main() -> None:
    alma_client = AlmaAPIClient(API_KEYS["DIIT_SCRIPTS"])
    vendor_codes = get_active_vendor_codes(alma_client)
    vendor_data = get_vendor_data(alma_client, vendor_codes)
    column_headers = vendor_data[0].keys()
    with open("vendor_data.csv", "wt") as fh:
        writer = DictWriter(fh, column_headers, dialect="excel")
        writer.writeheader()
        writer.writerows(vendor_data)


if __name__ == "__main__":
    main()
