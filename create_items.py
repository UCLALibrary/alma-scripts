#!/usr/bin/env -S python3 -u
import csv
import sys
import pprint as pp

from alma_api_keys import API_KEYS
from alma_api_client import Alma_Api_Client


def _has_items(bib_id, holding_id):
    # Check to see if the given bib/holding record has items.
    # Fetch just the first one (if any); data includes total number.
    params = {"limit": 1}
    items = alma.get_items(bib_id, holding_id, params)
    if items["total_record_count"] > 0:
        return True
    else:
        return False


def _format_barcode(barcode_prefix, barcode_number):
    # Left-pad numerical part with zeroes to 7 digits
    return barcode_prefix + str(barcode_number).rjust(7, "0")


def _format_item(item_data):
    barcode = _format_barcode(item_data["barcode_prefix"], item_data["barcode_number"])
    return {
        "item_data": {
            "barcode": barcode,
            "physical_material_type": {"value": item_data["material_type"]},
            "policy": {"value": item_data["circ_policy"]},
        }
    }


if __name__ == "__main__":
    alma = Alma_Api_Client(API_KEYS["DIIT_SCRIPTS"])
    # Values for creating items
    # TODO: Pass to program via files
    # CLARK0099066 is the last assigned barcode, via ALMA-42
    # LSC0229576 is last assigned, via ALMA-48
    # BUNCHE0007205 is last assigned, via ALMA-56
    # CSRC0014018 is last assigned, via ALMA-71
    # AISC0010319 is last assigned, via ALMA-80
    # FTVA0029273 is last assigned, via ALMA-88
    tsv_file = sys.argv[1]
    first_barcode = sys.argv[2]
    item_template = {
        "barcode_prefix": first_barcode[:-7],
        "barcode_number": int(first_barcode[-7:]),
        ##### CHANGE OR PARAMETERIZE #####
        "material_type": "RECORD",
        "circ_policy": "viewp",
    }

    # Read bib/holding ids from file
    with open(tsv_file) as tsv:
        reader = csv.DictReader(tsv, delimiter="\t")
        for row in reader:
            record_ids = dict(row)
            bib_id = record_ids["BIB_ID"]
            holding_id = record_ids["HOLDING_ID"]
            if _has_items(bib_id, holding_id):
                print(f"Holdings record {holding_id} has items - skipping")
            else:
                item_data = _format_item(item_template)
                # item_data['item_data']['barcode'] = 'TESTING'
                item = alma.create_item(bib_id, holding_id, item_data)
                api_status = item["api_response"]["status_code"]
                if api_status == 200:
                    barcode = item_data["item_data"]["barcode"]
                    print(f"Created item {barcode} on holdings {holding_id}")
                    item_template["barcode_number"] += 1
                else:
                    print(f"ERROR: Unable to add item to holdings {holding_id}")
                    pp.pprint(item)
