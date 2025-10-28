#!/usr/bin/env -S python3 -u
import argparse
import traceback
from datetime import datetime
from pymarc import Record, MARCReader, MARCWriter


def is_microform_location(location_code: str) -> bool:
    """Determine whether location is used for microforms."""
    return location_code in [
        "armi",
        "armiflm",
        "bimi",
        "birfmi",
        "ckmi",
        "csmi",
        "lwmi",
        "lsbihimi",
        "mgmi",
        "mgmigk",
        "mgrffiche",
        "clmi",
        "smmi",
        "sgmi",
        "srmnm",
        "srpnm",
        "yralmi",
        "yrmi",
        "yrmiclsd",
        "yrmiguides",
        "yrrismi",
    ]


def is_suppressed_location(location_code: str) -> bool:
    """Determine whether location is suppressed from public view."""
    # Most of these are not relevant... but easiest just to use the full list.
    # No practical way to check in real time; we could make an Analytics report
    # and query that once via API, but it's not worth the effort for this project.
    return location_code in [
        "aacleanup",
        "aicleanup",
        "archrome",
        "arcleanup",
        "arcotf",
        "arill",
        "bichrome",
        "bicleanup",
        "bicotf",
        "biill",
        "biof",
        "biofadmin",
        "biofads",
        "biofpsrml",
        "boechrome",
        "cacleanup",
        "cccollege",
        "ckcleanup",
        "clcleanup",
        "clicclocke",
        "cliccpnloc",
        "cliccpsloc",
        "cliccynloc",
        "cliccysloc",
        "clill",
        "clof",
        "clofbindr",
        "clrpr",
        "cscleanup",
        "eacleanup",
        "ercleanup",
        "erof",
        "ftwd",
        "geochrome",
        "ghostsr",
        "ilcleanup",
        "in",
        "lscleanup",
        "lwcleanup",
        "lwfac",
        "lwfacanx",
        "lwfacarch",
        "lwfacper",
        "lwill",
        "lwofac",
        "lwofaccess",
        "lwofcat",
        "lwofcoll",
        "lwofrf",
        "lwofshell",
        "lwoftech",
        "lwwillarch",
        "mg",
        "mgchrome",
        "mgcleanup",
        "mgcotf",
        "mgill",
        "mgofbind",
        "micleanup",
        "muchrome",
        "mucleanup",
        "mucotf",
        "muill",
        "seof",
        "seofadmin",
        "seoflibn",
        "sgcleanup",
        "sgcotf",
        "sgof",
        "smcleanup",
        "smcotf",
        "smill",
        "srcleanup",
        "uclsr",
        "ueofrest",
        "yrchrome",
        "yrcleanup",
        "yrill",
        "yrlb",
        "yrlcotf",
        "yrllost",
        "yrnrlfdp",
        "yrof",
        "yrofasd",
        "yrofbibs",
        "yrofcat",
        "yrofrf",
        "yrofrfcd",
        "yrofrfco",
        "yrofseri",
        "yrofseribd",
        "yrpenrs",
    ]


def get_oclc_symbol(record: Record) -> str:
    """Determine OCLC symbol, based on data in MARC record."""

    # Get Alma library code from H52 $b (holdings 852, embedded in MARC record.)
    library_code = record["H52"]["b"].upper()
    # SRLF: ZAS; anything else: CLU
    if library_code == "SRLF":
        return "ZAS"
    else:
        return "CLU"


def get_output_filename(oclc_symbol: str) -> str:
    """Return filename required by WEST."""
    yyyymmdd = datetime.today().strftime("%Y%m%d")
    return f"{oclc_symbol}.alma.combined.{yyyymmdd}.mrc"


def keep_record(record: Record) -> bool:
    """Determine whether MARC record should be kept for WEST analysis."""
    # Default to keep, reject based on following tests.
    keep = True

    # For logging
    # mms_id = record["001"].data

    # Reject based on various 008 values, all of which are for
    # continuing resources only (one of the Alma extract filters).
    field_008 = record["008"].data

    if field_008:
        # Reject based on 008/23: non-print not wanted.
        form_of_item = field_008[23]
        if form_of_item not in [" ", "d", "p"]:
            keep = False

        # Reject based on 008/28: gov pubs not wanted.
        government_publication = field_008[28]
        if government_publication not in [" ", "u", "|"]:
            keep = False
    else:
        # No 008, can't evaluate the record.
        keep = False

    # Reject if there's an 074 or 086:
    # government publications not caught via 008/28.
    # As of 202311, this finds no records - still needed?
    if record.get("074") or record.get("086"):
        keep = False

    # Reject based on location code: H52 $c (holdings 852, embedded in MARC record)
    location_code = record["H52"]["c"]
    # Microform locations
    if is_microform_location(location_code):
        keep = False

    # Suppressed locations
    if is_suppressed_location(location_code):
        keep = False

    # Other oddities
    if location_code == "UNASSIGNED":
        keep = False

    return keep


def write_record(record: Record) -> None:
    """Write record to file, based on OCLC symbol derived from data in record."""
    oclc_symbol = get_oclc_symbol(record)
    output_file = get_output_filename(oclc_symbol)
    writer = MARCWriter(open(output_file, "ab"))
    writer.write(record)
    writer.close()


def write_location_file(locations: set) -> None:
    """Write location codes from all records to file."""
    with open("location_codes.txt", "w") as f:
        for location in sorted(locations):
            f.write(f"{location}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--marc_file", help="MARC bib file to process", required=True
    )
    args = parser.parse_args()

    try:
        reader = MARCReader(open(args.marc_file, "rb"))  # , utf8_handling="ignore")
        # We may need to provide list of locations used; capture in this set
        locations = set()
        keep_count = 0
        reject_count = 0
        for record in reader:
            # Filter out inappropriate records
            if keep_record(record):
                location_code = record["H52"]["c"]
                locations.add(location_code)
                write_record(record)
                keep_count += 1
            else:
                reject_count += 1
        write_location_file(locations)
        print(f"{keep_count=}, {reject_count=}")
    except Exception:
        traceback.print_exc()
    finally:
        reader.close()


if __name__ == "__main__":
    main()
