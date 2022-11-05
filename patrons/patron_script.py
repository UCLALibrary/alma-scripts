import argparse
import ast
import csv
import hashlib
import json
import pprint as pp

# Long XML formatting routines for this project
import patron_xml_template as patron_xml

# experiments
from collections import Counter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--skip_hash_load",
        help="Skip loading patron hashes - use for full loads",
        action="store_true",
    )
    args = parser.parse_args()
    if args.skip_hash_load:
        previous_hashes = {}
        print("Skipping previous hashes...")
    else:
        previous_hashes = _load_hashes()

    data_files = _get_filenames()
    patrons = _get_patrons(data_files)
    print(f"Merged patrons: {len(patrons)}")

    # Compare hashes for current patrons to previous saved hashes.
    # Ignore patrons which match;
    # create new dictionary of new / updated patrons (no match).
    new_hashes = {}
    patrons_to_load = {}
    hash_match_count = 0
    for ucla_uid, patron in patrons.items():
        new_hash = _get_hash(patron)
        if new_hash == previous_hashes.get(ucla_uid):
            hash_match_count += 1
        else:
            patrons_to_load[ucla_uid] = patron
        new_hashes[ucla_uid] = new_hash

    print(f"Patrons to load: {len(patrons_to_load)}")
    print(f"Patrons not updated: {hash_match_count}")

    # Store hashes for all patrons
    _store_hashes(new_hashes)

    # Get counts of user groups, for now
    values = [patrons[uid]["USER_GROUP"] for uid in patrons_to_load]
    pp.pprint(Counter(values))
    # Finally, write file of XML for load into Alma.
    # TODO: How to handle when no patrons_to_load?
    patron_xml.write_xml(patrons_to_load)


def _get_filenames():
    # Multiple files of campus data, to be combined in various ways.
    # Names should be consistent, set by separate retrieval script.
    return {
        "bruincard_file": "bruincard_data.txt",
        "fsemail_file": "fsemail_data.txt",
        "registrar_file": "registrar_data.txt",
        "ucpath_file": "ucpath_data.txt",
    }


def _get_patrons(data_files):
    employees = _get_employees(data_files)
    students = _get_students(data_files)
    # Testing
    # for ucla_uid in employees.keys():
    # 	if ucla_uid in students:
    # 		print(f'Notice: {ucla_uid} is in both')

    # Merge students into employees, keeping student data
    # when patrons are in both groups.
    # Python 3.9 merge operator - not supported on exlsupport server 3.8...
    # patrons = employees | students
    # Python < 3.9 merge
    patrons = {**employees, **students}

    bruincard_data = _get_bruincard_data(data_files["bruincard_file"])
    # Add current barcodes for all patrons, where one exists
    for ucla_uid in patrons.keys():
        if ucla_uid in bruincard_data:
            patrons[ucla_uid]["BARCODE"] = bruincard_data.get(ucla_uid)
        else:
            # print(f'No barcode found for {ucla_uid}')
            patrons[ucla_uid]["BARCODE"] = None

    return patrons


def _get_employees(data_files):
    fsemail_data = _get_fsemail_data(data_files["fsemail_file"])
    ucpath_data = _get_ucpath_data(data_files["ucpath_file"])
    # Add email address from fsemail to ucpath patrons lacking email,
    # where possible.
    for ucla_uid, patron in ucpath_data.items():
        if patron["EMAIL_ADDRESS"] is None and ucla_uid in fsemail_data:
            # fsemail_data contains only patrons with email addresses
            ucpath_data[ucla_uid]["EMAIL_ADDRESS"] = fsemail_data[ucla_uid]
            # print(f'Notice: Added email to ucpath_data for {ucla_uid}')
    return ucpath_data


def _get_students(data_files):
    registrar_data = _get_registrar_data(data_files["registrar_file"])
    return registrar_data


def _get_fsemail_data(fsemail_file):
    fsemail_data = {}
    # Some unexpected data - 0xa0 nonbreaking spaces,
    # not utf-8 or ascii or proper utf-16.
    # Ignoring errors seems safe and correct.
    with open(fsemail_file, errors="ignore") as f:
        # TODO: Investigate why f.readlines() here includes extra line breaks;
        # make consistent through script
        lines = f.read().splitlines()
        for line in lines:
            # Each line represents data for one employee.
            # Fixed format, with some header lines which start with * -
            # ignore these.
            if not line.startswith("*"):
                ucla_uid = line[0:9].strip()
                email_address = line[44:94].strip()
                if ucla_uid != "" and email_address != "":
                    fsemail_data[ucla_uid] = email_address
    print(f"fsemail_data: {len(fsemail_data)}")
    return fsemail_data


def _get_ucpath_data(ucpath_file):
    ucpath_data = {}

    with open(ucpath_file) as f:
        lines = f.readlines()
        for line in lines:
            # Each line represents data for one employee.
            # Dictionary from SQL query, not json, not pickle-compatible.
            employee = ast.literal_eval(line)
            # UC Path uses a single space instead of empty string;
            # other strings appear to be unpadded
            for key, val in employee.items():
                if val == " ":
                    employee[key] = ""

            # Get just the data needed for Alma updates,
            # renaming for consistency with student data.
            primary_id = employee["employee_id"]
            patron = {
                "PRIMARY_ID": primary_id,
                "FIRST_NAME": employee["emp_first_name"],
                "MIDDLE_NAME": employee["emp_middle_name"],
                "LAST_NAME": employee["emp_last_name"],
                "EMAIL_ADDRESS": employee["email_addr"],
                # These are called work addresses
                "ADDRESS_LINE1": employee["work_addr_line1"],
                "ADDRESS_LINE2": employee["work_addr_line2"],
                "ADDRESS_CITY": employee["work_addr_city"],
                "ADDRESS_STATE_PROVINCE": employee["work_addr_state"],
                "ADDRESS_POSTAL_CODE": employee["work_addr_zip"],
                # UC Path data currently does not include country; assume USA
                "ADDRESS_COUNTRY": "USA",
                "PHONE_NUMBER": employee["campus_phone"],
                "EMPLOYEE_TYPE": employee["type"],
                "IS_LAW": employee["law"],
            }
            # UC Path doesn't have full name, so assemble it
            patron["FULL_NAME"] = _get_full_name(patron)
            # User group
            patron["USER_GROUP"] = _get_employee_user_group(patron)
            ucpath_data[primary_id] = patron
    print(f"ucpath_data: {len(ucpath_data)}")
    return ucpath_data


def _get_registrar_data(registrar_file):
    registrar_data = {}

    with open(registrar_file) as f:
        lines = f.readlines()
        for line in lines:
            # Each line represents data for one student.
            # Dictionary from SQL query, not json, not pickle-compatible.
            student = ast.literal_eval(line)
            # Registrar data is space-padded; get rid of those extra spaces
            for key, val in student.items():
                student[key] = val.strip()

            # Most data is not relevant; get just what's needed,
            # renaming for consistency with non-student data.
            # Only need CAREER[1], DIVISION[2], DEPT[7], DEGREE[3], CLASS[3]
            # CAREER_REAL and HONORS_REAL included while porting
            # from obsolete Voyager StudentLoader.
            primary_id = student["STU_ID"]
            patron = {
                "PRIMARY_ID": primary_id,
                "FULL_NAME": student["STU_NM"],
                "CAREER_REAL": student["CAREER"],
                "CAREER": student["CAREER"][0],
                "CLASS": student["CLASS"],
                "DEGREE": student["DEG_CD"],
                "DEPT": student["SR_DEPT_CD"],
                "DIVISION": student["SR_DIV_CD"],
                "EMAIL_ADDRESS": student["SS_EMAIL_ADDR"],
                "HONORS_REAL": student["HONORS"],
                "HONORS": student["SP_PGM2"],
            }
            # Registrar has address data which needs remapping for Alma
            patron.update(_get_student_address(student))
            # Registrar has just 'UNITED' for USA; change to USA
            patron["ADDRESS_COUNTRY"] = (
                "USA"
                if patron["ADDRESS_COUNTRY"] == "UNITED"
                else patron["ADDRESS_COUNTRY"]
            )
            # Registrar provides just full name; add name parts
            patron.update(_split_patron_name(patron["FULL_NAME"]))
            # User group
            patron["USER_GROUP"] = _get_student_user_group(patron)

            registrar_data[primary_id] = patron
    print(f"registrar_data: {len(registrar_data)}")
    return registrar_data


def _get_bruincard_data(bruincard_file):
    bruincard_data = {}
    with open("bruincard_data.txt") as f:
        # CSV file, mostly... multiple types of record,
        # with different number of fields.
        # Some are quoted, some not.
        # Keep only rows with 'Active' in 4th field.
        for line in csv.reader(f):
            ucla_uid = line[1]
            barcode = ucla_uid + line[0]
            if line[3] == "Active":
                bc_existing = bruincard_data.get(ucla_uid, "0")
                if barcode > bc_existing:
                    bruincard_data[ucla_uid] = barcode
    print(f"bruincard_data: {len(bruincard_data)}")
    return bruincard_data


def _get_student_address(student):
    # Registrar provides 2 addresses / phones, local (M_) and permanent (P_).
    # Most students have only local data released for use.
    # If student has local address line 1, use local; else use permanent.
    if student["M_STREET_1"] != "":
        address = {
            "ADDRESS_LINE1": student["M_STREET_1"],
            "ADDRESS_LINE2": student["M_STREET_2"],
            "ADDRESS_CITY": student["M_CITY_NAME"],
            # Registrar has these separate; take the first that exists
            "ADDRESS_STATE_PROVINCE": student["M_STATE_CD"]
            if student["M_STATE_CD"] != ""
            else student["M_PROV_CD"],
            "ADDRESS_POSTAL_CODE": student["M_ZIP_CD"],
            "ADDRESS_COUNTRY": student["M_CNTRY7"],
            "PHONE_NUMBER": student["M_PHONE_NO"],
        }
    else:
        address = {
            "ADDRESS_LINE1": student["P_STREET_1"],
            "ADDRESS_LINE2": student["P_STREET_2"],
            "ADDRESS_CITY": student["P_CITY_NAME"],
            # Registrar has these separate; take the first that exists
            "ADDRESS_STATE_PROVINCE": student["P_STATE_CD"]
            if student["P_STATE_CD"] != ""
            else student["P_PROV_CD"],
            "ADDRESS_POSTAL_CODE": student["P_ZIP_CD"],
            "ADDRESS_COUNTRY": student["P_CNTRY7"],
            "PHONE_NUMBER": student["P_PHONE_NO"],
        }

    return address


def _split_patron_name(full_name):
    """
    Splits combined 'LAST, FIRST MIDDLE...' into 3 values in a dictionary.
    MIDDLE may contain multiple terms in one string;
    FIRST and LAST will be single terms
    """
    # Registrar data is usually 'LAST, FIRST MIDDLE' - 'comma space',
    # but may be multiple commas. May also be FIRST..LAST....
    # asking reg for better data.
    (last_name, separator, other_names) = full_name.partition(", ")
    if last_name == full_name:
        # No 'comma space'; assume name is FIRST MIDDLE(s) LAST or FIRST LAST.
        names = full_name.split(" ")
        first_name = names[0]
        last_name = names[-1]
        if len(names) >= 3:
            middle_name = " ".join(names[1 : len(names) - 1])
        else:
            middle_name = ""
    else:
        (first_name, separator, middle_name) = other_names.partition(" ")
    names = {
        "FIRST_NAME": first_name,
        "MIDDLE_NAME": middle_name,
        "LAST_NAME": last_name,
    }
    return names


def _get_full_name(patron):
    # Combined first/middle/last names into 'LAST, FIRST MIDDLE'
    return (
        f"{patron['LAST_NAME']}, {patron['FIRST_NAME']} {patron['MIDDLE_NAME']}".strip()
    )


def _get_student_user_group(patron):
    # Handles students only, as data is much different than employees.
    # Order of evaluation matters!

    # Music grads: dept values have changed... 1st 4 legacy, no data;
    # last 3 have data now.
    if patron["CAREER"] in ("G", "M", "D") and patron["DEPT"] in (
        "MUSCLGY",
        "MUSIC",
        "ETHNOMUS",
        "ETHNOMU",
        "ETHNMUS",
        "MUSC",
        "MUSCLG",
    ):
        user_group = "UGMU"
    # Management grads
    elif (
        patron["CAREER"] in ("G", "M", "D")
        and patron["DEGREE"] == "PHD"
        and patron["DIVISION"] == "MG"
    ):
        user_group = "UGM"
    # Law grads
    elif patron["CAREER"] == "L" or patron["DEGREE"] in ("JD", "LLM", "MLS"):
        user_group = "UGL"
    # Regular grads
    elif patron["CAREER"] in ("G", "M", "D"):
        user_group = "UG"
    # Music undergrads: dept values have changed... 1st 4 legacy, no data;
    # last 3 have data now.
    elif patron["CAREER"] in ("U", "I", "J") and patron["DEPT"] in (
        "MUSCLGY",
        "MUSIC",
        "ETHNOMUS",
        "ETHNOMU",
        "ETHNMUS",
        "MUSC",
        "MUSCLG",
    ):
        user_group = "UUMU"
    # Honors undergrads get treated like grads
    elif patron["CAREER"] in ("U", "I", "J") and patron["HONORS"] == "Y":
        user_group = "UG"
    # Regular undergrads
    elif patron["CAREER"] in ("U", "I", "J"):
        user_group = "UU"
    # TODO: Handle post-docs
    # Unknown / errors, so set a value which will make Alma reject the record
    else:
        user_group = "UNKNOWN"

    return user_group


def _get_employee_user_group(patron):
    # Handles employees only, as data is much different than students.
    # EMPLOYEE_TYPE and IS_LAW are integers.
    # Order of evaluation matters!

    # Academic (law)
    if patron["EMPLOYEE_TYPE"] == 4 and patron["IS_LAW"] == 1:
        user_group = "UAL"
    # Academic (regular)
    elif patron["EMPLOYEE_TYPE"] == 4 and patron["IS_LAW"] == 0:
        user_group = "UA"
    # Grad student (law)
    elif patron["EMPLOYEE_TYPE"] == 3 and patron["IS_LAW"] == 1:
        user_group = "UGL"
    # Grad student (regular)
    elif patron["EMPLOYEE_TYPE"] == 3 and patron["IS_LAW"] == 0:
        user_group = "UG"
    # Staff (law)
    elif patron["EMPLOYEE_TYPE"] == 1 and patron["IS_LAW"] == 1:
        user_group = "USL"
    # Staff (regular)
    elif patron["EMPLOYEE_TYPE"] == 1 and patron["IS_LAW"] == 0:
        user_group = "US"
    # Unknown / errors, so set a value which will make Alma reject the record
    else:
        user_group = "UNKNOWN"

    return user_group


def _get_hash(patron):
    # Hash the patron dictionary, so it can be stored and compared with future runs
    # to identify patrons whose campus data has not changed.
    encoded = json.dumps(patron, sort_keys=True).encode()
    return hashlib.sha1(encoded).hexdigest()


def _load_hashes() -> dict:
    # Loads patron hashes from a file into a dictionary,
    # keyed on patron ucla_uid.
    # Filename is constant.
    hash_file = "patron_hashes.dict"
    hashes = {}
    try:
        with open(hash_file, "r") as f:
            hashes = json.loads(f.read())
            print(f"Loaded previous hashes: {len(hashes)}")
    except FileNotFoundError:
        print(f"ERROR: {hash_file} not found, no hash comparison can be done.")
    return hashes


def _store_hashes(hashes: dict) -> None:
    # Stores a dictionary of patron hashes to a file.
    # Filename is constant.
    hash_file = "patron_hashes.dict"
    with open(hash_file, "w") as f:
        f.write(json.dumps(hashes))
        print(f"Stored hashes: {len(hashes)}")


if __name__ == "__main__":
    main()
