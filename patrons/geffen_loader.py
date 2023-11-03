import csv

# Long XML formatting routines for this project
import patron_xml_template as patron_xml


def _get_full_name(patron):
    # Combined first/middle/last names into 'LAST, FIRST MIDDLE'
    return (
        f"{patron['LAST_NAME']}, {patron['FIRST_NAME']} {patron['MIDDLE_NAME']}".strip()
    )


def _get_geffen_data() -> dict:
    # Return dictionary of patron dictionaries, keyed on UID
    geffen_data = {}
    with open("Geffen_Students.txt") as f:
        for line in csv.reader(f):
            if line:
                # Campus data drops leading zero from 9-digit UID;
                # left-pad with 0 as needed.
                primary_id = line[1].rjust(9, "0")
                patron = {
                    "PRIMARY_ID": primary_id,
                    "BARCODE": primary_id + line[0],
                    "FIRST_NAME": line[2],
                    "MIDDLE_NAME": line[3].replace("{null}", ""),
                    "LAST_NAME": line[4],
                    "EMAIL_ADDRESS": line[6],
                }
                patron["FULL_NAME"] = _get_full_name(patron)
                # These are all Geffen Academy Students
                patron["USER_GROUP"] = "GAS"
                # Use Geffen address for all
                patron["ADDRESS_LINE1"] = "11000 Kinross Avenue"
                patron["ADDRESS_LINE2"] = ""
                patron["ADDRESS_CITY"] = "Los Angeles"
                patron["ADDRESS_STATE_PROVINCE"] = "CA"
                patron["ADDRESS_POSTAL_CODE"] = "90095"
                patron["ADDRESS_COUNTRY"] = "USA"
                geffen_data[primary_id] = patron
    return geffen_data


def main() -> None:
    # Finally, write file of XML for load into Alma.
    geffen_data = _get_geffen_data()
    print(f"Geffen students: {len(geffen_data)}")
    patron_xml.write_xml(geffen_data, "geffen_students.xml")


if __name__ == "__main__":
    main()
