# Routines for converting patron dictionaries to the XML Alma requires.
from datetime import date, timedelta
from xml.sax.saxutils import escape


def get_expiry_date():
    # About 13 months from today - close enough, no need for day-of-month precision here.
    # Format as 'YYYY-MM-DD'
    expiry_date = date.today() + timedelta(days=round(13 * 365 / 12))
    return expiry_date.strftime("%Y-%m-%d")


# Use only one consistent expiry date; this is relevant only in the XML for Alma.
EXPIRY_DATE = get_expiry_date()
# Use consistent status date of today
STATUS_DATE = date.today().strftime("%Y-%m-%d")
# Use consistent purge date, currently 2036-12-31
PURGE_DATE = "2036-12-31"


def get_patron_xml(patron):
    """Returns a string of XML with patron data embedded"""
    xml_string = f"""\
<user>
	<record_type>PUBLIC</record_type>
	<primary_id>{patron['PRIMARY_ID']}</primary_id>
	<first_name>{patron['FIRST_NAME']}</first_name>
	<middle_name>{patron['MIDDLE_NAME']}</middle_name>
	<last_name>{patron['LAST_NAME']}</last_name>
	<full_name>{patron['FULL_NAME']}</full_name>
	<pin_number></pin_number>
	<user_title></user_title>
	<job_category></job_category>
	<job_description></job_description>
	<gender></gender>
	<user_group>{patron['USER_GROUP']}</user_group>
	<campus_code/>
	<web_site_url></web_site_url>
	<cataloger_level>00</cataloger_level>
	<preferred_language>en</preferred_language>
	<expiry_date>{EXPIRY_DATE}Z</expiry_date>
	<purge_date>{PURGE_DATE}Z</purge_date>
	<account_type>EXTERNAL</account_type>
	<external_id>SIS_temp</external_id>
	<password></password>
	<force_password_change></force_password_change>
	<status>ACTIVE</status>
	<status_date>{STATUS_DATE}Z</status_date>
	<pref_first_name></pref_first_name>
	<pref_middle_name></pref_middle_name>
	<pref_last_name></pref_last_name>
	<pref_name_suffix></pref_name_suffix>
	<user_roles/>
	<user_blocks/>
	<user_notes/>
	<user_statistics/>
	<proxy_for_users/>
"""
    xml_string += get_contact_info(patron)
    xml_string += get_barcodes(patron)
    # Close the XML for this patron
    xml_string += "</user>\n"

    return xml_string


def get_contact_info(patron):
    xml_string = "\t<contact_info>\n"
    xml_string += get_addresses(patron)
    xml_string += get_phones(patron)
    xml_string += get_emails(patron)
    xml_string += "\t</contact_info>\n"
    return xml_string


def get_addresses(patron):
    # Just one address for now
    xml_string = f"""\
		<addresses>
			<address preferred="true" segment_type="External">
				<line1>{patron['ADDRESS_LINE1']}</line1>
				<line2>{patron['ADDRESS_LINE2']}</line2>
				<city>{patron['ADDRESS_CITY']}</city>
				<state_province>{patron['ADDRESS_STATE_PROVINCE']}</state_province>
				<postal_code>{patron['ADDRESS_POSTAL_CODE']}</postal_code>
				<country>{patron['ADDRESS_COUNTRY']}</country>
				<address_note></address_note>
				<start_date>{STATUS_DATE}Z</start_date>
				<end_date>{EXPIRY_DATE}Z</end_date>
				<address_types>
					<address_type>home</address_type>
				</address_types>
			</address>
		</addresses>
"""
    return xml_string


def get_phones(patron):
    # Just one phone number for now.
    # Skip this if no phone number.
    xml_string = ""
    if patron.get("PHONE_NUMBER"):
        xml_string = f"""\
		<phones>
			<phone preferred="true" preferred_sms="false" segment_type="External">
				<phone_number>{patron['PHONE_NUMBER']}</phone_number>
				<phone_types>
					<phone_type>home</phone_type>
				</phone_types>
			</phone>
		</phones>
"""
    return xml_string


def get_emails(patron):
    # Just one email address for now
    xml_string = f"""\
		<emails>
			<email preferred="true" segment_type="External">
				<email_address>{patron['EMAIL_ADDRESS']}</email_address>
				<email_types>
					<email_type>work</email_type>
				</email_types>
			</email>
		</emails>
"""
    return xml_string


def get_barcodes(patron):
    # Just one barcode for now
    # Skip this if no barcode.
    xml_string = ""
    if patron.get("BARCODE") is not None:
        xml_string = f"""\
		<user_identifiers>
			<user_identifier segment_type="External">
				<id_type>BARCODE</id_type>
				<value>{patron['BARCODE']}</value>
				<note></note>
				<status>ACTIVE</status>
			</user_identifier>
		</user_identifiers>
"""
    return xml_string


def write_xml(patrons, xml_file: str = "alma_patrons.xml") -> None:
    # Existing file will be replaced.
    with open(xml_file, "w+t") as xml:
        header = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        list_start = "<users>\n"
        list_end = "</users>\n"

        xml.write(header)
        xml.write(list_start)
        for ucla_uid, patron in patrons.items():
            # Campus data can have unsafe-for-xml characters; escape strings for xml
            for key, val in patron.items():
                if isinstance(val, str):
                    patron[key] = escape(val)
            xml.write(get_patron_xml(patron))
        # Outside the patron loop
        xml.write(list_end)
