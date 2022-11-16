import xml.etree.ElementTree as ET
from pymarc import parse_xml_to_array, record_to_xml_node, Record
from io import BytesIO


def get_record_from_bib(alma_bib: bytes) -> Record:
    """Takes an Alma Bib and returns a pymarc Record containing <record> content."""
    root = ET.fromstring(alma_bib)
    record = root.find("record")

    # xml_declaration=False since we want only the <record> element, not a full XML doc
    marc_xml = ET.tostring(record, encoding="utf8", method="xml", xml_declaration=False)
    # pymarc needs file-like object
    with BytesIO(marc_xml) as fh:
        pymarc_record = parse_xml_to_array(fh)[0]
    return pymarc_record


def update_alma_bib_record(original_bib: bytes, new_record: Record) -> bytes:
    """Takes an Alma Bib and a pymarc Record and returns an updated Bib bytestring
    containing data from the new Record in the <record> element."""
    # convert Bib and Record to ET Elements,
    # using pymarc's record_to_xml_node for new_record
    bib_element = ET.fromstring(original_bib)
    new_record_element = record_to_xml_node(new_record)

    old_record_element = bib_element.find("record")
    bib_element.remove(old_record_element)
    bib_element.append(new_record_element)

    # xml_declaration=False because the Update Bib API doesn't require it, and the API
    # call fails if xml_declaration=True due to escape characters in ET's declaration
    bib_xml = ET.tostring(
        bib_element, encoding="utf8", method="xml", xml_declaration=False
    )
    return bib_xml
