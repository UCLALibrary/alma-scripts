import xml.etree.ElementTree as ET


def get_MARCXML_from_bib(alma_bib: bytes) -> bytes:
    """Takes an Alma Bib and returns only the MARCXML content as a bytestring."""
    root = ET.fromstring(alma_bib)
    record = root.find("record")
    marc_xml = ET.tostring(record, encoding="utf8", method="xml", xml_declaration=False)
    return marc_xml


def update_alma_bib_xml(orig_bib: bytes, new_record: bytes) -> bytes:
    """Takes an Alma Bib and a MARCXML Record and returns an updated Bib bytestring
    containing the new Record."""
    bib_element = ET.fromstring(orig_bib)
    record_element = ET.fromstring(new_record)
    old_record = bib_element.find("record")
    bib_element.remove(old_record)
    bib_element.append(record_element)

    # xml_declaration=False because the Update Bib API doesn't require it, and the API
    # call fails if xml_declaration=True due to escape characters in ET's declaration
    bib_xml = ET.tostring(
        bib_element, encoding="utf8", method="xml", xml_declaration=False
    )
    return bib_xml
