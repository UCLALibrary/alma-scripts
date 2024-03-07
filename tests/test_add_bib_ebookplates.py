import unittest
from pymarc import Field, Record, Subfield
from add_bib_ebookplates import (
    get_report_ebookplates,
    is_new_966,
    needs_bookplate_update,
    add_new_966,
    update_existing_966,
)


class TestAddBibEbookplates(unittest.TestCase):

    def test_get_report_ebookplates(self):
        sample_mapping_file = "tests/data/sample_SPAC_mappings.csv"
        sample_report_data = [
            {"MMS Id": "MMS1", "Fund Code": "FUND1"},
        ]
        report_with_ebookplates = get_report_ebookplates(
            sample_report_data, sample_mapping_file
        )
        self.assertEqual(report_with_ebookplates[0]["spac_code"], "SPAC1")
        self.assertEqual(report_with_ebookplates[0]["spac_name"], "Bookplate Label #1")
        self.assertEqual(report_with_ebookplates[0]["spac_url"], "https://example.com")

    def test_get_report_ebookplates_no_url(self):
        sample_mapping_file = "tests/data/sample_SPAC_mappings.csv"
        sample_report_data = [
            {"MMS Id": "MMS1", "Fund Code": "FUND3"},
        ]
        report_with_ebookplates = get_report_ebookplates(
            sample_report_data, sample_mapping_file
        )
        self.assertEqual(report_with_ebookplates[0]["spac_code"], "SPAC3")
        self.assertEqual(report_with_ebookplates[0]["spac_name"], "Bookplate Label #3")
        self.assertEqual(report_with_ebookplates[0]["spac_url"], "")

    def test_get_report_ebookplates_multiple_funds_single_spac(self):
        sample_mapping_file = "tests/data/sample_SPAC_mappings.csv"
        sample_report_data = [
            {"MMS Id": "MMS1", "Fund Code": "FUND2A"},
            {"MMS Id": "MMS2", "Fund Code": "FUND2B"},
        ]
        report_with_ebookplates = get_report_ebookplates(
            sample_report_data, sample_mapping_file
        )
        # in mapping file, FUND2A and FUND2B both map to SPAC2
        # this also tests that comma separated fund codes are handled correctly
        self.assertEqual(report_with_ebookplates[0]["spac_code"], "SPAC2")
        self.assertEqual(report_with_ebookplates[0]["spac_name"], "Bookplate Label #2")
        self.assertEqual(
            report_with_ebookplates[0]["spac_url"], "https://another-example.com"
        )
        self.assertEqual(report_with_ebookplates[1]["spac_code"], "SPAC2")
        self.assertEqual(report_with_ebookplates[1]["spac_name"], "Bookplate Label #2")
        self.assertEqual(
            report_with_ebookplates[1]["spac_url"], "https://another-example.com"
        )

    def test_get_report_ebookplates_multiple_spacs_single_fund(self):
        sample_mapping_file = "tests/data/sample_SPAC_mappings.csv"
        sample_report_data = [
            {"MMS Id": "MMS1", "Fund Code": "FUND4"},
        ]
        report_with_ebookplates = get_report_ebookplates(
            sample_report_data, sample_mapping_file
        )
        # in mapping file, FUND4 maps to SPAC4 and SPAC5
        self.assertEqual(report_with_ebookplates[0]["spac_code"], "SPAC4")
        self.assertEqual(report_with_ebookplates[0]["spac_name"], "Bookplate Label #4")
        self.assertEqual(report_with_ebookplates[0]["spac_url"], "https://example.com")
        self.assertEqual(report_with_ebookplates[1]["spac_code"], "SPAC5")
        self.assertEqual(report_with_ebookplates[1]["spac_name"], "Bookplate Label #5")
        self.assertEqual(
            report_with_ebookplates[1]["spac_url"],
            "https://another-example.com",
        )

    def test_is_new_966_original_empty(self):
        record = Record()
        spac_code = "SPAC"
        self.assertTrue(is_new_966(record, spac_code))

    def test_is_new_966_exact_match(self):
        record = Record()
        spac_code = "SPAC"
        record.add_field(
            Field(
                tag="966",
                indicators=[" ", " "],
                subfields=[
                    Subfield(code="a", value=spac_code),
                ],
            )
        )
        self.assertFalse(is_new_966(record, spac_code))

    def test_needs_bookplate_update_no_matching_ab(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        spac_code = "NOTSPAC"
        spac_name = "Different SPAC Name"
        spac_url = "https://example.com"

        # This is a new 966 field, so it doesn't need updating
        self.assertFalse(
            needs_bookplate_update(old_field, spac_code, spac_name, spac_url)
        )

    def test_needs_bookplate_update_no_original_c(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
            ],
        )
        spac_code = "SPAC"
        spac_name = "SPAC Name"
        spac_url = "https://example.com"

        # We will need to update the URL, since the original 966 field has no $c subfield
        self.assertTrue(
            needs_bookplate_update(old_field, spac_code, spac_name, spac_url)
        )

    def test_needs_bookplate_update_empty_new_url(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        spac_code = "SPAC"
        spac_name = "SPAC Name"
        spac_url = ""

        # We will need to update the URL, since the new URL is empty
        self.assertTrue(
            needs_bookplate_update(old_field, spac_code, spac_name, spac_url)
        )

    def test_needs_bookplate_update_new_url_mismatch(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        spac_code = "SPAC"
        spac_name = "SPAC Name"
        spac_url = "https://newexample.com"

        # We will need to update the URL, since the new URL doesn't match the original
        self.assertTrue(
            needs_bookplate_update(old_field, spac_code, spac_name, spac_url)
        )

    def test_needs_bookplate_update_new_name_mismatch(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        spac_code = "SPAC"
        spac_name = "Different SPAC Name"
        spac_url = "https://example.com"

        # We will need to update the bookplate text, since the new name doesn't match the original
        self.assertTrue(
            needs_bookplate_update(old_field, spac_code, spac_name, spac_url)
        )

    def test_add_new_966(self):
        old_record = Record()
        spac_code = "SPAC"
        spac_name = "SPAC Name"
        spac_url = "https://example.com"
        add_new_966(old_record, spac_code, spac_name, spac_url)
        self.assertTrue(old_record.get_fields("966"))
        self.assertEqual(
            old_record.get_fields("966")[0].get_subfields("a")[0], spac_code
        )
        self.assertEqual(
            old_record.get_fields("966")[0].get_subfields("b")[0], spac_name
        )
        self.assertEqual(
            old_record.get_fields("966")[0].get_subfields("c")[0], spac_url
        )

    def test_update_existing_966_replace_URL(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        spac_url = "https://newexample.com"
        spac_name = "SPAC Name"
        update_existing_966(old_field, spac_name, spac_url)
        self.assertEqual(old_field.get_subfields("c")[0], spac_url)
        self.assertEqual(old_field.get_subfields("b")[0], spac_name)

    def test_update_existing_966_remove_URL(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        spac_url = ""
        spac_name = "SPAC Name"
        update_existing_966(old_field, spac_name, spac_url)
        self.assertEqual(old_field.get_subfields("c"), [])


if __name__ == "__main__":
    unittest.main()
