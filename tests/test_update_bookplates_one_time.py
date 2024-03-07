import unittest
from pymarc import Field, Subfield
from update_bookplates_one_time import (
    get_spac_mappings,
    needs_bookplate_update,
    get_spac_info,
    update_existing_966,
)


class TestUpdateBookplatesOneTime(unittest.TestCase):
    def test_needs_bookplate_update(self):
        spac_mappings = [
            {"SPAC": "SPAC1", "NAME": "SPAC Name", "URL": "https://example.com"},
            {"SPAC": "SPAC2", "NAME": "SPAC Name2", "URL": "https://example2.com"},
        ]
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC1"),
                Subfield(code="b", value="Wrong SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        # any matching SPAC code should trigger an update
        self.assertTrue(needs_bookplate_update(old_field, spac_mappings))

    def test_needs_bookplate_update_no_match(self):
        spac_mappings = [
            {"SPAC": "SPAC1", "NAME": "SPAC Name", "URL": "https://example.com"},
            {"SPAC": "SPAC2", "NAME": "SPAC Name2", "URL": "https://example2.com"},
        ]
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC3"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        # no matching SPAC code should not trigger an update
        self.assertFalse(needs_bookplate_update(old_field, spac_mappings))

    def test_get_spac_info(self):
        spac_mappings = [
            {"SPAC": "SPAC1", "NAME": "SPAC Name", "URL": "https://example.com"},
            {"SPAC": "SPAC2", "NAME": "SPAC Name2", "URL": "https://example2.com"},
        ]
        field_966 = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC1"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value="https://example.com"),
            ],
        )
        self.assertEqual(
            get_spac_info(spac_mappings, field_966),
            {"spac_name": "SPAC Name", "spac_url": "https://example.com"},
        )

    def test_get_spac_info_different_values(self):
        spac_mappings = [
            {"SPAC": "SPAC1", "NAME": "SPAC Name", "URL": "https://example.com"},
            {"SPAC": "SPAC2", "NAME": "SPAC Name2", "URL": "https://example2.com"},
        ]
        field_966 = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC1"),
                Subfield(code="b", value="Different Name"),
                Subfield(code="c", value="https://different-example.com"),
            ],
        )
        self.assertEqual(
            get_spac_info(spac_mappings, field_966),
            {"spac_name": "SPAC Name", "spac_url": "https://example.com"},
        )

    def test_update_existing_966_add_URL(self):
        old_field = Field(
            tag="966",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="SPAC"),
                Subfield(code="b", value="SPAC Name"),
                Subfield(code="c", value=""),
            ],
        )
        spac_url = "https://example.com"
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
        self.assertEqual(old_field.get_subfields("b")[0], spac_name)

    def test_get_spac_mappings(self):
        spac_mappings = get_spac_mappings("tests/data/sample_SPAC_mappings.csv")
        # only two rows in the test file have a valid URL
        sample_mappings = [
            {
                "SPAC": "SPAC1",
                "NAME": "Bookplate Label #1",
                "URL": "https://example.com",
            },
            {
                "SPAC": "SPAC2",
                "NAME": "Bookplate Label #2",
                "URL": "https://another-example.com",
            },
        ]
        self.assertEqual(spac_mappings, sample_mappings)


if __name__ == "__main__":
    unittest.main()
