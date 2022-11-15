import csv
import sys
from datetime import datetime as dt


def get_xml_header() -> str:
    return """\
<?xml version="1.0" encoding="UTF-8"?>
<xb:payment_confirmation_data xmlns:xb="http://com/exlibris/repository/acq/xmlbeans">
  <xb:invoice_list>
"""


def get_xml_footer() -> str:
    return """\
  </xb:invoice_list>
</xb:payment_confirmation_data>
"""


def get_yyyymmdd(date_string: str) -> str:
    # Convert date_string "m/d/yyyy" to yyyymmdd
    return dt.strptime(date_string, "%m/%d/%Y").strftime("%Y%m%d")


def get_amount(amount_string: str) -> str:
    # Convert Excel-based string to required format:
    # Remove commas, format with 2 decimal places
    amount_float = float(amount_string.replace(",", ""))
    return f"{amount_float:.2f}"


def get_xml_invoice(row: dict) -> str:
    # dict_keys(['Vendor Code', 'Invoice Number',
    # 'Invoice Date', 'Invoice Gross Amount', 'Transaction Amount',
    # 'Check Number', 'Check Date'])
    vendor_code = row["Vendor Code"]
    # These have trailing spaces and non-breaking spaces
    invoice_number = row["Invoice Number"].strip()
    invoice_date = get_yyyymmdd(row["Invoice Date"])
    check_number = row["Check Number"]
    check_date = get_yyyymmdd(row["Check Date"])
    transaction_amount = get_amount(row["Transaction Amount"])
    return f"""\
    <xb:invoice>
        <xb:vendor_code>{vendor_code}</xb:vendor_code>
        <xb:invoice_number>{invoice_number}</xb:invoice_number>
        <xb:unique_identifier></xb:unique_identifier>
        <xb:payment_status>PAID</xb:payment_status>
        <xb:payment_note></xb:payment_note>
        <xb:invoice_date>{invoice_date}</xb:invoice_date>
        <xb:payment_voucher_date>{check_date}</xb:payment_voucher_date>
        <xb:payment_voucher_number>{check_number}</xb:payment_voucher_number>
        <xb:voucher_amount>
          <xb:currency>USD</xb:currency>
          <xb:sum>{transaction_amount}</xb:sum>
        </xb:voucher_amount>
    </xb:invoice>
"""


def main():
    input_file = sys.argv[1]
    xml = get_xml_header()
    # Windows-derived CSV has leading BOM, so specify utf-8-sig, not utf-8
    with open(input_file, encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file, dialect="excel")
        for row in reader:
            # Skip "empty" rows which should not be in the data...
            if row["Vendor Code"] != "":
                xml += get_xml_invoice(row)
        xml += get_xml_footer()
    print(xml)


if __name__ == "__main__":
    main()
