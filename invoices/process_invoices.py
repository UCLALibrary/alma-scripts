#!/usr/bin/env python3

import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from invoice import Invoice

def _get_pac_filename():
	# Daily files, named like: LIBRY-APINTRFC.YYYYMMDD
	# where YYYYMMDD is today's date.
	today = datetime.strftime(datetime.now(), '%Y%m%d')
	file_name = f'LIBRY-APINTRFC.{today}'
	return file_name

def _write_invoice_to_file(pac_invoice, pac_file):
	with open(pac_file, 'a') as f:
		f.writelines(pac_invoice)

# For testing only, modify invoice number to reflect test batch
def _inject_test_number(invoice, test_batch):
	invoice.data['invoice_number'] += test_batch
	invoice.data['pac_invoice_number']= invoice._format_invoice_number()
	invoice.data['pac_lines'] = invoice._get_pac_lines()

def main():
	PROD = True
	xml_file = sys.argv[1]
	pac_file = _get_pac_filename()
	if os.path.exists(pac_file):
		os.remove(pac_file)
	root = ET.parse(xml_file).getroot()
	# Namespace
	ns = {'alma': 'http://com/exlibris/repository/acq/invoice/xmlbeans'}
	# Loop through Alma XML data to build pac_invoice dictionary
	for alma_invoice in root.findall('.//alma:invoice', ns):
		invoice = Invoice(alma_invoice, ns)
		#####_inject_test_number(invoice, '-2')
		if PROD:
			if invoice.is_valid():
				_write_invoice_to_file(invoice.get_pac_format(), pac_file)
		else:
			# TODO: Changes to is_valid()
			invoice.is_valid()
			invoice.dump()
		# TODO: Real logging
		print(invoice.data['validation_message'])

if __name__ == '__main__':
	main()
