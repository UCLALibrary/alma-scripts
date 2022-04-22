import re
import xml.etree.ElementTree as ET
import pprint as pp
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal

class Invoice():
	def __init__(self, xml, ns):
		# Alma invoice is XML, ns is namespace
		# Build dictionary of relevant data from Alma invoice XML
		self.data = self._get_invoice_data(xml, ns)
		self._remove_unwanted_lines()
		self._add_other_data()
		# Calculate and format the final data
		self.data['pac_lines'] = self._get_pac_lines()

	def dump(self):
		pp.pprint(self.data, width=120)

	def get_pac_format(self):
		# Returns a string representing the PAC format of the invoice.
		# Stick a LF on the end since join won't.
		# Output in UPPER CASE to make PAC happy.
		return ('\n'.join(self.data['pac_lines']) + '\n').upper()

	def is_valid(self):
		# Determines if invoice is valid for PAC, or caller should reject it.
		# Adds validation_message to invoice to provide context to caller.
		# Assume it's good, override if it's not
		# TODO: Separate validation process from boolean function call....
		valid = True
		validation_message = 'OK'
		unwanted_prefixes = [
			'ADJUST',
			'BINDERY',
			'FOREIGN',
			'HOLD',
			'PACKAGE',
			'RECHARGE',
			'REFUND',
			'REIMBURSE',
			'RUSH',
			'SPECIAL',
			'UCLARCHG',
			'WIRE',
		]
		invoice_number = self.data['invoice_number']
		# LBS-created invoices which should be rejected
		if self.data['vendor_code'] == 'LBS':
			validation_message = f'LBS Invoice'
			valid = False
		# Invoice with no vendor registered with campus
		elif self.data['vck'] is None:
			validation_message = f'No VCK'
			valid = False
		
		# Some of these are created by LBS, some not
		for prefix in unwanted_prefixes:
			if invoice_number.startswith(prefix):
				validation_message = f'Unwanted prefix: {prefix}'
				valid = False
				break
		# Procard: starts with 5, then 3 digits, then 3 upper letters, then at least one digit
		procard_regex = '^5[0-9]{3}[A-Z]{3}[0-9]'
		if re.search(procard_regex, invoice_number):
			validation_message = 'Unwanted Procard'
			valid = False
		# Are the totals correct (probably)?
		# if not self._check_totals():
		# 	validation_message = f'Totals do not match'
		# 	valid = False
		# Are all lines 80 characters, ASCII only?
		for line in self.data['pac_lines']:
			if len(line) != 80:
				validation_message = f'Bad length: {line}'
				valid = False
			if not line.isascii():
				validation_message = f'Not ASCII: {line}'
				valid = False
				
		self.data['validation_message'] = f'{invoice_number} : {validation_message}'
		return valid

	def _remove_unwanted_lines(self):
		# Remove lines where amount is zero
		remaining_lines = [line for line in self.data['invoice_lines'] if line['total_price'] != 0]
		self.data['invoice_lines'] = remaining_lines

	def _add_other_data(self):
		# Calculate data, and format fields which are needed repeatedly
		self.data['pac_invoice_number'] = self._format_invoice_number()
		self.data['pac_vck'] = self._format_vck()
		self.data['pac_due_date'] = self._get_due_date()
		# Add line-specific data to each invoice line
		for inv_line in self.data['invoice_lines']:
			self._add_line_data(inv_line)
		# Split any shipping/handling (ESH) lines into ESH & TSH
		self._make_tsh_lines()
		# More info at invoice header level, based on the above line changes
		self._calculate_totals()
		self.data['pac_invoice_type'] = self._get_invoice_type()
		# Update FAUs for any credit lines
		self._update_credit_faus()

	def _add_line_data(self, inv_line):
		# Alma DISCOUNT lines have positive amounts which need to be negative
		if self._is_discount_line(inv_line):
			inv_line['total_price'] = Decimal(0) - inv_line['total_price']
		inv_line['lbs_tax_code'] = self._get_lbs_tax_code(inv_line)
		inv_line['line_code'] = self._get_line_code(inv_line)
		inv_line['pac_tax_code'] = self._get_pac_tax_code(inv_line)
		inv_line['description'] = self._get_description(inv_line)
		inv_line['fund_count'] = len(inv_line['fund_info'])
		# Set PAC FAU info
		for fund in inv_line['fund_info']:
			fund['pac_fau'] = self._get_pac_fau(fund['fau'])

	def _make_tsh_lines(self):
		# Find any ESH line(s) and split them into ESH/TSH lines
		# Collect any TSH lines in separate list for now
		tmp_tsh_lines = []
		for inv_line in self.data['invoice_lines']:
			if inv_line['line_code'] == 'ESH':
				tsh_line = self._split_esh_line(inv_line)
				tmp_tsh_lines.append(tsh_line)
		# Add all TSH lines to main collection of lines
		self.data['invoice_lines'].extend(tmp_tsh_lines)

	def _split_esh_line(self, esh_line):
		# Shipping & handling lines (ESH) need to be split:
		# * ESH line has amount set to 80% of original
		# * TSH line gets created, with amount 20% of original
		esh_rate = Decimal('0.80')
		tsh_rate = Decimal('0.20')
		original_price = esh_line['total_price']
		tsh_line = deepcopy(esh_line)
		# Change ESH line so amount is 80% of original
		esh_line['original_price'] = original_price
		esh_line['total_price'] = self._get_dollars(original_price * esh_rate)
		esh_line['line_number'] += '-ESH'
		# Change TSH line so amount is 20% of original
		tsh_line['original_price'] = original_price
		tsh_line['total_price'] = self._get_dollars(original_price * tsh_rate)
		# Change other values
		tsh_line['line_code'] = 'TSH'
		tsh_line['line_number'] += '-TSH'
		tsh_line['pac_tax_code'] = self._get_pac_tax_code(tsh_line)
		return tsh_line

	def _update_credit_faus(self):
		# Set special FAU for credit line items
		# Applies only to regular Debit invoices, not credit memos.
		# Applies only to tax lines (BA), not regular ones
		if self.data['pac_invoice_type'] == 'D':
			for inv_line in self.data['invoice_lines']:
				if inv_line['line_code'] == 'CR ' and inv_line['line_type'] == 'BA':
					special_fau = self._get_special_tax_fau(inv_line)
					# These should never be on split funds, based on prior code
					fund_info = inv_line['fund_info'][0]
					fund_info['fau'] = special_fau
					fund_info['pac_fau'] = self._get_pac_fau(special_fau)
					inv_line['fund_info'][0] = fund_info

	def _get_invoice_data(self, xml, ns):
		# Invoice header data
		data = {}
		data['currency'] = self._get_value(xml, 'invoice_amount/currency', ns)
		data['invoice_date'] = self._to_date(self._get_value(xml, 'invoice_date', ns))
		data['invoice_number'] = self._get_value(xml, 'invoice_number', ns)
		data['total_amount_alma'] = Decimal(self._get_value(xml, 'invoice_amount/sum', ns))
		data['unique_identifier'] = self._get_value(xml, 'unique_identifier', ns)
		data['vck'] = self._get_value(xml, 'vendor_FinancialSys_Code', ns)
		data['vendor_code'] = self._get_value(xml, 'vendor_code', ns)
		# Invoice line item data
		line_xml = xml.find('alma:invoice_line_list', ns)
		data['invoice_lines'] = self._get_invoice_lines(line_xml, ns)
		return data

	def _get_invoice_lines(self, xml, ns):
		# List of dictionaries, one for each invoice
		invoice_lines = []
		for inv_line_xml in xml:
			invoice_lines.append(self._get_invoice_line(inv_line_xml, ns))
		# Sort list of lines by line number
		return sorted(invoice_lines, key = lambda line: int(line['line_number']))

	def _get_invoice_line(self, xml, ns):
		# Dictionary of line item info
		inv_line = {}
		inv_line['total_price'] = Decimal(self._get_value(xml, 'total_price', ns))
		inv_line['line_type'] = self._get_value(xml, 'line_type', ns)
		inv_line['line_number'] = self._get_value(xml, 'line_number', ns)
		inv_line['note'] = self._get_value(xml, 'note', ns)
		inv_line['mms_id'] = self._get_value(xml, 'po_line_info/mms_record_id', ns)
		inv_line['po_line_number'] = self._get_value(xml, 'po_line_info/po_line_number', ns)
		# This is the "first" reporting code - others have different names
		inv_line['reporting_code'] = self._get_value(xml, 'reporting_code', ns)
		inv_line['title'] = self._get_value(xml, 'po_line_info/po_line_title', ns)
		# Invoice line item fund data
		inv_line['fund_info'] = self._get_funds(xml, ns)
		return inv_line

	def _get_funds(self, xml, ns):
		# List of dictionaries, one for each fund used by an invoice line
		funds = []
		xml_funds = xml.find('alma:fund_info_list', ns)
		# Not all invoice lines have funds
		if xml_funds:
			for xml_fund in xml_funds:
				funds.append(self._get_fund(xml_fund, ns))
		# Sort list of funds by fund code
		return sorted(funds, key = lambda fund: fund['fund_code'])

	def _get_fund(self, xml, ns):
		# Dictionary of Alma fund info
		fund = {}
		fund['usd_amount'] = Decimal(self._get_value(xml, 'local_amount/sum', ns))
		fund['fau'] = self._get_value(xml, 'external_id', ns)
#		fund['pac_fau'] = self._get_pac_fau(fund['fau'])
		fund['fund_code'] = self._get_value(xml, 'code', ns)
		fund['fund_name'] = self._get_value(xml, 'name', ns)
		return fund

	def _get_value(self, xml, path, ns):
		# Prepend namespace shortcut 'alma' to each part of path
		element = '/'.join(f'alma:{term}' for term in path.split('/'))
		value = xml.findtext(element, None, ns)
		# Strip some unwanted characters
		# Tab becomes space, CR/LF become blank
		# Unwanted NBSP (U+00A0) also becomes blank
		if value != None:
			value = value.replace('\xa0', '').replace('\n', '')
			value = value.replace('\r', '').replace('\t', ' ')
			# PAC folks don't want a few other characters, though we've sent
			# them for years.  But they make invoice searching harder, so...
			# Replace '#' (Pound), '&' (Ampersand), '*' (Asterisk) and '_' (Underscore) with space.
			value = value.replace('#', ' ').replace('&', ' ')
			value = value.replace('*', ' ').replace('_', ' ')
			# Non-ASCII quotes
			value = value.replace('’', '\'')
			# Unicode EN and EM dashes
			value = value.replace('\u2013', '-').replace('\u2014', '-')
		return value

	def _get_blanks(self, num):
		# Return a string with num blanks
		blank = ' '
		return blank.ljust(num, blank)

	def _get_dollars(self, amount):
		# Modify Decimal amount to be dollars.cents, removing fractional cents.
		return amount.quantize(Decimal('.01'))

	def _format_amount(self, amount):
		# Amount must be a Decimal representing dollars & (optionally) cents.
		# Make sure amount is correctly formatted as dollars.cents
		dollars = self._get_dollars(amount)
		# Amounts in PAC are always non-negative
		dollars = abs(dollars)
		# Remove period to get a whole number, and left-pad with 0.
		return str(dollars).replace('.', '').rjust(15, '0')
	
	def _format_invoice_number(self):
		# Trim to max 23 chars, fixed length: right-pad with spaces if needed
		if self.data['invoice_number'] is not None:
			return self.data['invoice_number'][:23].ljust(23, ' ')

	def _format_vck(self):
		# VCK can be 10 characters; PAC wants only first 9
		if self.data['vck'] is not None:
			return self.data['vck'][:9]
		else:
			return 'NO VCK   '

	def _get_due_date(self):
		# Due date is always 25 days after invoice date
		return self.data['invoice_date'] + timedelta(days=25)

	def _get_invoice_type(self):
		# (D)ebit (we pay them) or (C)redit (they pay us) invoice
		if self.data['total_amount_pac'] >= 0:
			return 'D'
		else:
			return 'C'

	def _calculate_totals(self):
		vendor_invoice_total = Decimal(0)
		total_state_taxable = Decimal(0)
		total_vendor_taxable = Decimal(0)
		total_non_taxable = Decimal(0)
		total_state_tax = Decimal(0)
		total_vendor_tax = Decimal(0)
		for inv_line in self.data['invoice_lines']:
			line_amount = inv_line['total_price']
			pac_tax_code = inv_line['pac_tax_code']
			lbs_tax_code = inv_line['lbs_tax_code']
			line_code = inv_line['line_code']
			# Tax lines - BA, can't change this code
			if inv_line['line_type'] == 'BA':
				if lbs_tax_code[:2] in ['VR']:
					total_vendor_tax += line_amount
				elif line_code == 'CR ' and lbs_tax_code[:1] == 'T':
					# Special tax rate credit line, add to vendor tax total
					total_vendor_tax += line_amount
					# and subtract from non taxable total
					total_non_taxable -= line_amount
				elif lbs_tax_code[:2] in ['TM', 'TS']:
					total_state_tax += line_amount
				else:
					# Not really tax, LBS coding problem...
					total_non_taxable += line_amount
			else:
				if pac_tax_code == 'SM':
					total_state_taxable += line_amount
				elif pac_tax_code == 'TM':
					total_vendor_taxable += line_amount
				else:
					total_non_taxable += line_amount
		# How much is taxable?
		self.data['total_state_taxable'] = total_state_taxable
		self.data['total_vendor_taxable'] = total_vendor_taxable
		self.data['total_non_taxable'] = total_non_taxable
		# How much tax?
		self.data['total_state_tax'] = total_state_tax
		self.data['total_vendor_tax'] = total_vendor_tax
		# Discounts
		self.data['total_discount'] = self._calculate_discount_total()
		# Invoice total for PAC, which can differ from the Alma inv total
		self.data['total_amount_pac'] = total_state_taxable + total_vendor_taxable + total_non_taxable + total_vendor_tax
		
	def _calculate_discount_total(self):
		# Always 0?
		return Decimal('0.00')

	def _check_totals(self):
		# TODO: This does not catch all problems, like ESH/TSH miscoding
		return self.data['total_amount_alma'] == (self.data['total_amount_pac'] + self.data['total_state_tax'])

	def _get_pac_fau(self, fau):
		# Converts FAU (fund identifier) from the readable format in Alma
		# to the format required by PAC.
		# Have to split by position since some internal elements are optional.
		loc = fau[0:1]
		account = fau[2:8]
		cc = fau[9:11]
		fund = fau[12:17]
		sub = fau[18:20]
		obj = fau[21:25]
		project = fau[26:32]
		# Project must be 6 characters; right-pad with blanks, up to 6
		project = project.ljust(6, ' ')
		# Source was 6 blanks; LBS wants char 4-9 (1-based) of the unique identifier in hopes of a useful PAC identifier.
		source = self.data['unique_identifier'][3:9]
		return loc + account + cc + fund + project + sub + obj + source
	
	def _to_date(self, alma_date):
		# Converts Alma date format mm/dd/YYYY to real date
		return datetime.strptime(alma_date, '%m/%d/%Y')

	def _to_yymmdd(self, date):
		# Converts real date to PAC format yymmdd
		return date.strftime('%y%m%d')

	def _get_lbs_tax_code(self, inv_line):
		return inv_line['reporting_code']

	def _get_line_code(self, inv_line):
		# Can depend on different factors: LBS tax code, line type, amount
		lbs_tax_code = inv_line['lbs_tax_code']
		line_type = inv_line['line_type']
		amount = inv_line['total_price']
		# Order of these checks matters
		if amount < 0:
			line_code = 'CR'
		elif line_type == 'SHIPMENT' and lbs_tax_code[2:5] == '-SH':
			line_code = 'ESH'
		elif lbs_tax_code[2:5] == '-FT':
			line_code = 'FT'
		elif lbs_tax_code == 'EX-PR':
			line_code = 'SVS'
		elif lbs_tax_code[2:5] == '-PR' and lbs_tax_code != 'EX-PR':
			line_code = 'MAT'
		elif lbs_tax_code in ['TA', 'TB', 'TC', 'TD', 'TE', 'TF', 'TG', 'TH', 'TI']:
			line_code = 'CR' # special non-standard tax is treated as credit
		else:
			line_code = 'DR'
		# 2 or 3 letters; must be padded to 3 chars.
		return line_code.ljust(3, ' ')

	def _get_pac_tax_code(self, inv_line):
		# Consists of 1-char sales_tax_code and 1-char tax_rate_group_code;
		# they always get set together so this routine is enough.
		# Order of evaluation probably matters.
		# TODO: Can we always just use 1st 2 chars of lbs_tax_code here?
		lbs_tax_code = inv_line['lbs_tax_code']
		line_code = inv_line['line_code']
		if lbs_tax_code[:2] == 'EX' or line_code == 'FT':
			pac_tax_code = 'E '
		# Some shipping/handling lines are VR ESH/TSH; 
		# all VR is handled the same
		elif lbs_tax_code[:2] == 'VR':
			pac_tax_code = 'TM'
		# Non-VR ESH
		elif line_code == 'ESH':
			pac_tax_code = 'E '
		elif line_code == 'TSH':
			pac_tax_code = 'SM'
		elif line_code == 'CR ':
			pac_tax_code = 'E '
		else:
			pac_tax_code = 'SM'

		return pac_tax_code

	def _get_special_tax_fau(self, inv_line):
		# If the invoice line is for a special tax rate, where the vendor
		# did not charge full tax for whatever reason, the line is treated
		# as a credit and requires a special FAU, which varies based on the 
		# LBS tax code.
		fau_accounts = {
			'TA': '115523', # 0.25%
			'TB': '115522', # 0.50%
			'TC': '115524', # 0.75%
			'TD': '115521', # 1.00%
			'TE': '115529', # 1.25%
			'TF': '115528', # 1.50% - also default account
			'TG': '115518', # 1.75%
			'TH': '115519', # 2.00%
			'TI': '115525'	# 2.25%
		}
		lbs_tax_code = inv_line['lbs_tax_code']
		account = fau_accounts.get(lbs_tax_code, '115528')
		# Special FAU has only location (4), account, and fund 18888
		return f'4 {account}    18888        '

	def _get_description(self, inv_line):
		# Combine several data elements.
		# Limit is 55 characters; if shorter, must be right-padded with spaces.
		# note can be None; other values always exist.
		description = \
			(self.data['unique_identifier'] + ' : ' + \
			inv_line['line_number'] + ' : ' + \
			str(inv_line['note']))
		return description[:55].ljust(55, ' ')

	def _is_discount_line(self, inv_line):
		# Returns true if inv_line is DISCOUNT and has a positive amount
		if inv_line['line_type'] == 'DISCOUNT' and inv_line['total_price'] > Decimal(0):
			return True
		else:
			return False

	def _needs_z21_line_item(self, inv_line):
		# Most "line items" need a Z21 line created, except for
		# regular taxes (not special tax rates)
		# Tax has Alma line type code BA, can't change it
		if inv_line['line_type'] == 'BA' and inv_line['lbs_tax_code'][:2] in ['TM', 'TS', 'VR']:
			return False
		else:
			return True

	def _get_z20_lines(self):
		# Every invoice has 1 Z20 card, representing the invoice header.
		# These are fixed format, 3 lines/card, 80 char/line.
		# Much of the data is constant; plug in relevant data, 
		# trimmed / formatted as needed.
		z20_line1 = self._get_z20_line1()
		z20_line2 = self._get_z20_line2()
		z20_line3 = self._get_z20_line3()
		return [z20_line1, z20_line2, z20_line3]

	def _get_z20_line1(self):
		batch_number = '999895'
		z20_line1 = \
			'Z200101 A' + \
			self.data['pac_vck'] + ' ' + \
			self.data['pac_invoice_number'] + \
			self._to_yymmdd(self.data['invoice_date']) + \
			f'99{batch_number}' + \
			self._get_blanks(24)
		return z20_line1

	def _get_z20_line2(self):
		z20_line2 = \
			'Z200102 ' + \
			self._format_amount(self.data['total_amount_pac']) + \
			self.data['pac_invoice_type'] + \
			self._format_amount(self.data['total_vendor_tax']) + \
			'99' + \
			self._format_amount(self.data['total_discount']) + \
			self._to_yymmdd(self.data['pac_due_date']) + \
			self._get_blanks(18)
		return z20_line2

	def _get_z20_line3(self):
		z20_line3 = \
			'Z200103    00    UCLANONE        CA' + \
			self._get_blanks(45)
		return z20_line3

	def _get_z21_lines(self):
		z21_lines = []
		pac_line_number = 0
		for inv_line in self.data['invoice_lines']:
			if self._needs_z21_line_item(inv_line):
				pac_line_number += 1
				inv_line['pac_line_number'] = pac_line_number
				z21_line1 = self._get_z21_line1(inv_line)
				z21_line2 = self._get_z21_line2(inv_line)
				z21_line3 = self._get_z21_line3(inv_line)
				z21_lines.extend([z21_line1, z21_line2, z21_line3])
				if inv_line['fund_count'] > 1:
					z41_lines = self._get_z41_lines(inv_line)
					z21_lines.extend(z41_lines)
		return z21_lines

	def _get_z21_line1(self, inv_line):
		z21_line1 = \
			'Z210101 A' + \
			self.data['pac_vck'] + ' ' + \
			self.data['pac_invoice_number'] + \
			str(inv_line['pac_line_number']).rjust(4, '0') + \
			inv_line['line_code'] + \
			self._get_blanks(31) # Includes undocumented blank, to pad to 80 chars
		return z21_line1

	def _get_z21_line2(self, inv_line):
		z21_line2 = \
			'Z210102 ' + \
			self._format_amount(inv_line['total_price']) + \
			inv_line['description'] + \
			inv_line['pac_tax_code']
		return z21_line2

	def _get_z21_line3(self, inv_line):
		# If a line has only one fund, FAU info in Z21 line 3, and no Z41s.
		# If a line has multiple funds, create Z41s, and no FAU info
		# in Z21 line 3.
		z41_lines = []
		if inv_line['fund_count'] == 1:
			fund_info = inv_line['fund_info'][0]
			z21_line3 = \
				'Z210103 ' + \
				fund_info['pac_fau'] + \
				self._get_blanks(26) + \
				'E' + self._get_blanks(4) + \
				inv_line['line_code'] + self._get_blanks(6)
		else:
			z21_line3 = \
				'Z210103 ' + \
				self._get_blanks(32) + \
				self._get_blanks(26) + \
				'E' + self._get_blanks(4) + \
				inv_line['line_code'] + self._get_blanks(6)
		return z21_line3

	def _get_z41_lines(self, inv_line):
		# Invoice lines with multiple funds need a Z41 card for each fund.
		# These are fixed format, 3 lines/card, 80 char/line.
		z41_lines = []
		for fund_info in inv_line['fund_info']:
			z41_line1 = \
				'Z410101 A' + \
				self.data['pac_vck'] + ' ' + \
				self.data['pac_invoice_number'] + \
				self._get_blanks(30) + \
				str(inv_line['pac_line_number']).rjust(4, '0') + \
				self._get_blanks(4)

			z41_line2 = \
				'Z410102 ' + \
				self._get_blanks(23) + \
				self._format_amount(fund_info['usd_amount']) + \
				self._get_blanks(34)

			z41_line3 = \
				'Z410103 ' + \
				fund_info['pac_fau'] + \
				self._get_blanks(40)

			z41_lines.extend([z41_line1, z41_line2, z41_line3])
		return z41_lines

	def _get_z25_lines(self):
		# Every invoice has 1 Z25 card, authorizing the invoice for payment.
		# These are fixed format, 1 line/card, 80 char/line.
		z25_line1 = \
			'Z250101 A' + \
			self.data['pac_vck'] + ' ' + \
			self.data['pac_invoice_number'] + \
			self._get_blanks(6) + \
			'Y' + self._get_blanks(31)
		return [z25_line1]

	def _get_pac_lines(self):
		pac_lines = self._get_z20_lines()
		pac_lines.extend(self._get_z21_lines())
		pac_lines.extend(self._get_z25_lines())
		return pac_lines
