#!/usr/bin/env -S python3 -u
import argparse
import sys
import traceback
from datetime import datetime
from pymarc import Record, Field, MARCReader, MARCWriter

def get_output_filename(oclc_symbol):
	# Returns filename required by WEST
	yyyymmdd = datetime.today().strftime('%Y%m%d')
	return f'{oclc_symbol}.alma.archived.{yyyymmdd}.mrc'


def get_tag_mapping():
	# Exported Alma bib records with holdings data embedded, via publishing profile.
	# Fields from the holdings record have first character of 'H' to make it possible
	# to identify these when embedded in bib records.
	return {
		'H52': '852',
		'H61': '561',
		'H66': '866',
		'H67': '867',
		'H68': '868',
		'H83': '583',
	}


def create_lhr(record):
	# Copy embedded holdings fields, plus a few bib fields, to create a new holdings record.
	lhr = Record()
	# Do remapping first so we can work with the real tag names
	tag_mapping = get_tag_mapping()
	for source_tag, target_tag in tag_mapping.items():
		for fld in record.get_fields(source_tag):
			fld.tag = target_tag
			lhr.add_ordered_field(fld)

	# Get record ids and add to new record
	holdings_id = get_holdings_id(record)
	lhr.add_ordered_field(Field(tag='001', data=holdings_id))
	bib_id = get_bib_id(record)
	lhr.add_ordered_field(Field(tag='004', data=bib_id))

	# Remove unwanted subfield $8 from copied holdings fields
	remove_sfd8s(lhr)
	# Remove unwanted 561 fields
	remove_unwanted_561s(lhr)
	# Update 852 field
	update_852_field(lhr)
	# Create bare-bones fixed fields
	add_007_field(lhr)
	add_008_field(lhr)
	update_leader(lhr)
	# Copy specific other fields from bib record to new record
	get_bib_fields(record, lhr)

	return lhr


def add_007_field(lhr):
	lhr.add_ordered_field(Field(tag='007', data='tu'))


def add_008_field(lhr):
	yymmdd = datetime.today().strftime('%y%m%d')
	lhr.add_ordered_field(Field(tag='008', data=f'{yymmdd}0u    8   0001uu   0{yymmdd}'))
	

def update_leader(lhr):
	# https://www.loc.gov/marc/holdings/hdleader.html
	lhr.leader[5:7] = 'cy'
	lhr.leader[9] = 'a'
	lhr.leader[17:19] = 'un'


def get_bib_id(record):
	# Bib id is in the 001 field.  There will always be one and only one.
	return record['001'].value()


def get_bib_fields(bib_record, lhr):
	# Copy some bib fields as-is to holdings record.
	# 022 (ISSN)
	for fld in bib_record.get_fields('022'):
		lhr.add_ordered_field(fld)
	# 035 (OCLC number): only real OCLC# fields
	for fld in bib_record.get_fields('035'):
		sfd_a = fld.get_subfields('a')
		if len(sfd_a) > 0 and sfd_a[0].startswith('(OCoLC)'):
			lhr.add_ordered_field(fld)


def get_holdings_id(record):
	# Alma export adds holdings id to $8 of exported holdings fields.
	# There will always be an 852 in record, so get holdings id from that.
	return record['852']['8']


def remove_unwanted_561s(lhr):
	# Many 561 fields are not for UCLA/SRLF; remove them
	for fld in lhr.get_fields('561'):
		sfd_a = fld.get_subfields('a')
		if len(sfd_a) > 0 and sfd_a[0] not in ['CLU', 'CLUSP', 'ZAS', 'ZASSP']:
			lhr.remove_field(fld)


def remove_sfd8s(lhr):
	# Alma export adds $8 with holdings id to each exported holdings field.
	# After storing this via get_holdings_id(), these are no longer needed/wanted.
	for fld in lhr.get_fields():
		if not fld.is_control_field():
			fld.delete_subfield('8')


def get_oclc_symbol(lhr):
	# Returns the OCLC "symbol", aka OCLC holdings code.
	# This is based on our location code, which at this point is in 852 $b.
	location_code = lhr['852']['b'].lower()
	# Only specific SRLF locations should be used, one per symbol
	if location_code == 'srbuo':
		oclc_symbol = 'ZAS'
	elif location_code == 'srucl':
		oclc_symbol = 'SPLC'
	elif location_code == 'srucl2':
		oclc_symbol = 'JACS'
	# Log any other SRLF locations, which are errors
	elif location_code.startswith('sr'):
		oclc_symbol = None
		print(f'Unexpected location code: {location_code}')
		print(f'{lhr}')
	else:
		oclc_symbol = 'CLU'
	return oclc_symbol


def update_852_field(lhr):
	# Adds OCLC symbol to 852 $a, removes 852 $b (Alma Library),
	# and moves 852 $c (location code) to 852 $b for WEST compliance.

	# pymarc subfield handling is awkward, but we can trust/assume that the Alma 852
	# starts with no $a, one $b, and one $c.
	
	# Raw update, setting 852 $b to 852 $c.
	lhr['852']['b'] = lhr['852']['c']
	
	oclc_symbol = get_oclc_symbol(lhr)
	# Get the current 852 as a field.	
	f852 = lhr.get_fields('852')[0]
	# Insert 852 $a at start of field
	f852.add_subfield('a', oclc_symbol, 0)
	# Finally, delete original $c
	f852.delete_subfield('c')
	# Field is updated automatically, no need to save it to the record.


def needs_lhr(record):
	# Alma export process is overly broad, so not every exported record needs an LHR.
	# Several things to check.
	# record is bib record, before tag remapping happens.
	holdings_id = record['H52']['8']

	# We only want serials (bib level b, i, or s); no logging needed
	if record.leader[7] not in ['b', 'i', 's']:
		return False
	# Must have a 583 field (exported as H83)
	if record.get_fields('H83') == []:
		print(f'ERROR: No 583 field in holdings {holdings_id}')
		return False
	# Must have an 035 $a starting with (OCoLC)
	# TODO: Refactor duplicate code
	has_oclc = False
	for fld in record.get_fields('035'):
		sfd_a = fld.get_subfields('a')
		if len(sfd_a) > 0 and sfd_a[0].startswith('(OCoLC)'):
			has_oclc = True
	if has_oclc == False:
		print(f'ERROR: No OCLC# in bib {get_bib_id(record)}')
		return False

	# Passed all the tests
	return True


def save_lhr(lhr):
	oclc_symbol = get_oclc_symbol(lhr)
	if oclc_symbol is not None:
		output_file = get_output_filename(oclc_symbol)
		writer = MARCWriter(open(output_file, 'ab'))
		writer.write(lhr)
		writer.close()


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-f', '--marc_file', help='MARC bib file to process', required=True)
	args = parser.parse_args()

	try:
		reader = MARCReader(open(args.marc_file, 'rb')) #, utf8_handling="ignore")
		for record in reader:
			if needs_lhr(record):
				lhr = create_lhr(record)
				save_lhr(lhr)
	except Exception as ex:
		traceback.print_exc()
	finally:
		reader.close()

if __name__ == '__main__':
	main()
