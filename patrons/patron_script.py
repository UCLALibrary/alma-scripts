import ast
import csv
import sys
import pprint as pp
from xml.sax.saxutils import escape

# Long XML formatting routines for this project
import patron_xml_template as patron_xml
# experiment
from collections import Counter

def get_registrar_data():
	registrar_data = {}

	with open('registrar_data.txt') as f:
		lines = f.readlines()
		for line in lines:
			# Each line represents data for one student.
			# Dictionary from SQL query, not json, not pickle-compatible.
			student = ast.literal_eval(line)
			# Registrar data is space-padded; get rid of those extra spaces
			for key, val in student.items():
				student[key] = val.strip()

			# Most data is not relevant; get just what's needed, renaming for consistency with non-student data
			# Only need CAREER[1], DIVISION[2], DEPT[7], DEGREE[3], CLASS[3]
			# CAREER_REAL and HONORS_REAL included while porting from obsolete Voyager StudentLoader.
			primary_id = student['STU_ID']
			patron = {
				'PRIMARY_ID': primary_id,
				'FULL_NAME': escape(student['STU_NM']),
				'CAREER_REAL': student['CAREER'],
				'CAREER': student['CAREER'][0],
				'CLASS': student['CLASS'],
				'DEGREE': student['DEG_CD'],
				'DEPT': student['SR_DEPT_CD'],
				'DIVISION': student['SR_DIV_CD'],
				'EMAIL_ADDRESS': escape(student['SS_EMAIL_ADDR']),
				'HONORS_REAL': student['HONORS'],
				'HONORS': student['SP_PGM2'],
			}
			# Registrar has address data which needs remapping for Alma
			patron.update(get_student_address(student))
			# Registrar has just 'UNITED' for USA; change to USA
			patron['ADDRESS_COUNTRY'] = 'USA' if patron['ADDRESS_COUNTRY'] == 'UNITED' else patron['ADDRESS_COUNTRY']
			# Registrar provides just full name; add name parts
			patron.update(split_patron_name(patron['FULL_NAME']))
			# User group
			patron['USER_GROUP'] = get_user_group(patron)

			registrar_data[primary_id] = patron
	return registrar_data

def get_bruincard_data():
	bruincard_data = {}
	with open('bruincard_data.txt') as f:
		# CSV file, mostly... multiple types of record, with different number of fields.
		# Some are quoted, some not.
		# Keep only rows with 'Active' in 4th field.
		for line in csv.reader(f):
			ucla_uid = line[1]
			barcode = ucla_uid + line[0]
			if line[3] == 'Active':
				bc_existing = bruincard_data.get(ucla_uid, '0')
				if barcode > bc_existing:
					bruincard_data[ucla_uid] = barcode
	return bruincard_data

def get_student_address(student):
	# Registrar provides 2 addresses / phones, local (M_) and permanent (P_).
	# Most students have only local data released for use.
	# If student has local address line 1, use local; else use permanent.
	if student['M_STREET_1'] != '':
		address = {
			'ADDRESS_LINE1': student['M_STREET_1'],
			'ADDRESS_LINE2': student['M_STREET_2'],
			'ADDRESS_CITY': student['M_CITY_NAME'],
			# Registrar has these separate; take the first that exists
			'ADDRESS_STATE_PROVINCE': student['M_STATE_CD'] if student['M_STATE_CD'] != '' else student['M_PROV_CD'],
			'ADDRESS_POSTAL_CODE': student['M_ZIP_CD'],
			'ADDRESS_COUNTRY': student['M_CNTRY7'],
			'PHONE_NUMBER': student['M_PHONE_NO'],
		}
	else:
		address = {
			'ADDRESS_LINE1': student['P_STREET_1'],
			'ADDRESS_LINE2': student['P_STREET_2'],
			'ADDRESS_CITY': student['P_CITY_NAME'],
			# Registrar has these separate; take the first that exists
			'ADDRESS_STATE_PROVINCE': student['P_STATE_CD'] if student['P_STATE_CD'] != '' else student['P_PROV_CD'],
			'ADDRESS_POSTAL_CODE': student['P_ZIP_CD'],
			'ADDRESS_COUNTRY': student['P_CNTRY7'],
			'PHONE_NUMBER': student['P_PHONE_NO'],
		}
	# Addresses can have unsafe-for-xml characters
	for key, val in address.items():
		address[key] = escape(val)

	return address

def split_patron_name(full_name):
	"""
		Splits combined 'LAST, FIRST MIDDLE...' into 3 values in a dictionary.
		MIDDLE may contain multiple terms in one string;
		FIRST and LAST will be single terms
	"""
	# Registrar data is usually 'LAST, FIRST MIDDLE' - 'comma space', but may be multiple commas. May also be FIRST..LAST.... asking reg for better data.
	(last_name, separator, other_names) = full_name.partition(', ')
	if last_name == full_name:
		# No 'comma space'; assume name is FIRST MIDDLE(s) LAST or FIRST LAST.
		names = full_name.split(' ')
		first_name = names[0]
		last_name = names[-1]
		if len(names) >= 3:
			middle_name = ' '.join(names[1:len(names)-1])
		else:
			middle_name = ''
	else:
		(first_name, separator, middle_name) = other_names.partition(' ')
	names = {
		'FIRST_NAME': first_name,
		'MIDDLE_NAME': middle_name,
		'LAST_NAME': last_name
	}
	return names

def get_user_group(patron):
	# Currently handles students only
	# TODO: Add non-students
	# Order of evaluation matters!

	# Music grads: dept values have changed... 1st 4 legacy, no data; last 3 have data now
	if patron['CAREER'] in ('G', 'M', 'D') and patron['DEPT'] in ('MUSCLGY', 'MUSIC', 'ETHNOMUS', 'ETHNOMU', 'ETHNMUS', 'MUSC', 'MUSCLG'):
		user_group = 'UGMU'
	# Management grads
	elif patron['CAREER'] in ('G', 'M', 'D') and patron['DEGREE'] == 'PHD' and patron['DIVISION'] == 'MG':
		user_group = 'UGM'
	# Law grads
	elif patron['CAREER'] == 'L' or patron['DEGREE'] in ('JD', 'LLM', 'MLS'):
		user_group = 'UGL'
	# Regular grads
	elif patron['CAREER'] in ('G', 'M', 'D'):
		user_group = 'UG'
	# Music undergrads: dept values have changed... 1st 4 legacy, no data; last 3 have data now
	elif patron['CAREER'] in ('U', 'I', 'J') and patron['DEPT'] in ('MUSCLGY', 'MUSIC', 'ETHNOMUS', 'ETHNOMU', 'ETHNMUS', 'MUSC', 'MUSCLG'):
		user_group = 'UUMU'
	# Honors undergrads get treated like grads
	elif patron['CAREER'] in ('U', 'I', 'J') and patron['HONORS'] == 'Y':
		user_group = 'UG'
	# Regular undergrads
	elif patron['CAREER'] in ('U', 'I', 'J'):
		user_group = 'UU'
	# TODO: Handle post-docs
	# Unknown / errors, so set a value which will make Alma reject the record
	else:
		user_group = 'UNKNOWN'

	return user_group

def main():
	xml_file = sys.argv[1]
	
	# Students only, for now
	registrar_data = get_registrar_data()
	bruincard_data = get_bruincard_data()

	# Registrar data is the primary source for students.
	# Add barcode from bruincard data, if it exists.
	for ucla_uid in registrar_data.keys():
		if ucla_uid in bruincard_data:
			registrar_data[ucla_uid]['BARCODE'] = bruincard_data.get(ucla_uid)
		else:
			print(f'No barcode found for {ucla_uid}')
			registrar_data[ucla_uid]['BARCODE'] = None

	# 505433166: In registrar, not in bruincard [FIXED 20220328]
	#pp.pprint(registrar_data['505433166'])
	# 905879848: In registrar and in bruincard
	#pp.pprint(registrar_data['905879848'])
	# 205725086 has only perm address
	#pp.pprint(registrar_data['205725086'])
	# 000429412 has P/PR career/class... via lib_resident SP called by java registrar program
	#pp.pprint(registrar_data['000429412'])
	# 005136507 has honors... not via HONORS but via SP_PGM2...
	#pp.pprint(registrar_data['005136507'])

	#values = [registrar_data[uid]['DEPT'] for uid in registrar_data]
	#pp.pprint(Counter(values))

	#print(len(registrar_data))

	# Supposedly post-docs; waiting for clarification from SAIT and UAS
	# p_pr = ['000429412', '003248116', '104235235', '105271608', '105091438', '105263123', '105263420', '105469746', '203150811', '105648748', '205291328', '203980437', '205465597', '205542893', '205429316', '305078110', '205868754', '302651093', '304307113', '304375625', '305461594', '305648733', '404819189', '404885264', '405063660', '405850772', '504544279', '505053854', '505065079', '505628282', '505458128', '604096784', '604947893', '605262814', '604389254', '605461738', '605648661', '605649910', '703761135', '704307093', '605868733', '705461733', '705648627', '705648665', '705648750', '804691650', '803678522', '804819795', '804883692', '804947991', '705849150', '804383920', '805648735', '904819498', '904374802', '904375024', '905260465']
	# for ucla_uid in registrar_data.keys():
	# 	if ucla_uid in p_pr:
	# 		pp.pprint(registrar_data[ucla_uid])

	patron_xml.write_xml(registrar_data, xml_file)

if __name__ == '__main__':
	main()