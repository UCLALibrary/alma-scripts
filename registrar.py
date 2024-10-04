import pymssql
from datetime import datetime
from database_credentials import REGISTRAR


def get_terms(use_date=datetime.today()):
    # Returns a tuple of terms (quarters) based on the current month.
    # Some overlap due to Law using semesters.
    # Format: 2-digit year followed by one of these:
    # 	'W' (winter), 'S' (spring), '1' (summer), 'F' (fall).
    current_year = use_date.year
    current_month = use_date.month  # integer
    if current_month == 12:
        current_year += 1

    if current_month in (12, 1, 2):
        # Winter (all) and Spring, for Law
        term_codes = ("W", "S")
    elif current_month in (3, 4):
        # Spring only
        term_codes = ("S", "S")
    elif current_month in (5, 6):
        # Spring and Summer
        term_codes = ("S", "1")
    elif current_month in (7, 8, 9):
        # Summer (all) and Fall, for Law
        # term_codes = ('1', 'F')
        # term_codes = ('1', '1')
        term_codes = ("F", "F")
    elif current_month in (10, 11):
        # Fall only
        term_codes = ("F", "F")

    # Prepend 2-digit year to codes and return as a tuple
    return tuple([str(current_year)[2:4] + code for code in term_codes])


def main():
    server = REGISTRAR["server"]
    database = REGISTRAR["database"]
    username = REGISTRAR["username"]
    password = REGISTRAR["password"]
    stored_procedure = REGISTRAR["stored_procedure"]
    terms = get_terms()

    conn = pymssql.connect(server, username, password, database)
    cursor = conn.cursor(as_dict=True)

    cursor.callproc(stored_procedure, terms)

    # https://github.com/pymssql/pymssql/pull/134
    # 20240506: Apparently no longer necessary, though issue is still open...
    # cursor.nextset()

    for row in cursor:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()
