#!/usr/bin/env -S python3 -u
import json
import xmltodict
import pprint as pp

from alma_api_keys import API_KEYS
from alma_api_client import Alma_Api_Client


def get_real_column_names(report_json):
    # Column names are buried in metadata
    # Get dictionary of column info
    # This seems to be available only on initial run
    # (first set of data, not subsequent ones),
    # even if col_names = true parameter is always passed to API.
    column_names = {}
    try:
        column_info = report_json["ResultXml"]["rowset"]["xsd:schema"][
            "xsd:complexType"
        ]["xsd:sequence"]["xsd:element"]
        # Create mapping of generic column names (Column0 etc.) to real column names
        for row in column_info:
            generic_name = row["@name"]
            real_name = row["@saw-sql:columnHeading"]
            column_names[generic_name] = real_name
    except KeyError:
        # OK to swallow this error
        pass
    return column_names


def get_filter(yyyymm):
    # By cat center: too slow for RAMS (17+ minutes, 300+ MB data)
    # 	filter_xml = f'''
    # <sawx:expr xsi:type="sawx:list" op="containsAll"
    # 	xmlns:saw="com.siebel.analytics.web/report/v1.1"
    # 	xmlns:sawx="com.siebel.analytics.web/expression/v1.1"
    # 	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    # 	xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    # >
    # 	<sawx:expr xsi:type="sawx:sqlExpression">LOWER("Bibliographic Details"."Local Param 02")</sawx:expr>
    # 	<sawx:expr xsi:type="xsd:string">$$a {cat_center}</sawx:expr>
    # </sawx:expr>
    # '''

    # By year/month: quick enough, usually 5000-10000 rows
    # No need for LOWER with just digits
    filter_xml = f"""
<sawx:expr xsi:type="sawx:list" op="like" 
	xmlns:saw="com.siebel.analytics.web/report/v1.1" 
	xmlns:sawx="com.siebel.analytics.web/expression/v1.1" 
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
	xmlns:xsd="http://www.w3.org/2001/XMLSchema"
>
	<sawx:expr xsi:type="sawx:sqlExpression">"Bibliographic Details"."Local Param 02"</sawx:expr>
	<sawx:expr xsi:type="xsd:string">%$$c {yyyymm}%</sawx:expr>
</sawx:expr>
"""

    # Boolean OR not working?
    # 	filter_xml = f'''
    # <sawx:expr xsi:type="sawx:logical" op="or"
    # 	xmlns:saw="com.siebel.analytics.web/report/v1.1"
    # 	xmlns:sawx="com.siebel.analytics.web/expression/v1.1"
    # 	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    # 	xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    # >
    # 	<sawx:expr xsi:type="sawx:list" op="like">
    # 	<sawx:expr xsi:type="sawx:sqlExpression">"Bibliographic Details"."Local Param 02"</sawx:expr>
    # 	<sawx:expr xsi:type="xsd:string">%$$c 202001%</sawx:expr></sawx:expr>
    # 	<sawx:expr xsi:type="sawx:list" op="like">
    # 	<sawx:expr xsi:type="sawx:sqlExpression">"Bibliographic Details"."Local Param 02"</sawx:expr>
    # 	<sawx:expr xsi:type="xsd:string">%$$c 202002%</sawx:expr></sawx:expr>
    # </sawx:expr>
    # '''
    # Strip out formatting characters which make API unhappy
    return filter_xml.replace("\n", "").replace("\t", "")


def get_report_data(report):
    # Report available only in XML
    # Entire XML report is a "list" with one value, in 'anies' element of json response
    xml = report["anies"][0]
    # Convert xml to python dict intermediate format
    xml_dict = xmltodict.parse(xml)
    # Convert this to real json
    report_json = json.loads(json.dumps(xml_dict))
    # Everything is in QueryResult dict
    report_json = report_json["QueryResult"]

    # Actual rows of data are a list of dictionaries, in this dictionary
    rows = report_json["ResultXml"]["rowset"]["Row"]

    # Clean up
    report_data = {
        "rows": rows,
        "column_names": get_real_column_names(report_json),
        "is_finished": report_json["IsFinished"],  # should always exist
        "resumption_token": report_json.get("ResumptionToken"),  # may not exist
    }

    return report_data


def run_report():
    alma = Alma_Api_Client(API_KEYS["DIIT_ANALYTICS"])
    report_path = "/shared/University of California Los Angeles (UCLA) 01UCS_LAL/Cataloging/Reports/API/Cataloging Statistics (API)"
    # From form
    yyyymm = "20220406"
    filter_xml = get_filter(yyyymm)

    # No need to URL-encode anything,
    # since requests library does that automatically
    constant_params = {
        "col_names": "true",
        "limit": 1000,  # valid values: 25 to 1000, best as multiple of 25
    }
    initial_params = {
        "path": report_path,
        "filter": filter_xml,
    }
    # First run: use constant + initial parameters merged
    report = alma.get_analytics_report(constant_params | initial_params)
    # pp.pprint(report)
    report_data = get_report_data(report)
    all_rows = report_data["rows"]
    # Preserve column_names as they don't seem to be set on subsequent runs
    column_names = report_data["column_names"]

    # Use the token from first run in all subsequent ones
    subsequent_params = {
        "token": report_data["resumption_token"],
    }

    while report_data["is_finished"] == "false":
        # After first run: use constant = subsequent parameters merged
        report = alma.get_analytics_report(constant_params | subsequent_params)
        report_data = get_report_data(report)
        all_rows.extend(report_data["rows"])

    return {"column_names": column_names, "rows": all_rows}


def expand_data(report_data):
    # Analytics data has 1 row per bib record;
    # multiple 962 fields are combined in ...
    data = []
    column_names = report_data["column_names"]
    pp.pprint(column_names)
    rows = report_data["rows"]
    for row in rows:
        # Update keys to use real column names, removing meaningless Column0
        new_row = dict(
            [(column_names.get(k), v) for k, v in row.items() if k != "Column0"]
        )
        # more transforms...
        data.append(new_row)
    return data


def main():
    report_data = run_report()
    # pp.pprint(report_data)
    pp.pprint(f"{len(report_data['rows']) = }")
    pp.pprint(expand_data(report_data))


if __name__ == "__main__":
    main()
