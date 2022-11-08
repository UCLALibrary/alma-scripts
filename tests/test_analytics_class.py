from alma_analytics_client import AlmaAnalyticsClient
from alma_api_keys import API_KEYS
from pprint import pprint


def main():
    aac = AlmaAnalyticsClient(API_KEYS["DIIT_ANALYTICS"])
    report_path = (
        "/shared/University of California Los Angeles (UCLA) 01UCS_LAL"
        "/Cataloging/Reports/API/Location API Test"
    )
    aac.set_report_path(report_path)
    print("\nTesting 500 rows per iteration, full report has 600+ rows")
    aac.set_rows_per_fetch(500)
    report = aac.get_report()
    # pprint(report)
    print(f"Rows: {len(report)}")
    # pprint(report[0].keys())

    print("\nTesting LIKE filter and instance reuse")
    aac.set_filter_like("Location", "Location Name", "%Belt%")
    report = aac.get_report()
    pprint(report)
    print(f"Rows: {len(report)}")

    print("\nTesting EQUAL filter")
    aac.set_filter_equal("Location", "Location Code", "lw")
    report = aac.get_report()
    pprint(report)
    print(f"Rows: {len(report)}")


if __name__ == "__main__":
    main()
