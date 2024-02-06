import csv
import requests
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime

# Database configuration - currently using pgsql Docker container
DATABASE = "fpds_raw"
USER = "postgres"  # replace with your username/env variable
PASSWORD = "default"  # replace with your password/env variable
HOST = "0.0.0.0"
PORT = "5432"

# FPDS ATOM feed base URL
ATOM_FEED_BASE_URL = "https://www.fpds.gov/ezsearch/FEEDS/ATOM"

# award sample: https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=LAST_MOD_DATE:[2023/03/21,2023/03/21]
# IDV sample: https://www.fpds.gov/ezsearch/FEEDS/ATOM?s=FPDS&FEEDNAME=PUBLIC&VERSION=1.5.3&q=PIID%3AW31P4Q08D0006


def fetch_fpds_data(start_date, end_date, url=None):
    if not url:
        # Construct the query URL for the first call
        # Example: &LAST_MOD_DATE:[2018-04-01,2018-04-30]
        query_param = f"LAST_MOD_DATE:[{start_date},{end_date}]"
        url = f"{ATOM_FEED_BASE_URL}?FEEDNAME=PUBLIC&q={query_param}"
    print("Fetching URL:", url)

    response = requests.get(url)
    return response.text


def parse_xml(xml_data, ns={'atom': 'http://www.w3.org/2005/Atom', 'ns1': 'https://www.fpds.gov/FPDS'}):
    root = ET.fromstring(xml_data)
    entries = root.findall('atom:entry', ns)
    # test url: https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=LAST_MOD_DATE:[2023-11-01,2023-11-02]

    records = []
    for entry in entries:
        # Extract data from each entry
        # Example fields - replace with actual FPDS feed fields
        title = entry.find('atom:title', ns).text
        print("Title: " + str(title))
        modified = entry.find('atom:modified', ns).text
        print("Modified: " + str(modified))

        PIID = entry.find('.//ns1:PIID', ns)
        PIID_text = PIID.text if PIID is not None else 'Not Available'
        print("PIID: " + str(PIID_text))


        UEI = entry.find('.//ns1:UEI', ns).text
        UEILegalBusinessName = entry.find('.//ns1:UEILegalBusinessName', ns).text
        ultimateParentUEI = entry.find('.//ns1:ultimateParentUEI', ns).text
        ultimateParentUEIName = entry.find('.//ns1:ultimateParentUEIName', ns).text
        obligatedAmount = entry.find('.//ns1:obligatedAmount', ns).text
        baseAndExercisedOptionsElement = entry.find('.//ns1:baseAndExercisedOptionsValue', ns)
        baseAndExercisedOptionsValue = baseAndExercisedOptionsElement.text if baseAndExercisedOptionsElement \
                                                                              is not None else 'Not Available'
        baseAndAllOptionsValue = entry.find('.//ns1:baseAndAllOptionsValue', ns).text
        signedDate = entry.find('.//ns1:signedDate', ns).text
        currentCompletionElement = entry.find('.//ns1:currentCompletionDate', ns)
        currentCompletionDate = currentCompletionElement.text if currentCompletionElement is not None else \
            'Not Available'
        fundingRequestingDepartmentID = entry.find('.//ns1:fundingRequestingAgencyID', ns).get('departmentID')
        fundingRequestingDepartmentName = entry.find('.//ns1:fundingRequestingAgencyID', ns).get('departmentName')
        fundingRequestingAgencyID = entry.find('.//ns1:fundingRequestingAgencyID', ns).text
        fundingRequestingAgencyName = entry.find('.//ns1:fundingRequestingAgencyID', ns).get('name')
        fundingRequestingOfficeID = entry.find('.//ns1:fundingRequestingOfficeID', ns).text
        fundingRequestingOfficeName = entry.find('.//ns1:fundingRequestingOfficeID', ns).get('name')
        contractingOfficeAgencyID = entry.find('.//ns1:contractingOfficeAgencyID', ns).text
        contractingOfficeID = entry.find('.//ns1:contractingOfficeID', ns).text
        createdBy = entry.find('.//ns1:createdBy', ns).text
        createdDate = entry.find('.//ns1:createdBy', ns).text
        lastModifiedBy = entry.find('.//ns1:createdBy', ns).text
        lastModifiedDate = entry.find('.//ns1:createdBy', ns).text
        approvedBy = entry.find('.//ns1:createdBy', ns).text
        approvedDate = entry.find('.//ns1:createdBy', ns).text
        closedBy = entry.find('.//ns1:createdBy', ns).text
        closedDate = entry.find('.//ns1:createdBy', ns).text
        # ... continue for other elements as per the FPDS feed

        record = (title, modified, PIID, UEI, UEILegalBusinessName, ultimateParentUEI, ultimateParentUEIName,
                  obligatedAmount, baseAndExercisedOptionsValue, baseAndAllOptionsValue, signedDate,
                  currentCompletionDate, fundingRequestingDepartmentID, fundingRequestingDepartmentName,
                  fundingRequestingAgencyID, fundingRequestingAgencyName, fundingRequestingOfficeID,
                  fundingRequestingOfficeName, contractingOfficeAgencyID, contractingOfficeID, createdBy, createdDate,
                  lastModifiedBy, lastModifiedDate, approvedBy, approvedDate, closedBy, closedDate)
        print(record)
        records.append(record)

        # Check for a next link for pagination
        next_link = root.find(".//atom:link[@rel='next']", ns)
        if next_link is not None:
            next_url = next_link.get('href')
            if next_url:
                # Fetch the next page of data
                next_page_data = fetch_fpds_data(None, None, url=next_url)
                # Parse the next page and extend the records list
                records.extend(parse_xml(next_page_data, ns))

        return records


def output_csv(records, filename="fpds_data.csv"):
    # Define the header based on the fields you are extracting from the XML
    headers = ['Title', 'Modified', 'PIID', 'UEI', 'UEILegalBusinessName', 'UltimateParentUEI', 'UltimateParentUEIName',
               'ObligatedAmount', 'BaseAndExercisedOptionsValue', 'BaseAndAllOptionsValue', 'SignedDate',
               'CurrentCompletionDate', 'FundingRequestingDepartmentID', 'FundingRequestingDepartmentName',
               'FundingRequestingAgencyID', 'FundingRequestingAgencyName', 'FundingRequestingOfficeID',
               'FundingRequestingOfficeName', 'ContractingOfficeAgencyID', 'ContractingOfficeID', 'CreatedBy', 'CreatedDate',
               'LastModifiedBy', 'LastModifiedDate', 'ApprovedBy', 'ApprovedDate', 'ClosedBy', 'ClosedDate']

    # Open the file in write mode
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(headers)

        # Write the records
        for record in records:
            writer.writerow(record)

    print(f"Data exported to {filename} successfully.")


def insert_into_db(records):
    try:
        conn = psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
        cur = conn.cursor()

        for record in records:
            cur.execute("INSERT INTO fpds_all (title, updated, contract_id, vendor_name, contract_amount,"
                        " start_date, end_date) VALUES (%s, %s, %s, %s, %s, %s, %s)", record)

        conn.commit()
        cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn is not None:
            conn.close()


def main():
    start_date = datetime(2023, 11, 1).strftime('%Y-%m-%d')
    end_date = datetime(2023, 11, 2).strftime('%Y-%m-%d')

    xml_data = fetch_fpds_data(start_date, end_date)
    records = parse_xml(xml_data)
    output_csv(records, 'fpds_data.csv')
    # insert_into_db(records)  # enable to insert into postgres

    print('Job complete.')

if __name__ == "__main__":
    main()
