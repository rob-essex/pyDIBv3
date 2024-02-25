import concurrent.futures
import csv
import requests
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime

# Database configuration - currently using pgsql Docker container
DATABASE = "fpds"
USER = "postgres"  # replace with your username/env variable
PASSWORD = "fibo112358"  # replace with your password/env variable
HOST = "0.0.0.0"
PORT = "5432"

# FPDS ATOM feed base URL
ATOM_FEED_BASE_URL = "https://www.fpds.gov/ezsearch/FEEDS/ATOM"

# award sample: https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=LAST_MOD_DATE:[2023/03/21,2023/03/21]
# IDV sample: https://www.fpds.gov/ezsearch/FEEDS/ATOM?s=FPDS&FEEDNAME=PUBLIC&VERSION=1.5.3&q=PIID%3AW31P4Q08D0006


def get_element_text(entry, element_name, ns, default=''):
    """
    Retrieves the text value of an XML element.

    Args:
        entry: The XML entry to search within.
        element_name: The tag name of the element to find.
        ns: The namespace dictionary.
        default: The default value to return if the element is not found or has no text.

    Returns:
        The text of the found element, or the default value if not found.
    """
    # Correctly format the namespace and element name for the search
    element = entry.find('.//{{{}}}{}'.format(ns['ns1'], element_name), ns)
    return element.text if element is not None else default


def get_element_attribute(entry, element_name, attribute_name, ns, default=''):
    """
    Retrieves the value of an attribute from an XML element.

    Args:
        entry: The XML entry to search within.
        element_name: The tag name of the element to find.
        attribute_name: The name of the attribute to retrieve.
        ns: The namespace dictionary.
        default: The default value to return if the element or attribute is not found.

    Returns:
        The value of the attribute, or the default value if the element or attribute is not found.
    """
    # Find the element using the provided namespace and element name
    element = entry.find('.//{{{}}}{}'.format(ns['ns1'], element_name), ns)
    # Return the attribute value if the element is found and the attribute exists, else return default
    return element.get(attribute_name) if element is not None and element.get(attribute_name) is not None else default


def get_nested_element(entry, parent_element_name, child_element_name, ns, default=''):
    """
    Retrieves the text of a nested XML element.

    Args:
        entry: The XML entry to search within.
        parent_element_name: The tag name of the parent element.
        child_element_name: The tag name of the child element to find within the parent.
        ns: The namespace dictionary.
        default: The default value to return if the element is not found or has no text.

    Returns:
        The text of the found child element, or the default value if not found.
    """
    # Find the parent element
    parent_element = entry.find('.//{{{}}}{}'.format(ns['ns1'], parent_element_name), ns)

    if parent_element is not None:
        # Find the child element within the parent
        child_element = parent_element.find('.//{{{}}}{}'.format(ns['ns1'], child_element_name), ns)
        return child_element.text if child_element is not None else default
    else:
        return default


def get_nested_attribute(entry, parent_element_name, child_element_name, attribute_name, ns, default=''):
    """
    Retrieves the value of an attribute from a nested XML element.

    Args:
        entry: The XML entry to search within.
        parent_element_name: The tag name of the parent element.
        child_element_name: The tag name of the child element to find within the parent.
        attribute_name: The name of the attribute within the child element whose value is to be returned.
        ns: The namespace dictionary.
        default: The default value to return if the element or attribute is not found.

    Returns:
        The value of the specified attribute, or the default value if not found.
    """
    # Find the parent element
    parent_element = entry.find('.//{{{}}}{}'.format(ns['ns1'], parent_element_name), ns)

    if parent_element is not None:
        # Find the child element within the parent
        child_element = parent_element.find('.//{{{}}}{}'.format(ns['ns1'], child_element_name), ns)
        if child_element is not None and attribute_name in child_element.attrib:
            return child_element.attrib[attribute_name]
    return default


def fetch_fpds_data(start_date, end_date, funding_agency_ID, naics, url=None):
    if not url:
        # Construct the query URL for the first call
        # Example: &LAST_MOD_DATE:[2018-04-01,2018-04-30]

        # date_query_param = f"+LAST_MOD_DATE:[{start_date},{end_date}]"  Testing build function

        if start_date is None or end_date is None:
            date_query_param = ''
        else:
            date_query_param = f"+LAST_MOD_DATE:[{start_date},{end_date}]"

        if funding_agency_ID is None:
            agency_query_param = ''
        else:
            agency_query_param = f"+FUNDING_AGENCY_ID:\"{funding_agency_ID}\""

        if naics is None:
            NAICS_query_param = ''
        else:
            NAICS_query_param = f"+PRINCIPAL_NAICS_CODE:\"{naics}\""

        url = f"{ATOM_FEED_BASE_URL}?FEEDNAME=PUBLIC&q={date_query_param}{agency_query_param}{NAICS_query_param}"

    # test url https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q=LAST_MOD_DATE:[2016-01-01,2023-12-31]+ULTIMATE_UEI:"W6ZWNL4GWP97"
    print("Fetching URL:", url)

    response = requests.get(url)
    return response.text


def parse_xml(xml_data, ns={'atom': 'http://www.w3.org/2005/Atom', 'ns1': 'https://www.fpds.gov/FPDS'}):
    # Query structure available at https://www.fpds.gov/wiki/index.php/Atom_Feed_Usage
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

        # PIID = get_element_text(entry, 'PIID', ns) legacy
        """
        PIID = get_nested_element(entry, 'awardContractID', 'PIID', ns) if not None else \
            get_nested_element(entry, 'IDVID', 'PIID', ns)
        modNumber = get_nested_element(entry, 'awardContractID', 'modNumber', ns) if not None else\
            get_nested_element(entry, 'IDVID', 'modNumber', ns)
        """
        referencedIDVPIID = get_nested_element(entry, 'referencedIDVID', 'PIID', ns)
        IDVModNumber = get_nested_element(entry, 'referencedIDVID', 'modNumber', ns)

        # Try to get the PIID from 'awardContractID' first
        PIID = get_nested_element(entry, 'awardContractID', 'PIID', ns)
        # If it wasn't found, try to get it from 'IDVID'
        if PIID == '':
            PIID = get_nested_element(entry, 'IDVID', 'PIID', ns)

        # Try to get the modNumber from 'awardContractID' first
        modNumber = get_nested_element(entry, 'awardContractID', 'modNumber', ns)
        # If it wasn't found, try to get it from 'IDVID'
        if modNumber == '':
            modNumber = get_nested_element(entry, 'IDVID', 'modNumber', ns)

        print(f"PIID:{PIID}  Ref IDV: {referencedIDVPIID}")

        UEI = get_element_text(entry, 'UEI', ns)
        UEILegalBusinessName = get_element_text(entry, 'UEILegalBusinessName', ns)
        immediateParentUEI = get_element_text(entry, 'immediateParentUEI', ns)
        immediateParentUEIName = get_element_text(entry, 'immediateParentUEIName', ns)
        domesticParentUEI = get_element_text(entry, 'domesticParentUEI', ns)
        domesticParentUEIName = get_element_text(entry, 'domesticParentUEIName', ns)
        ultimateParentUEI = get_element_text(entry, 'ultimateParentUEI', ns)
        ultimateParentUEIName = get_element_text(entry, 'ultimateParentUEIName', ns)
        vendorName = get_element_text(entry, 'vendorName', ns)
        vendorAlternateName = get_element_text(entry, 'vendorAlternateName', ns)
        vendorLegalOrganizationName = get_element_text(entry, 'vendorLegalOrganizationName', ns)
        vendorStreetAddress = get_nested_element(entry, 'vendorLocation', 'streetAddress', ns)
        vendorCity = get_nested_element(entry, 'vendorLocation', 'city', ns)
        vendorState = get_nested_element(entry, 'vendorLocation', 'state', ns)
        vendorZIPCode = get_nested_element(entry, 'vendorLocation', 'ZIPCode', ns)
        vendorCountryCode = get_nested_element(entry, 'vendorLocation', 'countryCode', ns)
        vendorPhoneNo = get_nested_element(entry, 'vendorLocation', 'phoneNo', ns)
        vendorFaxNo = get_nested_element(entry, 'vendorLocation', 'faxNo', ns)
        vendorCongressionalDistrictCode = get_nested_element(entry, 'vendorLocation', 'congressionalDistrictCode', ns)
        vendorEntityDataSource = get_nested_element(entry, 'vendorLocation', 'entityDataSource', ns)
        obligatedAmount = get_element_text(entry, 'obligatedAmount', ns)
        baseAndExercisedOptionsValue = get_element_text(entry, 'baseAndExercisedOptionsValue', ns)
        baseAndAllOptionsValue = get_element_text(entry, 'baseAndAllOptionsValue', ns)
        totalObligatedAmount = get_element_text(entry, 'totalObligatedAmount', ns)
        totalBaseAndExercisedOptionsValue = get_element_text(entry, 'totalBaseAndExercisedOptionsValue', ns)
        totalBaseAndAllOptionsValue = get_element_text(entry, 'totalBaseAndAllOptionsValue', ns)
        signedDate = get_element_text(entry, 'signedDate', ns)
        effectiveDate = get_element_text(entry, 'effectiveDate', ns)
        currentCompletionDate = get_element_text(entry, 'currentCompletionDate', ns)
        ultimateCompletionDate = get_element_text(entry, 'ultimateCompletionDate', ns)
        fundingRequestingDepartmentID = get_element_attribute(entry, 'fundingRequestingAgencyID', 'departmentID', ns)
        fundingRequestingDepartmentName = get_element_attribute(entry, 'fundingRequestingAgencyID', 'departmentName', ns)
        fundingRequestingAgencyID = get_element_text(entry, 'fundingRequestingAgencyID', ns)
        fundingRequestingAgencyName = get_element_attribute(entry, 'fundingRequestingAgencyID', 'name', ns)
        fundingRequestingOfficeID = get_element_text(entry, 'fundingRequestingOfficeID', ns)
        fundingRequestingOfficeName = get_element_attribute(entry, 'fundingRequestingOfficeID', 'name', ns)
        contractingOfficeAgencyID = get_element_text(entry, 'contractingOfficeAgencyID', ns)
        contractingOfficeID = get_element_text(entry, 'contractingOfficeID', ns)
        principalNAICSCode = get_element_text(entry, 'principalNAICSCode', ns)
        principalNAICSCodeDescription = get_element_attribute(entry, 'principalNAICSCode', 'description', ns)
        productOrServiceCode = get_element_text(entry, 'productOrServiceCode', ns)
        productOrServiceCodeDescription = get_element_attribute(entry, 'productOrServiceCode', 'description', ns)
        productOrServiceCodeType = get_element_attribute(entry, 'productOrServiceCode', 'productOrServiceType', ns)
        descriptionOfContractRequirement = get_element_text(entry, 'descriptionOfContractRequirement', ns)
        reasonForModification = get_element_text(entry, 'reasonForModification', ns)
        reasonForModificationDescription = get_element_attribute(entry, 'reasonForModification', 'description', ns)
        createdBy = get_element_text(entry, 'createdBy', ns)
        createdDate = get_element_text(entry, 'createdDate', ns)
        lastModifiedBy = get_element_text(entry, 'lastModifiedBy', ns)
        lastModifiedDate = get_element_text(entry, 'lastModifiedDate', ns)
        approvedBy = get_element_text(entry, 'approvedBy', ns)
        approvedDate = get_element_text(entry, 'approvedDate', ns)
        closedBy = get_element_text(entry, 'closedBy', ns)
        closedDate = get_element_text(entry, 'closedDate', ns)
        # ... continue for other elements as per the FPDS feed

        record = {
            'title': title,
            'modified': modified,
            'PIID': PIID,
            'modNumber': modNumber,
            'referencedIDVPIID': referencedIDVPIID,
            'IDVModNumber': IDVModNumber,
            'UEI': UEI,
            'UEILegalBusinessName': UEILegalBusinessName,
            'immediateParentUEI': immediateParentUEI,
            'immediateParentUEIName': immediateParentUEIName,
            'domesticParentUEI': domesticParentUEI,
            'domesticParentUEIName': domesticParentUEIName,
            'ultimateParentUEI': ultimateParentUEI,
            'ultimateParentUEIName': ultimateParentUEIName,
            'vendorName': vendorName,
            'vendorAlternateName': vendorAlternateName,
            'vendorLegalOrganizationName': vendorLegalOrganizationName,
            'vendorStreetAddress': vendorStreetAddress,
            'vendorCity': vendorCity,
            'vendorState': vendorState,
            'vendorZIPCode': vendorZIPCode,
            'vendorCountryCode': vendorCountryCode,
            'vendorPhoneNo': vendorPhoneNo,
            'vendorFaxNo': vendorFaxNo,
            'vendorCongressionalDistrictCode': vendorCongressionalDistrictCode,
            'vendorEntityDataSource': vendorEntityDataSource,
            'obligatedAmount': obligatedAmount,
            'baseAndExercisedOptionsValue': baseAndExercisedOptionsValue,
            'baseAndAllOptionsValue': baseAndAllOptionsValue,
            'totalObligatedAmount': totalObligatedAmount,
            'totalBaseAndExercisedOptionsValue': totalBaseAndExercisedOptionsValue,
            'totalBaseAndAllOptionsValue': totalBaseAndAllOptionsValue,
            'signedDate': signedDate,
            'effectiveDate': effectiveDate,
            'currentCompletionDate': currentCompletionDate,
            'ultimateCompletionDate': ultimateCompletionDate,
            'fundingRequestingDepartmentID': fundingRequestingDepartmentID,
            'fundingRequestingDepartmentName': fundingRequestingDepartmentName,
            'fundingRequestingAgencyID': fundingRequestingAgencyID,
            'fundingRequestingAgencyName': fundingRequestingAgencyName,
            'fundingRequestingOfficeID': fundingRequestingOfficeID,
            'fundingRequestingOfficeName': fundingRequestingOfficeName,
            'contractingOfficeAgencyID': contractingOfficeAgencyID,
            'contractingOfficeID': contractingOfficeID,
            'principalNAICSCode': principalNAICSCode,
            'principalNAICSCodeDescription': principalNAICSCodeDescription,
            'productOrServiceCode': productOrServiceCode,
            'productOrServiceCodeDescription': productOrServiceCodeDescription,
            'reasonForModificationDescription': reasonForModificationDescription,
            'productOrServiceCodeType': productOrServiceCodeType,
            'descriptionOfContractRequirement': descriptionOfContractRequirement,
            'reasonForModification': reasonForModification,
            'createdBy': createdBy,
            'createdDate': createdDate,
            'lastModifiedBy': lastModifiedBy,
            'lastModifiedDate': lastModifiedDate,
            'approvedBy': approvedBy,
            'approvedDate': approvedDate,
            'closedBy': closedBy,
            'closedDate': closedDate,
        }
        print(record)
        records.append(record)

    # Check for a next link for pagination
    next_link = root.find(".//atom:link[@rel='next']", ns)
    if next_link is not None:
        next_url = next_link.get('href')
        if next_url:
            # Fetch the next page of data
            print("### GOING TO NEXT PAGE ###")
            next_page_data = fetch_fpds_data(None, None, None, None, url=next_url)
            # Parse the next page and extend the records list
            records.extend(parse_xml(next_page_data, ns))

    return records


def output_csv(records, filename="fpds_data.csv"):
    if records:
        # Extract headers from the keys of the first record
        headers = records[0].keys()

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)

            # Write the header
            writer.writeheader()

            # Write the records
            for record in records:
                writer.writerow(record)

    print(f"Data exported to {filename} successfully.")


# Before inserting, replace empty strings with None for numeric fields
def preprocess_record(record):
    numeric_fields = ['obligatedAmount', 'baseAndExercisedOptionsValue', 'baseAndAllOptionsValue',
                      'totalObligatedAmount', 'totalBaseAndExercisedOptionsValue', 'totalBaseAndAllOptionsValue']
    for field in numeric_fields:
        if record[field] == '':
            record[field] = None  # Replace empty string with None
        else:
            record[field] = float(record[field])  # Ensure the value is a float
    return record


def insert_into_db(records):
    # Ensure connection is defined outside the try block for the finally block's scope
    conn = None
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
        cur = conn.cursor()

        # Prepare an INSERT statement. Make sure column names match your table's schema, excluding 'id'
        insert_query = """
        INSERT INTO fpds_raw (
            title, modified, PIID, modNumber, referencedIDVPIID, IDVModNumber, UEI, UEILegalBusinessName, 
            immediateParentUEI, immediateParentUEIName, domesticParentUEI, domesticParentUEIName, ultimateParentUEI, 
            ultimateParentUEIName, vendorName, vendorAlternateName, vendorLegalOrganizationName, vendorStreetAddress, 
            vendorCity, vendorState, vendorZIPCode, vendorCountryCode, vendorPhoneNo, vendorFaxNo, 
            vendorCongressionalDistrictCode, vendorEntityDataSource, obligatedAmount, baseAndExercisedOptionsValue, 
            baseAndAllOptionsValue, totalObligatedAmount, totalBaseAndExercisedOptionsValue, totalBaseAndAllOptionsValue, 
            signedDate, effectiveDate, currentCompletionDate, ultimateCompletionDate, fundingRequestingDepartmentID, 
            fundingRequestingDepartmentName, fundingRequestingAgencyID, fundingRequestingAgencyName, fundingRequestingOfficeID, 
            fundingRequestingOfficeName, contractingOfficeAgencyID, contractingOfficeID, principalNAICSCode, 
            principalNAICSCodeDescription, productOrServiceCode, productOrServiceCodeDescription, productOrServiceCodeType, 
            descriptionOfContractRequirement, reasonForModification, reasonForModificationDescription, createdBy, 
            createdDate, lastModifiedBy, lastModifiedDate, approvedBy, approvedDate, closedBy, closedDate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Iterate through records and insert each into the database
        for record in records:
            # Extract values in the same order as the INSERT statement's columns
            cur.execute(insert_query, (
                record['title'], record['modified'], record['PIID'], record['modNumber'], record['referencedIDVPIID'],
                record['IDVModNumber'], record['UEI'], record['UEILegalBusinessName'], record['immediateParentUEI'],
                record['immediateParentUEIName'], record['domesticParentUEI'], record['domesticParentUEIName'],
                record['ultimateParentUEI'], record['ultimateParentUEIName'], record['vendorName'],
                record['vendorAlternateName'], record['vendorLegalOrganizationName'], record['vendorStreetAddress'],
                record['vendorCity'], record['vendorState'], record['vendorZIPCode'], record['vendorCountryCode'],
                record['vendorPhoneNo'], record['vendorFaxNo'], record['vendorCongressionalDistrictCode'],
                record['vendorEntityDataSource'], record['obligatedAmount'], record['baseAndExercisedOptionsValue'],
                record['baseAndAllOptionsValue'], record['totalObligatedAmount'], record['totalBaseAndExercisedOptionsValue'],
                record['totalBaseAndAllOptionsValue'], record['signedDate'], record['effectiveDate'],
                record['currentCompletionDate'], record['ultimateCompletionDate'], record['fundingRequestingDepartmentID'],
                record['fundingRequestingDepartmentName'], record['fundingRequestingAgencyID'], record['fundingRequestingAgencyName'],
                record['fundingRequestingOfficeID'], record['fundingRequestingOfficeName'], record['contractingOfficeAgencyID'],
                record['contractingOfficeID'], record['principalNAICSCode'], record['principalNAICSCodeDescription'],
                record['productOrServiceCode'], record['productOrServiceCodeDescription'], record['productOrServiceCodeType'],
                record['descriptionOfContractRequirement'], record['reasonForModification'], record['reasonForModificationDescription'],
                record['createdBy'], record['createdDate'], record['lastModifiedBy'], record['lastModifiedDate'],
                record['approvedBy'], record['approvedDate'], record['closedBy'], record['closedDate']
            ))

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn is not None:
            conn.close()


def main():
    start_date = datetime(2024, 2, 13).strftime('%Y-%m-%d')
    end_date = datetime(2024, 2, 14).strftime('%Y-%m-%d')

    # Thread runs for each of the following agency IDs - up to 10. The following pulls from ALL agencies.
    funding_agency_IDs = ["1*","2*","3*","4*","5*","6*","7*","8*","9*","0*"]

    naics = "541611"

    records = []

    # Use ThreadPoolExecutor to run fetch_fpds_data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Map fetch_fpds_data across your UEIs
        future_to_uei = {executor.submit(fetch_fpds_data, start_date, end_date, id, naics): id for id in funding_agency_IDs}

        for future in concurrent.futures.as_completed(future_to_uei):
            uei = future_to_uei[future]
            try:
                xml_data = future.result()
                these_records = parse_xml(xml_data)
                records.extend(these_records)
            except Exception as exc:
                print(f"{uei} generated an exception: {exc}")

    # Once all threads complete, you can process the records as before
    # output_csv(records, 'fpds_data.csv')

    # insert_into_db(records)  # enable to insert into postgres
    insert_into_db(records)

    print('Job complete.')

if __name__ == "__main__":
    main()
