# Bare-bones Python script to download FPDS ATOM Feed results into Postgres or CSV

Downloaded datapoints configurable in the parse_xml function.

Query runs by selecting a date range in the main() function. Date range is inclusive (selecting a date range of 01 FEB 2024 - 02 FEB 2024 will return results for both days).

Use the data dictionary located at: https://www.fpds.gov/wiki/index.php/Atom_Feed_Specifications_V_1.5.3 to identify target data points and formats.
