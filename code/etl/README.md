
The code in this section performs all of the ETL.  It reads raw CSV files, cleans the raw data, loads them into a Postgres database in a standardized format, and combines the data of all partners into common tables.

`run_all_etl.py` performs all these steps from top to bottom. If the expected data is stored in the `data` subdirectory of the root of this repository, and the Postgres database is empty, `run_all_etl.py` will load the database with a clean copy of the data.

# Details

The `uploaders` subdirectory contains Python modules that prepare the Postgres database and load the data. These are essentially scripts, and when run in the order specified in `run_all_etl.py` they fill an empty database with our data.

These uploaders use two classes that are contained in `pipeline/tableuploader.py` to create each table in the database.

* `UploadTable()` loads a table into Postgres database from local CSVs, performing the cleaning as it goes.
* `CombineTable()` creates and populates a table in our Postgres database using data already in the database. (Generally combining the data from multiple partners).

When creating tables, all scripts refer to the `db_schema/SQLtables_cols.py` which defines the columns of each table. This ensures that the columns of each table are consistent across partners.  This file also documents our database schema programmatically within our code. See the Database Schema section below for details.

In order to load raw CSVs into the database, `UploadTable()` relies on a function that maps the raw data into a cleaned dataframe consistent with our database schema.  These are stored in the `cleaners` subdirectory.

When handling columns with many possible text values that are mapped to standard values, the cleaner functions use mapper CSVs contained in the `mappers` subdirectory.  These mapper CSVs are also used when creating lookup tables (`uploaders/load_lookuptables.py`) in order to enforce that a mapped column loaded to the database only contains valid values.

# Database Schema

We store our data in multiple schemas.  We create one schema for each partner,
(ex: 'noble' and 'kipp_nj'), where each table contains the data from that
individual partner.  We also create a 'common' schema where we combine the data
from all partners.

Each table has a standardized set of columns:

- Every table in a partner data schema contain the columns defined in 'partnerids' and 'data' of that table's dictionary in `db_schema/SQLtables_cols.py`.

    * This means that corresponding tables in partner data schemas (ex: noble.students and kipp_nj.students) contain identical columns.

* Every table in the 'common' schema contains the set of columns defined in 'commonids' and 'data' of that table's dictionary in `db_schema/SQLtables_cols.py`.