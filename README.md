# RefDQ
Data quality gateway for reference data.

See the [User Guide](docs/user_guide.md) for setup instructions and usage documentation.


## Version history
#### v0.1.0 Initial MVP release.

#### v0.1.1
### Bug fixes


#### v0.1.2
### Bug fixes
Rows to update/insert can report incorrect numbers. For example when inserting a file with one row, it says it will insert 2 rows.
Table groups list needs to be distinct. The same value gets repeated.
Schema comparison errors on non-integer numeric types. Due to temp tables being created with default data type numerc(38, 0) when fields are null.


#### v0.1.3
### Bug fixes
Numeric strings having decimal places appended (eg 100 -> 100.0). Now all values are cast to string when loading into Pandas dataframe.


#### v0.2.0
### Multi-column primary keys
Primary keys can now be multi-column, specified as a list.


#### v0.3.0
### Auto-detect target table using column names
Target table is automatically selected by finding the closest match of column names. Selection can still be changed manually.

### Refactor home page code
Improved code structure using an abstract base class for each section with a render function.
Functionality remains the same.


#### v0.3.1
### Removed the logo
