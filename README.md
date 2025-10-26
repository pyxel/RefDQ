# RefDQ
Data quality gateway for reference data.


## Version history
#### v0.1.0 Initial MVP release.

#### v0.1.1
### Bug fixes


#### v0.1.2
### Bug fixes
Rows to update/insert can report incorrect numbers. For example when inserting a file with one row, it says it will insert 2 rows.
Table groups list needs to be distinct. The same value gets repeated.
Schema comparison errors on non-integer numeric types. Due to temp tables being created with default data type numerc(38, 0) when fields are null.
