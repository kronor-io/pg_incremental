### pg\_incremental v1.3.0 (May 15, 2025)
* Adds an incremental.skip\_file function to use for erroneuous files in file pipelines
* Removes the hard dependency on pg\_cron at CREATE EXTENSION time

### pg\_incremental v1.2.0 (February 26, 2025)

* Fixes bug that could cause batched file list pipelines to crash
* Add a `max_batch_size` argument to file list pipelines
* Improve performance of batched file list pipelines
* Adjust the default schedule of file list pipelines to every 15 minutes

### pg\_incremental v1.1.1 (January 10, 2025)

* Fixes a bug that prevented file list pipelines from being refreshed

### pg\_incremental v1.1.0 (January 10, 2025)

* Fixes a bug that prevented insert..select pipelines (#2)
* Fixes a bug that caused file list pipelines to repeat first file
* Add batched mode for file list pipeline
* Add incremental.default\_file\_list\_function setting

### pg\_incremental v1.0.1 (December 12, 2024)

* PostgreSQL 15 support

### pg\_incremental v1.0.0 (December 12, 2024)

* Initial release
