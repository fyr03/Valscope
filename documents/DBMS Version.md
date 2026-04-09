# DBMS Test Version Information

This document records the version information of various Database Management Systems (DBMS) used in the testing of the Pinolo_Extension project.

## Supported DBMS Versions List

| DBMS | Version | Notes |
|------|---------|-------|
| TiDB | 8.0.11-TiDB-v7.5.1 | - |
| MySQL | 9.4.0 | - |
| OceanBase | 5.7.25-OceanBase_CE-v4.3.5.4 | Community Edition |
| MariaDB | 12.0.2-MariaDB-ubu2404 | Ubuntu 24.04 version |
| Percona Server | 8.0.43-34 | - |
| PolarDB | 8.0.32-X-Cluster-8.4.19 | X-Cluster version |

## Version Selection Notes

- Includes mainstream MySQL ecosystem databases
- Covers both commercial and community editions
- Includes cloud-native databases such as TiDB and PolarDB
- All selected versions are relatively new stable releases

## Version Compatibility Considerations

Different DBMS may have differences in SQL syntax, function implementations, and performance characteristics. These differences may affect the mutation testing results of Pinolo_Extension. When analyzing test results, these version differences should be taken into account.