## dbt-trino TBD (TBD)

### Features

### Fixes
- Fix `TrinoKerberosCredentials.trino_auth()` for kerberos authentication ([#78](https://github.com/starburstdata/dbt-trino/pull/78))
### Under the hood
- Move crossdb macros from trino-dbt-utils to adapter ([#83](https://github.com/starburstdata/dbt-trino/pull/83))
- Added dbt 1.2.0 standard adapter tests ([#83](https://github.com/starburstdata/dbt-trino/pull/83))

Contributors:
* [@vivianhsu0214](https://github.com/vivianhsu0214) ([#78](https://github.com/starburstdata/dbt-trino/pull/78))
* [@mdesmet](https://github.com/mdesmet) ([#78](https://github.com/starburstdata/dbt-trino/pull/83))

## dbt-trino 1.1.1 (June 20, 2022)

### Fixes
- Enable setting session properties per dbt model when `session_properties` is not defined in the dbt profile ([#71](https://github.com/starburstdata/dbt-trino/pull/71))
- Support impersonation with JWT, certificate and OAuth authentication ([#73](https://github.com/starburstdata/dbt-trino/issues/73), [#74](https://github.com/starburstdata/dbt-trino/pull/74))

Contributors:
* [@findinpath](https://github.com/findinpath) ([#71](https://github.com/starburstdata/dbt-trino/pull/71))
* [@mdesmet](https://github.com/mdesmet) ([#74](https://github.com/starburstdata/dbt-trino/pull/74))

## dbt-trino 1.1.0 (May 9, 2022)

### Features
- Add support for `on_table_exists` in table materialization ([#26](https://github.com/starburstdata/dbt-trino/issues/26), [#54](https://github.com/starburstdata/dbt-trino/pull/54))
- Adds support for OAuth2 authentication using web browser ([#40](https://github.com/starburstdata/dbt-trino/issues/40), [#41](https://github.com/starburstdata/dbt-trino/pull/41))
- Add `view_security` to define security mode for views ([#65](https://github.com/starburstdata/dbt-trino/pull/65))
- Support for dbt source freshness ([#28](https://github.com/starburstdata/dbt-trino/issues/28), [#61](https://github.com/starburstdata/dbt-trino/pull/61))

### Fixes
- Add support for future versions of dbt-core ([#55](https://github.com/starburstdata/dbt-trino/issues/55), [#65](https://github.com/starburstdata/dbt-trino/pull/65))

### Under the hood
- Add PostgreSQL docker container for testing ([#66](https://github.com/starburstdata/dbt-trino/issues/66), [#67](https://github.com/starburstdata/dbt-trino/pull/67))
- Migrate to new adapter testing framework ([#57](https://github.com/starburstdata/dbt-trino/issues/57), [#65](https://github.com/starburstdata/dbt-trino/pull/65))
- Implement trino-python-client's prepared statements using `experimental_python_types` ([#61](https://github.com/starburstdata/dbt-trino/pull/61))
  
Contributors:
* [@hovaesco](https://github.com/hovaesco) ([#54](https://github.com/starburstdata/dbt-trino/pull/54), [#65](https://github.com/starburstdata/dbt-trino/pull/65), [#67](https://github.com/starburstdata/dbt-trino/pull/67))
* [@smith-m](https://github.com/smith-m) ([#65](https://github.com/starburstdata/dbt-trino/pull/65))
* [@mdesmet](https://github.com/mdesmet) ([#41](https://github.com/starburstdata/dbt-trino/pull/41), [#61](https://github.com/starburstdata/dbt-trino/pull/61))

## dbt-trino 1.0.3 (March 2, 2022)

### Features
- Adds support for Trino certificate authentication ([#45](https://github.com/starburstdata/dbt-trino/pull/45))

### Fixes
- Supporting custom schemas in incremental models ([#17](https://github.com/starburstdata/dbt-trino/issues/17), [#39](https://github.com/starburstdata/dbt-trino/pull/39))
- Supporting column type overrides in seeds ([#42](https://github.com/starburstdata/dbt-trino/issues/42)), ([#44](https://github.com/starburstdata/dbt-trino/pull/44))

### Under the hood
- Add missing tests to Makefile ([#43](https://github.com/starburstdata/dbt-trino/pull/43))
- Introduce PR template and `CHANGELOG.md` ([#47](https://github.com/starburstdata/dbt-trino/pull/47))

Contributors:
* [@hovaesco](https://github.com/hovaesco) ([#43](https://github.com/starburstdata/dbt-trino/pull/43), [#47](https://github.com/starburstdata/dbt-trino/pull/47))
* [@austenLacy](https://github.com/austenLacy) ([#45](https://github.com/starburstdata/dbt-trino/pull/45))
* [@rahulj51](https://github.com/rahulj51) ([#39](https://github.com/starburstdata/dbt-trino/pull/39))
* [@mdesmet](https://github.com/mdesmet) ([#44](https://github.com/starburstdata/dbt-trino/pull/44))

## dbt-trino 1.0.1 (January 24, 2022)

### Features
- Add support for JWT authentication ([#31](https://github.com/starburstdata/dbt-trino/issues/31), [#32](https://github.com/starburstdata/dbt-trino/pull/32))
- Adds certificate authentication as an optional credential parameter ([#23](https://github.com/starburstdata/dbt-trino/issues/23), [#24](https://github.com/starburstdata/dbt-trino/pull/24))

### Fixes
- Drop redundant transaction queries ([#20](https://github.com/starburstdata/dbt-trino/issues/20), [#30](https://github.com/starburstdata/dbt-trino/pull/30))

### Under the hood
- Upgrade to dbt-core 1.0.1 ([#34](https://github.com/starburstdata/dbt-trino/pull/34))

Contributors:
* [@Haunfelder](https://github.com/Haunfelder) ([#24](https://github.com/starburstdata/dbt-trino/pull/24))
* [@hovaesco](https://github.com/hovaesco) ([#30](https://github.com/starburstdata/dbt-trino/pull/30), [#32](https://github.com/starburstdata/dbt-trino/pull/32),  [#34](https://github.com/starburstdata/dbt-trino/pull/34))

## dbt-trino 1.0.0  (December 20, 2021)

### Features
- Add support for dbt `current_timestamp()` macro ([#15](https://github.com/starburstdata/dbt-trino/pull/15))

### Fixes
- Change default batch size to 1000 ([#16](https://github.com/starburstdata/dbt-trino/issues/16), [#18](https://github.com/starburstdata/dbt-trino/pull/18))

### Under the hood
- Add CI for Trino and Starburst ([#14](https://github.com/starburstdata/dbt-trino/pull/14))
- Add release pipeline ([#21](https://github.com/starburstdata/dbt-trino/pull/21))
- Upgrade to dbt-core 1.0.0 and variety of adjustments ([#25](https://github.com/starburstdata/dbt-trino/pull/25))

Contributors:
* [@findinpath](https://github.com/findinpath) ([#15](https://github.com/starburstdata/dbt-trino/pull/15))
* [@hovaesco](https://github.com/hovaesco) ([#14](https://github.com/starburstdata/dbt-trino/pull/14), [#18](https://github.com/starburstdata/dbt-trino/pull/18), [#21](https://github.com/starburstdata/dbt-trino/pull/21), [#25](https://github.com/starburstdata/dbt-trino/pull/25))

## dbt-trino 0.21.0  (October 8, 2021)

### Features
- Adding support for incremental models append strategy ([#1](https://github.com/starburstdata/dbt-trino/issues/1), [#10](https://github.com/starburstdata/dbt-trino/pull/10))

### Under the hood
- Upgrade to dbt-core 0.21.0 ([#11](https://github.com/starburstdata/dbt-trino/pull/11))


Contributors:
* [@findinpath](https://github.com/findinpath) ([#10](https://github.com/starburstdata/dbt-trino/pull/10))
* [@hovaesco](https://github.com/hovaesco) ([#11](https://github.com/starburstdata/dbt-trino/pull/11))
