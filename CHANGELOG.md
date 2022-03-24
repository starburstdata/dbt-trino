## dbt-trino TBD (TBD)

### Features

### Fixes

### Under the hood
- Add support for future versions of dbt-core
  
Contributors:

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
