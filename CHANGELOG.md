# Changelog

## [0.4.0](https://github.com/contiamo/datahub-sap-hana/compare/v0.3.0...v0.4.0) (2023-07-12)


### Features

* build column lineage using sqlglot  ([#46](https://github.com/contiamo/datahub-sap-hana/issues/46)) ([fa4cc77](https://github.com/contiamo/datahub-sap-hana/commit/fa4cc77f35becfc666af8ef3ca232eab8854a2b2))


### Bug Fixes

* add create method to fix tests ([#57](https://github.com/contiamo/datahub-sap-hana/issues/57)) ([6b35bc8](https://github.com/contiamo/datahub-sap-hana/commit/6b35bc8213d335f37997b046a9bde2b565964cdd))
* add schema filter ([#53](https://github.com/contiamo/datahub-sap-hana/issues/53)) ([58fe048](https://github.com/contiamo/datahub-sap-hana/commit/58fe048933b21ed39149e167457786be28f1a6b9))
* correctly parse schema value for cross schema queries ([#62](https://github.com/contiamo/datahub-sap-hana/issues/62)) ([61bcd24](https://github.com/contiamo/datahub-sap-hana/commit/61bcd2439ae4a4c5a8db43e7ced91afbd709422c))
* make-downstream-lowercase ([#55](https://github.com/contiamo/datahub-sap-hana/issues/55)) ([ed15994](https://github.com/contiamo/datahub-sap-hana/commit/ed159946be8fdd7b7e57f3bc35855b104da17b98))


### Miscellaneous

* add description to readme ([#71](https://github.com/contiamo/datahub-sap-hana/issues/71)) ([bbbdbe2](https://github.com/contiamo/datahub-sap-hana/commit/bbbdbe215f3d37fec39a2e55befc6c8eaa79ef04))
* bump acryl-datahub from 0.10.4.3 to 0.10.5.1 ([44e8864](https://github.com/contiamo/datahub-sap-hana/commit/44e8864098a997e7b2277842e107408bbc590ff1))
* bump black from 23.3.0 to 23.7.0 ([b836cc9](https://github.com/contiamo/datahub-sap-hana/commit/b836cc931dde04d0fd3e7549c6043b163dbb328f))
* bump deepdiff from 6.3.0 to 6.3.1 ([#65](https://github.com/contiamo/datahub-sap-hana/issues/65)) ([b54fd2b](https://github.com/contiamo/datahub-sap-hana/commit/b54fd2b773eae25e0e2401ad0d8090ce43c2ed9f))
* bump hdbcli from 2.17.14 to 2.17.21 ([d94692e](https://github.com/contiamo/datahub-sap-hana/commit/d94692e57dec4b42645d6f09c5996aac07933447))
* bump pyright from 1.1.314 to 1.1.315 ([#42](https://github.com/contiamo/datahub-sap-hana/issues/42)) ([d3da693](https://github.com/contiamo/datahub-sap-hana/commit/d3da6937a0de71e8e2f702dc36d8f3e084a2b92f))
* bump ruff from 0.0.272 to 0.0.275 ([#44](https://github.com/contiamo/datahub-sap-hana/issues/44)) ([c743ffe](https://github.com/contiamo/datahub-sap-hana/commit/c743ffe2110a4dca26f0378a27604458d2b1dde3))
* bump ruff from 0.0.275 to 0.0.277 ([#60](https://github.com/contiamo/datahub-sap-hana/issues/60)) ([3a68ad6](https://github.com/contiamo/datahub-sap-hana/commit/3a68ad66e6b3a1a53ec8a71e601a57d572631c7a))
* bump sqlglot from 16.7.7 to 16.8.1 ([7f07d6e](https://github.com/contiamo/datahub-sap-hana/commit/7f07d6e8709d462ca7bc65f7973308e39ee569a4))
* bump unimport from 0.16.0 to 1.0.0 ([3651fa5](https://github.com/contiamo/datahub-sap-hana/commit/3651fa50c46f1573a537a73bbaad540e389496d0))
* fix a flappy lineage test case ([#58](https://github.com/contiamo/datahub-sap-hana/issues/58)) ([dcf8c36](https://github.com/contiamo/datahub-sap-hana/commit/dcf8c3660cd3698fd784a0d7bc5cb61b1622ec44))
* fix various linting issues ([f3a9524](https://github.com/contiamo/datahub-sap-hana/commit/f3a952421144553fef563ad0c3c7eee4b261a2d7))

## [0.3.0](https://github.com/contiamo/datahub-sap-hana/compare/v0.2.3...v0.3.0) (2023-06-19)


### Features

* add integration tests ([#21](https://github.com/contiamo/datahub-sap-hana/issues/21)) ([796b052](https://github.com/contiamo/datahub-sap-hana/commit/796b052d6eabd86850451a025626a52d88bb56a8))
* add MIT license ([9c5c137](https://github.com/contiamo/datahub-sap-hana/commit/9c5c1373af82c0e1c79f4567c9b5f24d403c4954))
* add new recipe files ([#38](https://github.com/contiamo/datahub-sap-hana/issues/38)) ([037d6c8](https://github.com/contiamo/datahub-sap-hana/commit/037d6c861cad7c959e2b0fd826cdfd775211bde5))
* update to latest datahub 0.10.2+ ([#13](https://github.com/contiamo/datahub-sap-hana/issues/13)) ([f2b9b9f](https://github.com/contiamo/datahub-sap-hana/commit/f2b9b9fef6e3b57d47a09d0e77ba592edf274370))


### Bug Fixes

* add sys filter in query ([#36](https://github.com/contiamo/datahub-sap-hana/issues/36)) ([8cb32ff](https://github.com/contiamo/datahub-sap-hana/commit/8cb32ff5ae0682c1a3aeb7f8ff6bdba350356220))
* changed query to show output in lowercase ([#39](https://github.com/contiamo/datahub-sap-hana/issues/39)) ([99607ba](https://github.com/contiamo/datahub-sap-hana/commit/99607bab0dfe98b6e78559ff5971f9d00b4617f2))
* changed schema view pattern comparison ([#35](https://github.com/contiamo/datahub-sap-hana/issues/35)) ([4360d4e](https://github.com/contiamo/datahub-sap-hana/commit/4360d4e3e8e73943de9287cdb51099b9bc9c222a))
* deleted unusued folders in integration_tests ([#40](https://github.com/contiamo/datahub-sap-hana/issues/40)) ([248bf6f](https://github.com/contiamo/datahub-sap-hana/commit/248bf6f0f832691eaa6559f9a8f18df967c19c9c))


### Miscellaneous

* add default vscode python settings ([#25](https://github.com/contiamo/datahub-sap-hana/issues/25)) ([1e7ecc5](https://github.com/contiamo/datahub-sap-hana/commit/1e7ecc58b5c04d77ddffc5e0b1837edfabc9d2c1))
* add email to License ([4ae6be1](https://github.com/contiamo/datahub-sap-hana/commit/4ae6be174a3cc6530761095ecc9ff157e7c8e156))
* bump acryl-datahub from 0.10.3.1 to 0.10.3.2 ([#26](https://github.com/contiamo/datahub-sap-hana/issues/26)) ([e2185d6](https://github.com/contiamo/datahub-sap-hana/commit/e2185d6c2fd1b5750601c119c50614f9f952af76))
* bump acryl-datahub from 0.10.3.2 to 0.10.4.1 ([#29](https://github.com/contiamo/datahub-sap-hana/issues/29)) ([99df58a](https://github.com/contiamo/datahub-sap-hana/commit/99df58a02caaf7b05f5092f001f5b328626b8ece))
* bump hdbcli from 2.16.26 to 2.17.14 ([#30](https://github.com/contiamo/datahub-sap-hana/issues/30)) ([c09355c](https://github.com/contiamo/datahub-sap-hana/commit/c09355caee90a1b4a2ef906edb67aefb15085346))
* bump pyright from 1.1.311 to 1.1.313 ([#23](https://github.com/contiamo/datahub-sap-hana/issues/23)) ([6353f77](https://github.com/contiamo/datahub-sap-hana/commit/6353f7723eb588660df2500a9167940a384641f9))
* bump pyright from 1.1.313 to 1.1.314 ([#28](https://github.com/contiamo/datahub-sap-hana/issues/28)) ([2f4dbce](https://github.com/contiamo/datahub-sap-hana/commit/2f4dbce6f30e5e4789a7eae74b2c0c15060781aa))
* bump ruff from 0.0.270 to 0.0.272 ([#24](https://github.com/contiamo/datahub-sap-hana/issues/24)) ([c336df4](https://github.com/contiamo/datahub-sap-hana/commit/c336df4197161d9c4254d1587fb494fec69dbf21))
* edited taskfile ([#37](https://github.com/contiamo/datahub-sap-hana/issues/37)) ([5447602](https://github.com/contiamo/datahub-sap-hana/commit/544760214db40d605d832e4f1cefa33b4fd3871f))
* remove any instructions for SAP Hana express ([9c5c137](https://github.com/contiamo/datahub-sap-hana/commit/9c5c1373af82c0e1c79f4567c9b5f24d403c4954))

### [0.2.3](https://www.github.com/contiamo/datahub-sap-hana/compare/v0.2.2...v0.2.3) (2022-01-20)


### Miscellaneous

* generate checksums during releases ([8a3f09b](https://www.github.com/contiamo/datahub-sap-hana/commit/8a3f09b46616354c47b4c2c192b880c698c27f55))

### [0.2.2](https://www.github.com/contiamo/datahub-sap-hana/compare/v0.2.1...v0.2.2) (2022-01-19)


### Bug Fixes

* use metadata action to get semver tag ([2daf62d](https://www.github.com/contiamo/datahub-sap-hana/commit/2daf62db09414e92ed5a72ee2ca3136a8fa45a62))

### [0.2.1](https://www.github.com/contiamo/datahub-sap-hana/compare/v0.2.0...v0.2.1) (2022-01-19)


### Bug Fixes

* set correct step order during releases ([08fc00f](https://www.github.com/contiamo/datahub-sap-hana/commit/08fc00f4ec842b306350bf6fec614e3efc7375cf))

## [0.2.0](https://www.github.com/contiamo/datahub-sap-hana/compare/v0.1.2...v0.2.0) (2022-01-19)


### Features

* create docker image and related ci/cd workflow ([7a61d18](https://www.github.com/contiamo/datahub-sap-hana/commit/7a61d18b4d6138ee20082885ed26e183f02908cc))


### Bug Fixes

* support installation on Mac M1 ([#7](https://www.github.com/contiamo/datahub-sap-hana/issues/7)) ([7a61d18](https://www.github.com/contiamo/datahub-sap-hana/commit/7a61d18b4d6138ee20082885ed26e183f02908cc))


### Miscellaneous

* add information about glcoud auth for docker ([7012614](https://www.github.com/contiamo/datahub-sap-hana/commit/7012614ae6fc264e3bbacfbda841e812ec21d7b3))
* add unit testing ([#4](https://www.github.com/contiamo/datahub-sap-hana/issues/4)) ([5dfbab3](https://www.github.com/contiamo/datahub-sap-hana/commit/5dfbab3af5b9a9531b84265fcb62dddeb6e0be85))

### [0.1.2](https://www.github.com/contiamo/datahub-sap-hana/compare/v0.1.1...v0.1.2) (2022-01-17)


### Bug Fixes

* add missing env variables in the release flow ([2a50bfb](https://www.github.com/contiamo/datahub-sap-hana/commit/2a50bfb58c711bdd42de408eaa3ebcb66901c449))

### [0.1.1](https://www.github.com/contiamo/datahub-sap-hana/compare/v0.1.0...v0.1.1) (2022-01-17)


### Miscellaneous

* highlight that pre-build releases are available ([f93727a](https://www.github.com/contiamo/datahub-sap-hana/commit/f93727a350d108e14dfb0d2a824936509161347e))

## 0.1.0 (2022-01-17)


### Features

* initial project setup ([6bcbbd8](https://www.github.com/contiamo/datahub-sap-hana/commit/6bcbbd85ba9b22ba89c48dba856d7df4d34827a7))
* initial working implementation ([06bd546](https://www.github.com/contiamo/datahub-sap-hana/commit/06bd54686aaa89656d5f509648ad7a3454dec564))
