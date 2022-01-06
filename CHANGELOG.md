# Changelog

## 0.1.0 (2022-01-06)


### Features

* Add a limit to the number of bytes that can be outstanding to a publisher ([#54](https://www.github.com/googleapis/java-pubsublite-flink/issues/54)) ([8bc5058](https://www.github.com/googleapis/java-pubsublite-flink/commit/8bc50585c098e5d18d42256db32cf41fcf8d384a))
* Add a publisher cache for the pubsub lite sink ([#18](https://www.github.com/googleapis/java-pubsublite-flink/issues/18)) ([52ab0ba](https://www.github.com/googleapis/java-pubsublite-flink/commit/52ab0ba3b484431d88b39c578753b7a5f81b9842))
* Add a publisher which can wait for outstanding messages ([#19](https://www.github.com/googleapis/java-pubsublite-flink/issues/19)) ([69fb64a](https://www.github.com/googleapis/java-pubsublite-flink/commit/69fb64a42e89d2c85d36064494948f3fd8676d9a))
* Add a serializing publisher ([#20](https://www.github.com/googleapis/java-pubsublite-flink/issues/20)) ([65824e0](https://www.github.com/googleapis/java-pubsublite-flink/commit/65824e0d0378e614c1879dea42d541f90ec398b4))
* Add SplitDiscovery for use in split enumerator ([#14](https://www.github.com/googleapis/java-pubsublite-flink/issues/14)) ([b68d2b4](https://www.github.com/googleapis/java-pubsublite-flink/commit/b68d2b445de3762a9ffb4f8db3155ad80b440161))
* Add the checkpoint and serializer for the split enumerator ([#12](https://www.github.com/googleapis/java-pubsublite-flink/issues/12)) ([e4649d5](https://www.github.com/googleapis/java-pubsublite-flink/commit/e4649d50bbab9cd13b78b7c1fa5a2e478797a6b3))
* add the DeserializingSplitReader ([#11](https://www.github.com/googleapis/java-pubsublite-flink/issues/11)) ([8b63ec4](https://www.github.com/googleapis/java-pubsublite-flink/commit/8b63ec4b8dd0c6b0d9335ed8b15974f39c82a91e))
* Add the pubsub lite source and settings ([#17](https://www.github.com/googleapis/java-pubsublite-flink/issues/17)) ([5e1bb41](https://www.github.com/googleapis/java-pubsublite-flink/commit/5e1bb41d048bb4b99ee14515674cdd9f32a4275e))
* Add the pubsub lite source reader ([#13](https://www.github.com/googleapis/java-pubsublite-flink/issues/13)) ([2c407e9](https://www.github.com/googleapis/java-pubsublite-flink/commit/2c407e91ba2d1592ae71cf65ff0e78349d2904a0))
* add the pubsublite sink ([#21](https://www.github.com/googleapis/java-pubsublite-flink/issues/21)) ([186c93b](https://www.github.com/googleapis/java-pubsublite-flink/commit/186c93bb0f50622d86417f17c25aab8a24c557da))
* Add the pubsublite split enumerator ([#16](https://www.github.com/googleapis/java-pubsublite-flink/issues/16)) ([8a79086](https://www.github.com/googleapis/java-pubsublite-flink/commit/8a790869a24b280b50ec5098fde5c531a642504c))
* Add the stop condition to allow users to stop based on offsets ([#59](https://www.github.com/googleapis/java-pubsublite-flink/issues/59)) ([4d3f41d](https://www.github.com/googleapis/java-pubsublite-flink/commit/4d3f41db927cd8b31a95851548ef58a303392010))
* add the Uniform partition assigner  ([#15](https://www.github.com/googleapis/java-pubsublite-flink/issues/15)) ([5f7abc2](https://www.github.com/googleapis/java-pubsublite-flink/commit/5f7abc2ef20fef7e81449fc4c02a7bb9095060fd))
* Create the completable pull subscriber: a version of blocking pull subscriber which can finish ([#7](https://www.github.com/googleapis/java-pubsublite-flink/issues/7)) ([f5ae84f](https://www.github.com/googleapis/java-pubsublite-flink/commit/f5ae84ff47c8ec2f5ece89864ab770cf5e27f727))
* create the MessageSplitReader ([#10](https://www.github.com/googleapis/java-pubsublite-flink/issues/10)) ([24b8e7e](https://www.github.com/googleapis/java-pubsublite-flink/commit/24b8e7efe725dd9a8489aecb2337b4f3c52d6b8d))
* initial code generation ([f2ab73d](https://www.github.com/googleapis/java-pubsublite-flink/commit/f2ab73df31e16079b216449a9f75e8d4a4c000fc))
* Initial commit, create an implementation of SourceSplit for a subscription partition ([#5](https://www.github.com/googleapis/java-pubsublite-flink/issues/5)) ([879fc3f](https://www.github.com/googleapis/java-pubsublite-flink/commit/879fc3f056eb2c3cf2e18bfe8d9ac7b94e85a1a0))
* integration test for the psl sink and source ([#22](https://www.github.com/googleapis/java-pubsublite-flink/issues/22)) ([1c654d1](https://www.github.com/googleapis/java-pubsublite-flink/commit/1c654d11702388ad1f2dd00f615cf1594293708f))


### Bug Fixes

* Fix errors caused by client library upgrade ([#50](https://www.github.com/googleapis/java-pubsublite-flink/issues/50)) ([5e5caf3](https://www.github.com/googleapis/java-pubsublite-flink/commit/5e5caf35a43432cf989019657418e2358c85a4eb))
* fix Java 17 build. No wildcard import. ([#86](https://www.github.com/googleapis/java-pubsublite-flink/issues/86)) ([9102c5f](https://www.github.com/googleapis/java-pubsublite-flink/commit/9102c5fdfd6f9215240afe98e10b6671967a08c1))
* **java:** add -ntp flag to native image testing command ([#1299](https://www.github.com/googleapis/java-pubsublite-flink/issues/1299)) ([#89](https://www.github.com/googleapis/java-pubsublite-flink/issues/89)) ([46ecef7](https://www.github.com/googleapis/java-pubsublite-flink/commit/46ecef749f061449ce4dfb132e61cdf0ac25ed02))
* **java:** java 17 dependency arguments ([#1266](https://www.github.com/googleapis/java-pubsublite-flink/issues/1266)) ([#77](https://www.github.com/googleapis/java-pubsublite-flink/issues/77)) ([5dd4901](https://www.github.com/googleapis/java-pubsublite-flink/commit/5dd4901a6db5904cfb998087b222278af431c868))
* **java:** run Maven in plain console-friendly mode ([#1301](https://www.github.com/googleapis/java-pubsublite-flink/issues/1301)) ([#97](https://www.github.com/googleapis/java-pubsublite-flink/issues/97)) ([c4d856b](https://www.github.com/googleapis/java-pubsublite-flink/commit/c4d856b489ce143aa8eb59421943670fa1d18c4d))
* Move internal packages to "internal" use a property for flink and pubsublite versions ([#45](https://www.github.com/googleapis/java-pubsublite-flink/issues/45)) ([0a7f0a9](https://www.github.com/googleapis/java-pubsublite-flink/commit/0a7f0a98e7ae270eed61ae38f021fd4d25292edc))
* **test:** Fix flake in PubsubLiteSourceReaderTest ([#44](https://www.github.com/googleapis/java-pubsublite-flink/issues/44)) ([09871f3](https://www.github.com/googleapis/java-pubsublite-flink/commit/09871f3797350f80023aaba05560adb6924ae75c))
* Use a cursor client so we can close the client ([#55](https://www.github.com/googleapis/java-pubsublite-flink/issues/55)) ([7de246d](https://www.github.com/googleapis/java-pubsublite-flink/commit/7de246d101a08844220fb6fb5b1668fadf089ac1))


### Documentation

* update README ([#38](https://www.github.com/googleapis/java-pubsublite-flink/issues/38)) ([a6db108](https://www.github.com/googleapis/java-pubsublite-flink/commit/a6db108f83849c801262c6464f4b21d1aace8fc8))


### Dependencies

* update dependency com.google.cloud:google-cloud-pubsublite-parent to v0.18.0 ([#34](https://www.github.com/googleapis/java-pubsublite-flink/issues/34)) ([0e74b49](https://www.github.com/googleapis/java-pubsublite-flink/commit/0e74b49e57e63967fa4bd15e4d58ad2125571e13))
* update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.3.0 ([#74](https://www.github.com/googleapis/java-pubsublite-flink/issues/74)) ([a91ca46](https://www.github.com/googleapis/java-pubsublite-flink/commit/a91ca468b0473ea16a207f6ac6a1de26782ed771))
* update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.1 ([#83](https://www.github.com/googleapis/java-pubsublite-flink/issues/83)) ([9824ff0](https://www.github.com/googleapis/java-pubsublite-flink/commit/9824ff06fcfa9986a47a01d9824ef8201e1ff8ad))
* update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.3 ([#91](https://www.github.com/googleapis/java-pubsublite-flink/issues/91)) ([8038470](https://www.github.com/googleapis/java-pubsublite-flink/commit/803847011a22d63c72ac344c20bb31a53ed1a7c1))
* update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.5 ([#92](https://www.github.com/googleapis/java-pubsublite-flink/issues/92)) ([3a5967e](https://www.github.com/googleapis/java-pubsublite-flink/commit/3a5967e860c25ed142e5b54b2c569c2423d83fc9))
* update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.6 ([#93](https://www.github.com/googleapis/java-pubsublite-flink/issues/93)) ([5c4801e](https://www.github.com/googleapis/java-pubsublite-flink/commit/5c4801ebfddab12189503c176d5186c82d17eca1))
* update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.7 ([#95](https://www.github.com/googleapis/java-pubsublite-flink/issues/95)) ([dbffea2](https://www.github.com/googleapis/java-pubsublite-flink/commit/dbffea2e2e2573721cfee5d418584148128c0000))
* update dependency org.slf4j:slf4j-api to v1.7.32 ([#29](https://www.github.com/googleapis/java-pubsublite-flink/issues/29)) ([6c3f6e6](https://www.github.com/googleapis/java-pubsublite-flink/commit/6c3f6e6088d33ba1445c4b0bbdae11d3f3ca54ca))
* update flink.version to v1.13.1 ([#46](https://www.github.com/googleapis/java-pubsublite-flink/issues/46)) ([56ce037](https://www.github.com/googleapis/java-pubsublite-flink/commit/56ce0370d14de0331223ca736bc70409e380b52d))
* update flink.version to v1.13.2 ([#51](https://www.github.com/googleapis/java-pubsublite-flink/issues/51)) ([754e2c1](https://www.github.com/googleapis/java-pubsublite-flink/commit/754e2c18ac15d679430392bee3a5166832fea503))
* update pubsublite.version to v0.18.0 ([#47](https://www.github.com/googleapis/java-pubsublite-flink/issues/47)) ([e4a88f3](https://www.github.com/googleapis/java-pubsublite-flink/commit/e4a88f39b7f5098fa0c36fc5daac9b602da358cc))
