# Changelog

## 0.1.0 (2021-09-27)


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
* Move internal packages to "internal" use a property for flink and pubsublite versions ([#45](https://www.github.com/googleapis/java-pubsublite-flink/issues/45)) ([0a7f0a9](https://www.github.com/googleapis/java-pubsublite-flink/commit/0a7f0a98e7ae270eed61ae38f021fd4d25292edc))
* **test:** Fix flake in PubsubLiteSourceReaderTest ([#44](https://www.github.com/googleapis/java-pubsublite-flink/issues/44)) ([09871f3](https://www.github.com/googleapis/java-pubsublite-flink/commit/09871f3797350f80023aaba05560adb6924ae75c))


### Documentation

* update README ([#38](https://www.github.com/googleapis/java-pubsublite-flink/issues/38)) ([a6db108](https://www.github.com/googleapis/java-pubsublite-flink/commit/a6db108f83849c801262c6464f4b21d1aace8fc8))


### Dependencies

* update dependency com.google.cloud:google-cloud-pubsublite-parent to v0.18.0 ([#34](https://www.github.com/googleapis/java-pubsublite-flink/issues/34)) ([0e74b49](https://www.github.com/googleapis/java-pubsublite-flink/commit/0e74b49e57e63967fa4bd15e4d58ad2125571e13))
* update dependency org.slf4j:slf4j-api to v1.7.32 ([#29](https://www.github.com/googleapis/java-pubsublite-flink/issues/29)) ([6c3f6e6](https://www.github.com/googleapis/java-pubsublite-flink/commit/6c3f6e6088d33ba1445c4b0bbdae11d3f3ca54ca))
* update flink.version to v1.13.1 ([#46](https://www.github.com/googleapis/java-pubsublite-flink/issues/46)) ([56ce037](https://www.github.com/googleapis/java-pubsublite-flink/commit/56ce0370d14de0331223ca736bc70409e380b52d))
* update flink.version to v1.13.2 ([#51](https://www.github.com/googleapis/java-pubsublite-flink/issues/51)) ([754e2c1](https://www.github.com/googleapis/java-pubsublite-flink/commit/754e2c18ac15d679430392bee3a5166832fea503))
* update pubsublite.version to v0.18.0 ([#47](https://www.github.com/googleapis/java-pubsublite-flink/issues/47)) ([e4a88f3](https://www.github.com/googleapis/java-pubsublite-flink/commit/e4a88f39b7f5098fa0c36fc5daac9b602da358cc))
