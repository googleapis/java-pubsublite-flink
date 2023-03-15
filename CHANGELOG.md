# Changelog

## [0.1.1](https://github.com/googleapis/java-pubsublite-flink/compare/v0.1.0...v0.1.1) (2023-03-15)


### Bug Fixes

* Package naming and release config ([#193](https://github.com/googleapis/java-pubsublite-flink/issues/193)) ([0f35468](https://github.com/googleapis/java-pubsublite-flink/commit/0f354686564951250b6ac44417d459de43aca355))
* Update readme and trigger release ([#196](https://github.com/googleapis/java-pubsublite-flink/issues/196)) ([8b6fe77](https://github.com/googleapis/java-pubsublite-flink/commit/8b6fe77a28f7e0e910df4e34ae6f20a29163b704))


### Dependencies

* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.12.0 ([#191](https://github.com/googleapis/java-pubsublite-flink/issues/191)) ([77648f7](https://github.com/googleapis/java-pubsublite-flink/commit/77648f764031055e7492ba3a21aa444a53668dac))

## 0.1.0 (2023-03-14)


### Features

* Add a limit to the number of bytes that can be outstanding to a publisher ([#54](https://github.com/googleapis/java-pubsublite-flink/issues/54)) ([8bc5058](https://github.com/googleapis/java-pubsublite-flink/commit/8bc50585c098e5d18d42256db32cf41fcf8d384a))
* Add a publisher cache for the pubsub lite sink ([#18](https://github.com/googleapis/java-pubsublite-flink/issues/18)) ([52ab0ba](https://github.com/googleapis/java-pubsublite-flink/commit/52ab0ba3b484431d88b39c578753b7a5f81b9842))
* Add a publisher which can wait for outstanding messages ([#19](https://github.com/googleapis/java-pubsublite-flink/issues/19)) ([69fb64a](https://github.com/googleapis/java-pubsublite-flink/commit/69fb64a42e89d2c85d36064494948f3fd8676d9a))
* Add a serializing publisher ([#20](https://github.com/googleapis/java-pubsublite-flink/issues/20)) ([65824e0](https://github.com/googleapis/java-pubsublite-flink/commit/65824e0d0378e614c1879dea42d541f90ec398b4))
* Add build scripts for native image testing in Java 17 ([#1440](https://github.com/googleapis/java-pubsublite-flink/issues/1440)) ([#139](https://github.com/googleapis/java-pubsublite-flink/issues/139)) ([6f3bd36](https://github.com/googleapis/java-pubsublite-flink/commit/6f3bd3633ce95dedde275c2d795b760707f52bd0))
* Add SplitDiscovery for use in split enumerator ([#14](https://github.com/googleapis/java-pubsublite-flink/issues/14)) ([b68d2b4](https://github.com/googleapis/java-pubsublite-flink/commit/b68d2b445de3762a9ffb4f8db3155ad80b440161))
* Add the checkpoint and serializer for the split enumerator ([#12](https://github.com/googleapis/java-pubsublite-flink/issues/12)) ([e4649d5](https://github.com/googleapis/java-pubsublite-flink/commit/e4649d50bbab9cd13b78b7c1fa5a2e478797a6b3))
* Add the DeserializingSplitReader ([#11](https://github.com/googleapis/java-pubsublite-flink/issues/11)) ([8b63ec4](https://github.com/googleapis/java-pubsublite-flink/commit/8b63ec4b8dd0c6b0d9335ed8b15974f39c82a91e))
* Add the pubsub lite source and settings ([#17](https://github.com/googleapis/java-pubsublite-flink/issues/17)) ([5e1bb41](https://github.com/googleapis/java-pubsublite-flink/commit/5e1bb41d048bb4b99ee14515674cdd9f32a4275e))
* Add the pubsub lite source reader ([#13](https://github.com/googleapis/java-pubsublite-flink/issues/13)) ([2c407e9](https://github.com/googleapis/java-pubsublite-flink/commit/2c407e91ba2d1592ae71cf65ff0e78349d2904a0))
* Add the pubsublite sink ([#21](https://github.com/googleapis/java-pubsublite-flink/issues/21)) ([186c93b](https://github.com/googleapis/java-pubsublite-flink/commit/186c93bb0f50622d86417f17c25aab8a24c557da))
* Add the pubsublite split enumerator ([#16](https://github.com/googleapis/java-pubsublite-flink/issues/16)) ([8a79086](https://github.com/googleapis/java-pubsublite-flink/commit/8a790869a24b280b50ec5098fde5c531a642504c))
* Add the stop condition to allow users to stop based on offsets ([#59](https://github.com/googleapis/java-pubsublite-flink/issues/59)) ([4d3f41d](https://github.com/googleapis/java-pubsublite-flink/commit/4d3f41db927cd8b31a95851548ef58a303392010))
* Add the Uniform partition assigner  ([#15](https://github.com/googleapis/java-pubsublite-flink/issues/15)) ([5f7abc2](https://github.com/googleapis/java-pubsublite-flink/commit/5f7abc2ef20fef7e81449fc4c02a7bb9095060fd))
* Create the completable pull subscriber: a version of blocking pull subscriber which can finish ([#7](https://github.com/googleapis/java-pubsublite-flink/issues/7)) ([f5ae84f](https://github.com/googleapis/java-pubsublite-flink/commit/f5ae84ff47c8ec2f5ece89864ab770cf5e27f727))
* Create the MessageSplitReader ([#10](https://github.com/googleapis/java-pubsublite-flink/issues/10)) ([24b8e7e](https://github.com/googleapis/java-pubsublite-flink/commit/24b8e7efe725dd9a8489aecb2337b4f3c52d6b8d))
* Initial code generation ([f2ab73d](https://github.com/googleapis/java-pubsublite-flink/commit/f2ab73df31e16079b216449a9f75e8d4a4c000fc))
* Initial commit, create an implementation of SourceSplit for a subscription partition ([#5](https://github.com/googleapis/java-pubsublite-flink/issues/5)) ([879fc3f](https://github.com/googleapis/java-pubsublite-flink/commit/879fc3f056eb2c3cf2e18bfe8d9ac7b94e85a1a0))
* Integration test for the psl sink and source ([#22](https://github.com/googleapis/java-pubsublite-flink/issues/22)) ([1c654d1](https://github.com/googleapis/java-pubsublite-flink/commit/1c654d11702388ad1f2dd00f615cf1594293708f))
* Restructure flink connector for new internal APIs and reduce API surface ([#173](https://github.com/googleapis/java-pubsublite-flink/issues/173)) ([2d287cb](https://github.com/googleapis/java-pubsublite-flink/commit/2d287cbc4d3c032485c1c7e43b0e47a16bcfdc0e))
* Upgrade Pub/Sub Lite version ([#187](https://github.com/googleapis/java-pubsublite-flink/issues/187)) ([76794d4](https://github.com/googleapis/java-pubsublite-flink/commit/76794d43513aef21c68ec17fa3f3d276d740f636))


### Bug Fixes

* Change commit logic to use streams instead of single RPCs ([#177](https://github.com/googleapis/java-pubsublite-flink/issues/177)) ([a1d06f0](https://github.com/googleapis/java-pubsublite-flink/commit/a1d06f014a881f8429ed29878e22a28692a7831b))
* Change to use the sinkv2 interfaces ([#186](https://github.com/googleapis/java-pubsublite-flink/issues/186)) ([e142808](https://github.com/googleapis/java-pubsublite-flink/commit/e14280854d6a22892e7c6ccf9bf86872e4a2a7e8))
* Enable longpaths support for windows test ([#1485](https://github.com/googleapis/java-pubsublite-flink/issues/1485)) ([#144](https://github.com/googleapis/java-pubsublite-flink/issues/144)) ([3bcbc6b](https://github.com/googleapis/java-pubsublite-flink/commit/3bcbc6b1a29a1f50ef5c3e017358349eada65863))
* Fix errors caused by client library upgrade ([#50](https://github.com/googleapis/java-pubsublite-flink/issues/50)) ([5e5caf3](https://github.com/googleapis/java-pubsublite-flink/commit/5e5caf35a43432cf989019657418e2358c85a4eb))
* Fix Java 17 build. No wildcard import. ([#86](https://github.com/googleapis/java-pubsublite-flink/issues/86)) ([9102c5f](https://github.com/googleapis/java-pubsublite-flink/commit/9102c5fdfd6f9215240afe98e10b6671967a08c1))
* **java:** Add -ntp flag to native image testing command ([#1299](https://github.com/googleapis/java-pubsublite-flink/issues/1299)) ([#89](https://github.com/googleapis/java-pubsublite-flink/issues/89)) ([46ecef7](https://github.com/googleapis/java-pubsublite-flink/commit/46ecef749f061449ce4dfb132e61cdf0ac25ed02))
* **java:** Java 17 dependency arguments ([#1266](https://github.com/googleapis/java-pubsublite-flink/issues/1266)) ([#77](https://github.com/googleapis/java-pubsublite-flink/issues/77)) ([5dd4901](https://github.com/googleapis/java-pubsublite-flink/commit/5dd4901a6db5904cfb998087b222278af431c868))
* **java:** Run Maven in plain console-friendly mode ([#1301](https://github.com/googleapis/java-pubsublite-flink/issues/1301)) ([#97](https://github.com/googleapis/java-pubsublite-flink/issues/97)) ([c4d856b](https://github.com/googleapis/java-pubsublite-flink/commit/c4d856b489ce143aa8eb59421943670fa1d18c4d))
* Move internal packages to "internal" use a property for flink and pubsublite versions ([#45](https://github.com/googleapis/java-pubsublite-flink/issues/45)) ([0a7f0a9](https://github.com/googleapis/java-pubsublite-flink/commit/0a7f0a98e7ae270eed61ae38f021fd4d25292edc))
* **test:** Fix flake in PubsubLiteSourceReaderTest ([#44](https://github.com/googleapis/java-pubsublite-flink/issues/44)) ([09871f3](https://github.com/googleapis/java-pubsublite-flink/commit/09871f3797350f80023aaba05560adb6924ae75c))
* Upgrade flink connector to version 1.15.0, equivalent to dataproc 2.1 ([#176](https://github.com/googleapis/java-pubsublite-flink/issues/176)) ([b80a462](https://github.com/googleapis/java-pubsublite-flink/commit/b80a46297b46c7eeaba310d6b5a8e26b0485e7d4))
* Use a cursor client so we can close the client ([#55](https://github.com/googleapis/java-pubsublite-flink/issues/55)) ([7de246d](https://github.com/googleapis/java-pubsublite-flink/commit/7de246d101a08844220fb6fb5b1668fadf089ac1))
* Various fixes for flink ([#183](https://github.com/googleapis/java-pubsublite-flink/issues/183)) ([ed2207f](https://github.com/googleapis/java-pubsublite-flink/commit/ed2207fcbe89acdcc3f70b2241a0df5e6b4bcfac))


### Documentation

* Update README ([#38](https://github.com/googleapis/java-pubsublite-flink/issues/38)) ([a6db108](https://github.com/googleapis/java-pubsublite-flink/commit/a6db108f83849c801262c6464f4b21d1aace8fc8))


### Dependencies

* **java:** Update actions/github-script action to v5 ([#1339](https://github.com/googleapis/java-pubsublite-flink/issues/1339)) ([#107](https://github.com/googleapis/java-pubsublite-flink/issues/107)) ([7e8b628](https://github.com/googleapis/java-pubsublite-flink/commit/7e8b628875b7b841741232fc9228cfe56c599fdd))
* Update actions/github-script action to v6 ([#118](https://github.com/googleapis/java-pubsublite-flink/issues/118)) ([316d979](https://github.com/googleapis/java-pubsublite-flink/commit/316d97991fa261b7aa19e4ec945c99dcfa07cbdd))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v0.18.0 ([#34](https://github.com/googleapis/java-pubsublite-flink/issues/34)) ([0e74b49](https://github.com/googleapis/java-pubsublite-flink/commit/0e74b49e57e63967fa4bd15e4d58ad2125571e13))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.11.2 ([#189](https://github.com/googleapis/java-pubsublite-flink/issues/189)) ([093511b](https://github.com/googleapis/java-pubsublite-flink/commit/093511bb554c9caaa9b63987d5bb2afafdd5e67e))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.3.0 ([#74](https://github.com/googleapis/java-pubsublite-flink/issues/74)) ([a91ca46](https://github.com/googleapis/java-pubsublite-flink/commit/a91ca468b0473ea16a207f6ac6a1de26782ed771))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.1 ([#83](https://github.com/googleapis/java-pubsublite-flink/issues/83)) ([9824ff0](https://github.com/googleapis/java-pubsublite-flink/commit/9824ff06fcfa9986a47a01d9824ef8201e1ff8ad))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.10 ([#113](https://github.com/googleapis/java-pubsublite-flink/issues/113)) ([b6ef366](https://github.com/googleapis/java-pubsublite-flink/commit/b6ef366912cb91af05719323f8b591802b870c42))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.11 ([#117](https://github.com/googleapis/java-pubsublite-flink/issues/117)) ([07715c2](https://github.com/googleapis/java-pubsublite-flink/commit/07715c2ea2ba61060a82a9ce48b21445797ac674))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.12 ([#129](https://github.com/googleapis/java-pubsublite-flink/issues/129)) ([ccdf443](https://github.com/googleapis/java-pubsublite-flink/commit/ccdf443091085619deaaf4501292f803b1a523fe))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.3 ([#91](https://github.com/googleapis/java-pubsublite-flink/issues/91)) ([8038470](https://github.com/googleapis/java-pubsublite-flink/commit/803847011a22d63c72ac344c20bb31a53ed1a7c1))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.5 ([#92](https://github.com/googleapis/java-pubsublite-flink/issues/92)) ([3a5967e](https://github.com/googleapis/java-pubsublite-flink/commit/3a5967e860c25ed142e5b54b2c569c2423d83fc9))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.6 ([#93](https://github.com/googleapis/java-pubsublite-flink/issues/93)) ([5c4801e](https://github.com/googleapis/java-pubsublite-flink/commit/5c4801ebfddab12189503c176d5186c82d17eca1))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.7 ([#95](https://github.com/googleapis/java-pubsublite-flink/issues/95)) ([dbffea2](https://github.com/googleapis/java-pubsublite-flink/commit/dbffea2e2e2573721cfee5d418584148128c0000))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.4.8 ([#99](https://github.com/googleapis/java-pubsublite-flink/issues/99)) ([1b1050f](https://github.com/googleapis/java-pubsublite-flink/commit/1b1050fa32b457f951e35f07daf9df80edc2f58d))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.5.0 ([#131](https://github.com/googleapis/java-pubsublite-flink/issues/131)) ([be0b592](https://github.com/googleapis/java-pubsublite-flink/commit/be0b592e4c68dfd5be1120dd00e310d7ca9349a3))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.5.1 ([#132](https://github.com/googleapis/java-pubsublite-flink/issues/132)) ([39828fc](https://github.com/googleapis/java-pubsublite-flink/commit/39828fc150a3535a32fea4c30a1afeefb6c2d1ec))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.5.2 ([#133](https://github.com/googleapis/java-pubsublite-flink/issues/133)) ([a35345f](https://github.com/googleapis/java-pubsublite-flink/commit/a35345f110b2a8539e561a57d0253af4b02192f5))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.5.3 ([#134](https://github.com/googleapis/java-pubsublite-flink/issues/134)) ([232a473](https://github.com/googleapis/java-pubsublite-flink/commit/232a4739db70cdf00b4a543e7cdd17c3ea033d8e))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.5.4 ([#137](https://github.com/googleapis/java-pubsublite-flink/issues/137)) ([05ab761](https://github.com/googleapis/java-pubsublite-flink/commit/05ab76182cd731ef5344d93b005e0dcd416049c1))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.5.5 ([#138](https://github.com/googleapis/java-pubsublite-flink/issues/138)) ([52936b5](https://github.com/googleapis/java-pubsublite-flink/commit/52936b550b3a5f55a12c12364f9014988c105073))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.6.0 ([#140](https://github.com/googleapis/java-pubsublite-flink/issues/140)) ([6174a17](https://github.com/googleapis/java-pubsublite-flink/commit/6174a172ed8c92702f70cb4e916eec15d6178017))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.6.1 ([#143](https://github.com/googleapis/java-pubsublite-flink/issues/143)) ([a8c3e95](https://github.com/googleapis/java-pubsublite-flink/commit/a8c3e95b11d8ea2d9bdd72ead3d8f4211dbe7010))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.6.3 ([#147](https://github.com/googleapis/java-pubsublite-flink/issues/147)) ([b05b54a](https://github.com/googleapis/java-pubsublite-flink/commit/b05b54a0a1fd60e82c9d1c792224328fee03da5c))
* Update dependency com.google.cloud:google-cloud-pubsublite-parent to v1.9.4 ([#151](https://github.com/googleapis/java-pubsublite-flink/issues/151)) ([fef6285](https://github.com/googleapis/java-pubsublite-flink/commit/fef6285e6d94b20c67935ef229272843c3fa9a27))
* Update dependency kr.motd.maven:os-maven-plugin to v1.7.1 ([#166](https://github.com/googleapis/java-pubsublite-flink/issues/166)) ([0ca1b3d](https://github.com/googleapis/java-pubsublite-flink/commit/0ca1b3da6ae8fbdb2bc1b18e74c56b8807ab8d90))
* Update dependency org.slf4j:slf4j-api to v1.7.32 ([#29](https://github.com/googleapis/java-pubsublite-flink/issues/29)) ([6c3f6e6](https://github.com/googleapis/java-pubsublite-flink/commit/6c3f6e6088d33ba1445c4b0bbdae11d3f3ca54ca))
* Update dependency org.slf4j:slf4j-api to v1.7.34 ([#105](https://github.com/googleapis/java-pubsublite-flink/issues/105)) ([3c955b9](https://github.com/googleapis/java-pubsublite-flink/commit/3c955b9e4347fa8132b15c08d24cf36a38adde51))
* Update dependency org.slf4j:slf4j-api to v1.7.35 ([#110](https://github.com/googleapis/java-pubsublite-flink/issues/110)) ([0c456a1](https://github.com/googleapis/java-pubsublite-flink/commit/0c456a16da5e5d4dc17868f05fc09a0057d9060f))
* Update dependency org.slf4j:slf4j-api to v1.7.36 ([#116](https://github.com/googleapis/java-pubsublite-flink/issues/116)) ([cc09d80](https://github.com/googleapis/java-pubsublite-flink/commit/cc09d801e72b47182d502d1e3708ca923ffaa527))
* Update dependency org.slf4j:slf4j-api to v2 ([#148](https://github.com/googleapis/java-pubsublite-flink/issues/148)) ([0cfef6a](https://github.com/googleapis/java-pubsublite-flink/commit/0cfef6ad37807e75af522a97cc111b102a873982))
* Update dependency org.slf4j:slf4j-api to v2.0.6 ([#150](https://github.com/googleapis/java-pubsublite-flink/issues/150)) ([9eb5ebc](https://github.com/googleapis/java-pubsublite-flink/commit/9eb5ebcb89ab2c4ce2fa721e43cbe1bda7138073))
* Update flink.version to v1.13.1 ([#46](https://github.com/googleapis/java-pubsublite-flink/issues/46)) ([56ce037](https://github.com/googleapis/java-pubsublite-flink/commit/56ce0370d14de0331223ca736bc70409e380b52d))
* Update flink.version to v1.13.2 ([#51](https://github.com/googleapis/java-pubsublite-flink/issues/51)) ([754e2c1](https://github.com/googleapis/java-pubsublite-flink/commit/754e2c18ac15d679430392bee3a5166832fea503))
* Update pubsublite.version to v0.18.0 ([#47](https://github.com/googleapis/java-pubsublite-flink/issues/47)) ([e4a88f3](https://github.com/googleapis/java-pubsublite-flink/commit/e4a88f39b7f5098fa0c36fc5daac9b602da358cc))
