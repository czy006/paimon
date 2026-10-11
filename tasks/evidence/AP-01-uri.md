<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# AP-01 URI boundary increment

Date: 2026-10-11. Base: release-1.3.2 / c05f7d1; branch codex/archive-partitions-132.
Runtime: Temurin11.0.32.1 with Maven3.9.16. Java source/target remains1.8; javap confirms major version52.
Status: URI component implemented and verified; AP-01 as a whole is NOT complete.

## Implemented scope

- Package-private ArchivePath: validates absolute hierarchical storage-root URIs before Paimon normalization, removes trailing slash, normalizes scheme/host and numeric port.
- Rejects root/bucket-only URI, local authority alias, query/fragment/userInfo, dot/encoded-dot paths, encoded separators, controls, nested escapes, malformed UTF-8 and unpaired UTF-16 surrogates.
- Compares scheme, server host ignoring case, numeric port, and complete path segments for containment/overlap. Different bucket/port/prefix does not qualify.
- Suppresses unsafe input/cause text in validation exceptions. No filesystem or network operations.
- Root ownership, symlink identity, supported plugins/credentials, persistent root registration, full partition typing/table/branch/history gates are not implemented by this helper. They remain AP-01/AP-08 work.
- Persistent external-paths use Paimon Path(String)'s unescaped-text semantics; target URI uses Path(URI). The forthcoming registration gate must compare actual physical paths without silently interchanging those parsers.

## RED / GREEN and independent review

All Maven invocations used the same command-local JDK11 prefix recorded in AP-00.

| Step / log in /tmp/paimon-archive-132.AFLqQ5 | Observed result |
|---|---|
| archive-path-red.log: -pl paimon-core -Dtest=ArchivePathTest test, before implementing ArchivePath | Exit1, test compilation fails for missing ArchivePath; expected missing-implementation RED, not a behavior-test result |
| archive-path-green.log: ArchivePathTest,FileStoreCommitTest,PartitionPredicateTest |71 tests passed: new29 + existing42; no failures/errors/skips |
| Independent read-only review |2 Required findings: host case/port alias overlap and invalid escaped UTF-8 destination rewriting |
| archive-path-review-red.log: added corresponding tests before fixes |36 tests,6 failures,0 errors/skips: one authority case + five malformed-UTF8 parameters; reproduces findings |
| archive-path-review-green.log: after fixes |36 passed,0 failures/errors/skips |
| JDK11 minimal experiment with raw unpaired surrogate |URI(String) accepts the raw surrogate; toASCIIString throws; motivated strict well-formed UTF-16 checking |
| archive-path-surrogate-red.log: two raw-surrogate cases before fix |38 tests,2 failures,0 errors/skips |
| archive-path-surrogate-green.log: after pair validation |38 passed,0 failures/errors/skips; valid supplementary character and literal U+FFFD also accepted |
| Independent final read-only review |No remaining Critical/Required in the URI component; full archiver explicitly excluded |

## Verification of the verification

Removed the sameAuthority containment guard in this isolated worktree, then ran:

```shell
env JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home /Users/conradjam/software/apache-maven-3.9.16/bin/mvn -B -ntp -pl paimon-core -Dtest=ArchivePathTest test
```

archive-path-mutation.log records38 tests and2 expected assertion failures:
containsOnlyMatchingSchemeAuthorityAndCompletePathSegments (different bucket incorrectly accepted), and
treatsHostCaseAndNumericPortAliasesAsTheSameAuthority (different port incorrectly accepted).
This was not a format/build failure. Restored the guard using apply_patch and verified the source SHA-256 exactly matched the pre-mutation candidate:
9b23650a5ff30575a020c720a19b21ba095ca59ac264e8b321dae3fee92600e4.

Final restored run, archive-path-final-tests.log:

- -pl paimon-core -Dtest=ArchivePathTest,FileStoreCommitTest,PartitionPredicateTest test.
- 80 actual tests: ArchivePath38, FileStoreCommit39, PartitionPredicate3.
- Failures0/errors0/skipped0; BUILD SUCCESS, exit0. Compilation, Checkstyle and Spotless validation passed.
- Root -N apache-rat:check also passed, exit0,0 unknown licenses (archive-path-root-license.log).

No mutation remains. No Spark procedure registered, archive data copied, snapshot replaced, MinIO container started, or real table migrated. The earlier11-test Spark baseline is AP-00 evidence, not archive behavior evidence. Temporary raw logs may expire; counts and commands above are the durable summary in this worktree.

## Sources

- [JDK11 URI components, equality and escape decoding](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/URI.html): host/port semantics and replacement-on-decoding-error explain the two review corrections.
- [JDK11 strict CharsetDecoder](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/charset/CharsetDecoder.html#decode(java.nio.ByteBuffer)): REPORT behavior rejects invalid byte sequences instead of substituting a destination character.
- This tag's paimon-api/.../fs/Path.java:144,189,339 confirms Path(String) vs Path(URI), normalization and unescaped toString behavior.
- Storage-root rejection policy is feature-specific; official Paimon docs do not document a built-in archive_partitions procedure.

## Next

Finish AP-01's table/partition/branch/persistent-root/FileIO guards using this tested helper; then implement fixed-snapshot plans. Keep AP-01 unchecked until its complete acceptance contract passes. No commit/push/PR is implied by this evidence.
