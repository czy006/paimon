# paimon-s3-native 原生 S3 FileIO（AWS SDK v2）实现 Spec

> **文档版本:** v2（2026-09-04，基于本地 Flink 源码逐类核实后重写）
>
> **给后续执行 Agent 的纪律（必须遵守）:**
> 1. 本 Spec 的下游消费者是 AI 实现者。**实现任何一个类之前，必须先打开 §3 索引中列出的对应 Flink 参考文件通读**，再按 §7 的溯源注释规范写代码。禁止凭印象实现。
> 2. 所有对 Flink 参考实现的行为偏离必须登记在 §5.5 偏离清单（D1–D8），代码中以 `[DEVIATION Dn]` 注释标注。未登记的偏离视为实现缺陷。
> 3. 里程碑 M0 是门禁（gate），三项 Spike 全过才准进入 M1。
> 4. Java 源码级别必须是 **Java 8**（禁止 `var`、`List.of`、`CompletableFuture.orTimeout`、`InputStream.readAllBytes` 等 9+ API；`java.time.*` 可用），产物须在 JDK 8 与 JDK 11 运行时均可用。
> 5. 除 §2.3 列出的新建文件与 §2.3 末尾注明的两处存量文件追加外，不得改动任何现有代码。

**目标:** 在 fork（release-1.3）新建 `paimon-s3-native` + `paimon-s3-native-impl` 模块对，实现纯 AWS SDK v2、零 Hadoop 依赖的 Paimon `FileIO`，替换数据面对 Hadoop S3AFileSystem + AWS SDK v1 的依赖。v1 范围：有界并发 multipart 异步写、`s3.*`/`fs.s3a.*` 配置兼容层、`VectoredReadable` 向量读。

**架构:** `S3NativeFileIO` 直接实现 `org.apache.paimon.fs.FileIO`（不经过 `HadoopCompliantFileIO`/Hadoop FileSystem 抽象）。元数据与读路径使用同步 `S3Client`（Apache HTTP 连接器），multipart part 上传与向量读使用异步 `S3AsyncClient`（Netty 连接器）。客户端由 `S3NativeClientProvider` 构建并按（解析后选项， authority）缓存。参考实现为本地 Apache Flink master 的 `flink-filesystems/flink-s3-fs-native`（FLINK-38592）；**不移植**其 RecoverableWriter、delegation token、entropy 注入、bulk copy、per-bucket 配置、metrics bridge、CRT 传输。

**技术栈:** Java 8（source/target，运行时兼容 8/11）、AWS SDK v2 2.44.4（`s3` / `netty-nio-client` / `apache-client`，随需带传递依赖）、Maven shade（含重定位与 SPI 合并）、JUnit 5、MinIO Testcontainers。

**已确认决策**（2026-09-04 与维护者确认，不得推翻）:

| 决策点 | 结论 |
| --- | --- |
| 归宿 | fork 先行（服务客户 `s3://prd-datalake`），按可上游化规范写，未来可提 PR 回 apache/paimon |
| 与 paimon-s3 关系 | 新模块对独立成 jar，部署二选一（换 jar 切换，回滚 = 换回旧 jar），不改动现有 paimon-s3 |
| v1 功能范围 | 异步写路径 + `fs.s3a.*` 兼容层 + VectoredReadable；CRT 排除 |
| 验收标准 | 功能 + 性能双门槛：行为测试全绿 + 基准写吞吐 ≥1.5x vs paimon-s3（默认值，可由维护者调整；客户灰度不进 Spec 验收） |

---

## 1. 背景与动机

1. **现状**：`paimon-filesystems/paimon-s3-impl` 的数据面为 `S3FileIO` → `HadoopCompliantFileIO` → `org.apache.hadoop.fs.s3a.S3AFileSystem`（hadoop-aws 3.3.4）→ AWS SDK v1 1.12.319。为 Java 8 兼容还手工补丁了 SDK v1 的 `XmlResponsesSaxParser`（`paimon-s3-impl/pom.xml` shade filter），维护成本高。
2. **SDK v1 已 EOL**（2025-12-31 官方停止支持），无后续安全修复。
3. **吞吐证据**：Flink 官方基准（cwiki 406620396，FLIP-555 / apache/flink#27187）显示 SDK v2 异步路径对 Presto S3（SDK v1 同步客户端）checkpoint 写吞吐 2.17x（~200 MB/s vs ~92 MB/s），P99 时长减半。S3A 底层同为 SDK v1 同步客户端，差距同样适用。
4. **S3A 路径做不到的能力**：`paimon-common` 的 `VectoredReadable` 接口（Parquet 列式批量读）S3A 路径未实现，原生实现可用 HTTP Range GET 补上。

## 2. 范围

### 2.1 做（v1）

- 新模块对：`paimon-filesystems/paimon-s3-native`（loader 壳 + 插件打包）+ `paimon-filesystems/paimon-s3-native-impl`（实现，shade 内嵌）。
- `S3NativeFileIO implements FileIO`：SPI 全部抽象方法的 S3 原生实现（§5 语义规范）。
- 目录语义采用 S3A 兼容的 **directory marker**（0 字节对象、key 以 `/` 结尾，见偏离 D1）。
- 客户端工厂 + 缓存：sync `S3Client`（ApacheHttpClient）、async `S3AsyncClient`（NettyNioAsyncHttpClient）。
- 输出流：本地临时文件按 part 缓冲，`S3AsyncClient.uploadPart` 有界并发上传，`CompleteMultipartUpload`/`AbortMultipartUpload` 收尾；小文件单 `PutObject` 捷径（偏离 D3）。
- 输入流：惰性 seek + 读缓冲 + Range 重开；`pread` 位置读（线程安全、不移动游标）使 default `readVectored` 生效。
- 配置兼容层：`s3.*` / `s3a.*` 原生键 + 常见 `fs.s3a.*` 别名（§6.3）。
- 插件打包（mirror `paimon-s3` 的 `paimon-plugin-s3-native/` 目录方案）与部署文档。
- 测试：`FileIOBehaviorTestBase` 的 S3 子类（MinIO）、移植 Flink 单测用例、基准脚本。

### 2.2 不做（v1 明确排除）

- CRT 传输（JNI、无法 shade 重定位、Flink 亦默认关闭）。
- `S3TransferManager`：经核实（§4 事实 F8），其在非 CRT 的 Netty 客户端上仅执行单请求 `PutObject`，不产生并行 multipart；我们的并发由自己控制（D3）。依赖也随之裁剪。
- Flink `NativeS3RecoverableWriter` 系（断点续传、side object、Committer）——Paimon 用自身 snapshot 提交协议。
- Delegation token、entropy 注入、bulk copy、per-bucket 配置（`s3.bucket.<name>.*`）、metrics bridge、STS assume-role、自定义凭证 provider 类列表（`fs.s3.aws.credentials.provider`）。
- SSE 加密透传（`S3EncryptionConfig` 移植成本极低，列为 M5 可选项）。
- 不改动/不替换现有 `paimon-s3`、`paimon-s3-impl`；不删 `XmlResponsesSaxParser` 补丁；不提上游 PR。

### 2.3 涉及文件（全部新建，另有两处存量文件追加）

| 模块 | 文件 | 移植来源（见 §3） |
| --- | --- | --- |
| `paimon-filesystems/paimon-s3-native` | `pom.xml`（mirror `paimon-s3/pom.xml` 的 unpack+shade 结构，目录名 `paimon-plugin-s3-native`） | `paimon-s3/pom.xml`（Paimon 自有，非 Flink） |
| | `src/main/java/org/apache/paimon/s3native/S3NativeLoader.java`（实现 `FileIOLoader`） | `paimon-s3/.../S3Loader.java`（Paimon 自有） |
| | `src/main/resources/META-INF/services/org.apache.paimon.fs.FileIOLoader` | 同上 |
| | `src/test/java/org/apache/paimon/s3native/S3NativeFileIOBehaviorTest.java` | 新写（挂接 Paimon `FileIOBehaviorTestBase`） |
| `paimon-filesystems/paimon-s3-native-impl` | `pom.xml`（SDK v2 依赖 + §6.7 shade） | `flink-s3-fs-native/pom.xml` |
| | `src/main/java/org/apache/paimon/s3native/S3NativeFileIO.java` | `NativeS3FileSystem` 语义映射 |
| | `src/main/java/org/apache/paimon/s3native/S3NativeClientProvider.java` | `S3ClientProvider`（裁剪） |
| | `src/main/java/org/apache/paimon/s3native/S3NativeOptions.java` | `NativeS3FileSystemFactory` ConfigOption 表 |
| | `src/main/java/org/apache/paimon/s3native/S3ConfigTranslator.java` | 新写（`S3FileIO` 的前缀映射思想） |
| | `src/main/java/org/apache/paimon/s3native/S3NativeObjectOperations.java` | `writer/NativeS3ObjectOperations`（同步操作移植 + 异步扩展） |
| | `src/main/java/org/apache/paimon/s3native/S3NativeSeekableInputStream.java` | `NativeS3InputStream` + `VectoredReadable` |
| | `src/main/java/org/apache/paimon/s3native/S3NativePositionOutputStream.java` | `NativeS3OutputStream` + `writer/NativeS3RecoverableFsDataOutputStream` 缓冲模式 |
| | `src/main/java/org/apache/paimon/s3native/S3PathUtils.java` | `NativeS3ObjectOperations.extractKey/extractBucketName` |
| | `src/main/java/org/apache/paimon/s3native/S3FileStatus.java`（如需独立类型；亦可直接用 Paimon `FileStatus` 实现类） | `S3FileStatus` |
| | `src/test/java/...`（§8.2 清单） | 对应 Flink 测试 |
| 基准 | `docs/analysis/benchmarks/s3-native-benchmark.md` + `tools/s3-native-bench/`（§8.4） | 新写 |
| 存量追加（仅追加，不改已有内容） | `docs/content/maintenance/filesystems.md`（追加 "S3 Native" 节） | — |
| | `paimon-filesystems/pom.xml`（`<modules>` 追加两行） | — |

> 包名 `org.apache.paimon.s3native`，与现有 `org.apache.paimon.s3` 隔离，避免同 classloader 冲突。

## 3. Flink 参考源码索引（实现前必读）

**本地参考仓库:** `/Users/SL/javaProject/flink`（branch `master`，验证时 HEAD `d5ff3fed338`，验证日期 2026-09-04）。
**参考模块:** `flink-filesystems/flink-s3-fs-native`（2.4-SNAPSHOT；FLINK-38592 引入，2026-03 合入；上游标注 `@Experimental`）。
**下文 `$FLINK_S3N` = `/Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native`。**

### 3.1 类级处置表

| # | Flink 文件（`$FLINK_S3N/src/main/java/org/apache/flink/fs/s3native/` 下） | 处置 | 移植到 | 复用要点 |
| --- | --- | --- | --- | --- |
| R1 | `S3ClientProvider.java`（1023 行） | **移植裁剪** | `S3NativeClientProvider` | Builder 模式；凭证链（自定义类→静态→Default，我们去掉委托令牌与自定义类）；`resolveRegion`（显式 → `DefaultAwsRegionProviderChain` → 抛错）；`S3Configuration`（pathStyle/chunkedEncoding/checksumValidation）；`StandardRetryStrategy`（maxAttempts=retries+1，`BackoffStrategy.exponentialDelay`）；Apache/Netty 两个 builder 的全部超时与 `maxConnections`；close 顺序（TM→async→sync→credentials，逐个 try/catch）。去掉：CRT、STS、metrics、`DynamicTemporaryAWSCredentialsProvider`。注意其 `closeAsync` 用了 `CompletableFuture.orTimeout`（Java 9+），Java 8 须改为同步关闭（见 D 注） |
| R2 | `NativeS3InputStream.java`（411 行） | **近乎原样移植** | `S3NativeSeekableInputStream` | `nextReadPos`/`streamPos` 双游标 + `lazySeek()`；前向 skip 阈值 = `readBufferSize`（在缓冲内消费，否则 Range 重开）；`targetPos >= contentLength` → EOF 释放流；`seek()` 仅记位置不发 IO；`skip()` 仅移指针；`ReentrantLock.lockInterruptibly` 全方法守护；close 时 `abort()` 避免 drain。追加 `pread` 实现（§6.6） |
| R3 | `NativeS3OutputStream.java`（214 行） | **模式移植** | `S3NativePositionOutputStream` 的小文件捷径分支 | 64KB `BufferedOutputStream` → 本地临时文件 `s3-upload-<uuid>` → close 单条 `putObject(RequestBody.fromFile)`；`fileUploaded` 恰一次标志；锁守护 write/flush/close |
| R4 | `writer/NativeS3RecoverableFsDataOutputStream.java` | **缓冲模式移植** | `S3NativePositionOutputStream` 的 part 缓冲 | 每 part 一个临时文件 + 64KB 缓冲；`currentPartSize >= minPartSize` 触发 `uploadCurrentPart()`；`completedParts`（partNumber+eTag）有序收集；异常路径 `tryAbortUploadAndReleaseResources`。去掉：persist/resume/side object（recoverable 语义） |
| R5 | `writer/NativeS3ObjectOperations.java`（580 行） | **移植扩展** | `S3NativeObjectOperations` | `startMultiPartUpload`（CreateMultipartUpload）、`uploadPart`（UploadPart + `RequestBody.fromFile`，改 async 重载见 D3）、`commitMultiPartUpload`（CompleteMultipartUpload + `NoSuchUploadException` 时 HeadObject 兜底——"commit 已成功但响应丢失"恢复语义）、`abortMultiPartUpload`、`putObject`、`getObjectMetadata`（HeadObject）、`deleteObject`（404→false 语义）、`extractKey/extractBucketName`。追加：batch delete（D6）、UploadPartCopy 大对象 rename（D7）、marker 操作（D1） |
| R6 | `NativeS3FileSystem.java` | **语义参考** | `S3NativeFileIO` | `getFileStatus`（HeadObject → NoSuchKey 转 prefix 目录判定 → FileNotFoundException；403 歧义日志）；`listStatus`（ListObjectsV2 prefix + delimiter `/` + continuationToken 分页；Contents→文件、CommonPrefixes→目录）；`delete`（分类→删除/递归）；`rename`（CopyObject+Delete，仅文件）；`create`（NO_OVERWRITE 先 exists 再抛 `IOException("File already exists")`）。**不照搬**的部分见 D1/D2/D4/D5 |
| R7 | `NativeS3FileSystemFactory.java`（761 行） | **选项表参考** | `S3NativeOptions` | 全部 ConfigOption 键名、默认值、校验（part size 5MB–5GB、maxConnections>0、readBufferSize 下限 256KB）。不移植：entropy/bulk-copy/CRT/metrics/SSE/assume-role/credentials-provider-class 选项 |
| R8 | `S3FileStatus.java` | 参考 | 目录/文件状态构造 | 文件（len、mtime）与目录（len=0）两种形态 |
| R9 | `S3ExceptionUtils.java`、`NativeS3FileIoUtils.java` | 参考 | 错误转换与临时文件工具 | `toIOException`、errorCode/errorMessage 提取；临时下载文件创建 |
| R10 | `writer/NativeS3RecoverableWriter.java`、`writer/NativeS3Committer.java`、`writer/NativeS3Recoverable*.java` | **不移植** | — | Paimon 无此需求 |
| R11 | `token/*`、`metrics/*`、`NativeS3BulkCopyHelper.java`、`S3BucketConfig.java`、`BucketConfigProvider.java`、`S3EncryptionConfig.java`（M5 可选）、`S3BlockLocation.java`、`NativeS3AFileSystemFactory.java` | **不移植** | — | 范围裁剪（SSE 除外，见 M5） |

### 3.2 Flink 测试参考（用例移植来源，`$FLINK_S3N/src/test/java/org/apache/flink/fs/s3native/`）

| Flink 测试 | 基础设施 | 我们的处理 |
| --- | --- | --- |
| `NativeS3InputStreamTest.java` | 纯单测（stub client，无容器） | 移植 seek/skip/lazySeek/EOF/close 用例到 `S3NativeSeekableInputStreamTest`（stub `S3Client`） |
| `S3ClientProviderTest.java` | 纯单测 | 移植凭证链/region 解析/超时配置断言 |
| `writer/NativeS3RecoverableFsDataOutputStreamTest.java` | 纯单测 | 移植 part 边界用例（恰好等于/跨越 `minPartSize`、尾 part、失败 abort）到输出流测试 |
| `NativeS3FileSystemITCase.java` | SeaweedFS Testcontainer（`SeaweedFsNativeS3TestContainer.java`） | 不引入 SeaweedFS；等价覆盖由 `S3NativeFileIOBehaviorTest`（MinIO）承担 |
| `NativeS3FileIoUtilsTest.java`、`S3ExceptionUtilsTest.java`、`S3FileStatusTest.java`、`BucketConfigProviderTest.java` 等 | 纯单测 | 按移植的对应类选择性移植 |

## 4. 已核实的关键技术事实（实现依据，不得凭记忆推翻）

以下事实全部于 2026-09-04 在本地 Flink 源码逐条核实：

- **F1 SDK 版本**：`fs.s3.aws.sdk.version = 2.44.4`（`$FLINK_S3N/pom.xml:34`）。依赖 `software.amazon.awssdk:s3 / s3-transfer-manager / sts / netty-nio-client / apache-client / aws-crt-client`；我们不引入 `s3-transfer-manager`、`sts`、`aws-crt-client`。
- **F2 零 Hadoop**：模块无任何 `org.apache.hadoop` 依赖。
- **F3 默认值**（`NativeS3FileSystemFactory`）：`s3.upload.min.part.size` 默认 **5MB**（`S3_MULTIPART_MIN_PART_SIZE = 5L << 20`，合法区间 5MB–5GB）；`s3.upload.max.concurrent.uploads` 默认 **CPU 核数**；`s3.read.buffer.size` 默认 **256KB** 且代码下限钳制 256KB；`s3.connection.max` 默认 **50**（同步与异步客户端共用）；`s3.connection.timeout` / `s3.socket.timeout` / `s3.connection.max-idle-time` 默认各 **60s**；`s3.retry.max-num-retries` 默认 3（`maxAttempts = retries + 1`）、`s3.retry.base-delay` 100ms、`s3.retry.throttle.base-delay` 1s、`s3.retry.max-backoff` 20s、circuit breaker 默认关闭；`s3.chunked-encoding.enabled` 与 `s3.checksum-validation.enabled` 默认 true（MinIO 等 S3 兼容存储不兼容时可关闭——两个开关必须保留）。
- **F4 Region 语义**：`s3.region` 无默认值。未配置时走 `DefaultAwsRegionProviderChain`（`AWS_REGION` 环境变量 → `~/.aws/config` → EC2 元数据），全部失败抛 `IllegalArgumentException`，**不回退 us-east-1**。
- **F5 凭证链**：自定义 provider 类（我们裁掉）→ `StaticCredentialsProvider`（access/secret 均非空时）→ 委托令牌（我们裁掉）→ `DefaultCredentialsProvider`，用 `AwsCredentialsProviderChain` 组装。
- **F6 客户端构建**：同步 `S3Client.builder().httpClientBuilder(ApacheHttpClient.builder().maxConnections(..).connectionTimeout(..).socketTimeout(..).tcpKeepAlive(true).connectionMaxIdleTime(..))`；异步 `S3AsyncClient.builder().httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(..).connectionTimeout(..).readTimeout(..).connectionAcquisitionTimeout(..))`；两者均 `.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(..).chunkedEncodingEnabled(..).checksumValidationEnabled(..).build())`、`.overrideConfiguration(ClientOverrideConfiguration)`、可选 `.endpointOverride(URI)`。重试经 `ClientOverrideConfiguration.retryStrategy(StandardRetryStrategy...)`。
- **F7 系统属性兜底**：`S3ClientProvider.Builder.build()` 里 `endpoint` 缺省回退 `System.getProperty("s3.endpoint")`，`pathStyleAccess` 回退 `System.getProperty("s3.path.style.access")`（测试基建依赖此机制，**必须保留**）。
- **F8 TransferManager 真相**：`S3TransferManager` 仅在 CRT 客户端上才执行并行 multipart；Netty 客户端上是单请求传输。Flink 的 `NativeS3ObjectOperations.uploadPart` 用的是**同步** `s3Client.uploadPart`（顺序上传）；非 recoverable 的 `NativeS3OutputStream` 是整文件临时文件 + 单条 `PutObject`。因此我们的并发 multipart 必须自己用 `S3AsyncClient.uploadPart(request, AsyncRequestBody.fromFile(file))` 有界并发实现（偏离 D3）。
- **F9 getFileStatus**：HeadObject；`NoSuchKeyException` → prefix 目录判定（ListObjectsV2 maxKeys=1）；其 `contentLength==0/null` 即视为目录的写法会把 **0 字节普通文件误判为目录**，我们不采纳（偏离 D4）。
- **F10 mkdirs**：Flink 恒 `true`、**不落目录 marker**。该语义过不了 Paimon 行为套件 `testMkdirsReturnsTrueWhenCreatingDirectory`（要求 `mkdirs(dir)` 后 `exists(dir) == true`）与 `testMkdirsCreatesParentDirectories`，因此必须采用 marker（偏离 D1）。
- **F11 delete**：文件 → DeleteObject；路径不存在 → 捕获 `FileNotFoundException` 返回 false；目录 + `recursive=false` → 一律抛 `IOException`（含空目录）；递归 → `listStatus` 逐对象递归删除。
- **F12 rename**：仅文件；`CopyObject` + `DeleteObject`；目录抛 `UnsupportedOperationException`（我们须支持目录 rename，偏离 D2）。
- **F13 close 顺序**：`transferManager → asyncClient → s3Client → credentialsProvider（若 SdkAutoCloseable）`，逐个 try/catch 吃掉异常；Flink 用 `CompletableFuture.orTimeout`（Java 9+）做超时，Java 8 实现为顺序同步关闭（无 TM 后更简单）。
- **F14 shade 配置**（`$FLINK_S3N/pom.xml`）：重定位 `software.amazon.awssdk`（排除 `software.amazon.awssdk.crt.**`，JNI 不可重定位——我们无 CRT 仍保留排除以防万一）、`org.apache.http`、`org.apache.commons.logging`、`io.netty`、`com.typesafe.netty`、`org.reactivestreams`；`ServicesResourceTransformer`（**必须**，SDK v2 用 ServiceLoader 发现 HTTP 连接器）+ `ManifestResourceTransformer`；剔除签名文件。
- **F15 jackson 风险（新增，Flink 未处理）**：SDK v2 传递依赖 Jackson。Paimon 的 `ComponentClassLoader` 对非 `org.apache.paimon.**` 类是 **parent-first**，宿主（Flink/Spark）自带的 Jackson 版本可能与 SDK 2.44.4 要求不匹配导致 `NoSuchMethodError`。**必须额外重定位 `com.fasterxml.jackson`**（Flink 无此问题因 Flink 插件 classloader 全 child-first）。M0-S3 冒烟覆盖。

## 5. 语义规范（FileIO SPI → S3 操作）

### 5.0 术语约定

- **key**：S3 对象键，`s3://bucket/a/b` → bucket=`bucket`，key=`a/b`（去头部 `/`，与 Flink `extractKey` 一致）。
- **directory marker**：key 为 `<dir>/`（以 `/` 结尾）的 0 字节对象，代表目录的存在性（S3A 兼容语义）。
- **prefix 判定**：对 key 前缀 `p/` 执行 `ListObjectsV2(maxKeys=1)`，有结果即"目录有内容"。

### 5.1 方法映射总表

| FileIO 方法 | S3 操作 | 语义 |
| --- | --- | --- |
| `isObjectStore()` | — | `true` |
| `configure(CatalogContext)` | — | `S3ConfigTranslator` 解析（含别名展开与未知键 WARN），存入实例；客户端缓存键含解析结果 |
| `newInputStream(Path)` | HeadObject 取 `contentLength`，随后惰性 `GetObject` | 返回 `S3NativeSeekableInputStream`；文件不存在抛 `FileNotFoundException`（来自 HeadObject） |
| `newOutputStream(Path, overwrite)` | 见 §6.5 | `overwrite=false` 且对象存在（HeadObject 命中非 marker key）→ `IOException("File already exists: ...")`（对齐 Flink `create(NO_OVERWRITE)` 行为）；`overwrite=true` 直接写（S3 PUT 天然覆盖，无需先删，比 Flink 的先 delete 少一次请求） |
| `getFileStatus(Path)` | HeadObject(key) → HeadObject(key+"/") → prefix 判定 | 依序：①key 是非 marker 对象 → 文件状态（**0 字节也是文件**，D4）；②key+"/" 是 marker → 目录状态（len=0, isDir=true）；③prefix 有内容 → 目录状态；④否则 `FileNotFoundException` |
| `listStatus(Path)` | ListObjectsV2(prefix=key+"/", delimiter="/") + continuationToken 分页 | `contents` 中 key 以 `/` 结尾的 marker **跳过**；其余为文件项；`commonPrefixes` 为目录项。路径是文件时返回单元素数组（先 getFileStatus 判定）。结果**不保证顺序** |
| `exists(Path)` | 同 getFileStatus 的①②③ | 任一命中即 true |
| `delete(Path, recursive)` | 见 5.2 | |
| `mkdirs(Path)` | PutObject marker | 见 5.3 |
| `rename(src, dst)` | CopyObject/DeleteObject（或 UploadPartCopy，D7） | 见 5.4 |

### 5.2 delete 精确语义

输入分类（同 5.1 ①②③④）：

1. **不存在（④）** → 返回 `false`，不抛错（F11 一致，行为套件 `testNotExistingFileDeletion` 覆盖）。
2. **文件（①）** → `DeleteObject`，返回 `true`。
3. **目录 + `recursive=false`**：列出该 prefix 下**非 marker** 子项；为空 → 删除 marker，返回 `true`（行为套件 `testExistingEmptyDirectoryDeletion`：删后 `exists == false`）；非空 → `IOException`（`testExistingNonEmptyDirectoryDeletion`）。
4. **目录 + `recursive=true`**：`ListObjectsV2`（**无 delimiter**，取该 prefix 下全部 key，含子目录 marker）分页收集，按 ≤1000/批 `DeleteObjects`（batch delete，D6）；批响应中 `NotFound` 的 key 视为成功，其余 per-key 错误抛 `IOException`；返回 `true`。

### 5.3 mkdirs 精确语义（D1）

1. HeadObject(key)：命中**非 marker** 对象 → `IOException`（`testMkdirsFailsForExistingFile`）。
2. 逐级祖先 HeadObject：任一祖先是普通对象 → `IOException`（`testMkdirsFailsWithExistingParentFile`）。
3. 否则 `PutObject(key + "/", 空 body)`（marker，幂等：已存在则跳过），返回 `true`。
4. 目录已有子内容（无 marker）→ 直接返回 `true`（`testMkdirsReturnsTrueForExistingDirectory`），**不**要求补建 marker。

> 根路径（key 为空）不建 marker。

### 5.4 rename 精确语义（D2/D7）

1. src 分类为④ → 返回 `false`。
2. dst 已存在（①②③任一命中）→ 返回 `false`（不覆盖）。
3. **文件 → 文件**：HeadObject 取 contentLength；`≤5GB` → `CopyObject` + `DeleteObject`（F12 模式）；`>5GB` → `CreateMultipartUpload` + `UploadPartCopy` 分段（每段 5GB）+ `CompleteMultipartUpload` + `DeleteObject`（D7，S3A 对齐）。任一步失败抛 `IOException`。
4. **目录 → 目录**（D2，Flink 不支持，我们必须支持）：ListObjectsV2 无 delimiter 列出 src prefix 下全部 key（含 marker）→ 对每个 key 依 3 的规则复制到 dst prefix（对象 ≤5GB 用 CopyObject；>5GB 用 UploadPartCopy 链）→ 全部成功后按批删除 src 侧 key → 返回 `true`。**非原子**（与 S3A 现状一致；Paimon 提交协议对 rename 的使用不依赖跨对象原子性，见 §10 R6）。
5. 文件 ↔ 目录交叉：src 是目录而 dst 是已存在文件 → `IOException`；其余交叉按 dst 父目录存在性自然处理（对象存储无强制约束）。

### 5.5 偏离清单（对 Flink 参考实现的有意偏离，代码中 `[DEVIATION Dn]` 标注）

| # | 偏离 | 理由（证据） |
| --- | --- | --- |
| D1 | **目录 marker**：Flink 不落 marker（F10），我们落 | 行为套件 `testMkdirsReturnsTrueWhenCreatingDirectory`/`testMkdirsCreatesParentDirectories` 要求空目录 `exists == true`；S3A 现状同样落 marker，保持迁移等价 |
| D2 | **目录 rename**：Flink 抛 `UnsupportedOperationException`（F12），我们递归实现 | Paimon 现有 S3A 路径支持目录 rename，砍掉属回归 |
| D3 | **part 并发上传**：Flink 同步顺序 `uploadPart`（F8），我们用 `S3AsyncClient.uploadPart` + `AsyncRequestBody.fromFile` 有界并发 | 本 Spec 的吞吐目标来源；TransferManager 并行 multipart 仅 CRT 可用而 CRT 被排除 |
| D4 | **0 字节文件判定**：Flink 以 `contentLength==0` 疑似目录（F9），我们以 key 是否 `/` 结尾区分 marker 与空文件 | Flink 写法会把合法空文件误判为目录 |
| D5 | **delete 目录非递归**：Flink 一律抛异常（F11），我们空目录（仅 marker）放行 | 行为套件 `testExistingEmptyDirectoryDeletion` |
| D6 | **递归删除批量化**：Flink 逐对象递归（F11），我们 `DeleteObjects` 批 ≤1000 | 分区过期/快照过期会删大量对象，逐个删是吞吐瓶颈 |
| D7 | **大对象 rename**：Flink 单 `CopyObject`（上限 5GB），>5GB 走 `UploadPartCopy` | S3 API 硬约束；S3A 对齐 |
| D8 | **范围裁剪**：无 STS/委托令牌/CRT/metrics/entropy/bulk-copy/per-bucket 配置/自定义凭证类/SSE(M5 可选)/TransferManager | v1 范围决策（§2.2）；F8 使 TransferManager 无收益 |

## 6. 详细设计

### 6.1 模块与生命周期

```
paimon-filesystems/paimon-s3-native            (loader 壳, 2 类, 无重依赖)
  S3NativeLoader implements FileIOLoader
    getScheme()       -> "s3"              // 与 paimon-s3 相同 → 部署互斥（§3 已确认决策）
    plugin 目录        -> "paimon-plugin-s3-native"
    impl 类           -> "org.apache.paimon.s3native.S3NativeFileIO"
    requiredOptions() -> s3.access-key|s3.access.key、s3.secret-key|s3.secret.key（与 S3Loader 完全一致）
  S3NativePluginFileIO extends PluginFileIO（内部类，isObjectStore()=true，mirror S3Loader.S3PluginFileIO）

paimon-filesystems/paimon-s3-native-impl        (实现, shade 进上述目录)
  S3NativeFileIO implements FileIO, Closeable
```

- 客户端缓存：`S3NativeFileIO` 内 `static final ConcurrentHashMap<CacheKey, S3NativeClientProvider>`，`CacheKey =（S3NativeOptions 解析快照, authority）`（mirror 现有 `S3FileIO.CACHE` 模式，含同样的"不主动回收"取舍，§10 R5）。authority 为 null 时用 `"DEFAULT"`。
- `close()`：v1 no-op（与 `S3FileIO` 对齐，缓存常驻）；JVM shutdown hook best-effort 逐个关闭（F13 顺序，Java 8 同步版）。
- **多 bucket**：每个 Path 的 authority 即 bucket；不同 authority 各自的缓存实例共享同一 `S3NativeOptions`。

### 6.2 `S3NativeClientProvider`（R1 裁剪版）

```
syncClient  = S3Client.builder()
    .httpClientBuilder(ApacheHttpClient.builder()
        .maxConnections(o.maxConnections)          // 默认 50
        .connectionTimeout(o.connectionTimeout)     // 60s
        .socketTimeout(o.socketTimeout)             // 60s
        .tcpKeepAlive(true)
        .connectionMaxIdleTime(o.maxIdleTime))      // 60s
    .region(resolveRegion(o.region))                // F4 语义，失败抛 IllegalArgumentException
    .credentialsProvider(chain)                     // 静态 → DefaultCredentialsProvider（F5 裁剪版）
    .serviceConfiguration(S3Configuration.builder()
        .pathStyleAccessEnabled(o.pathStyle)        // 默认 false
        .chunkedEncodingEnabled(o.chunkedEncoding)  // 默认 true
        .checksumValidationEnabled(o.checksumValidation)) // 默认 true
    .overrideConfiguration(ClientOverrideConfiguration.builder()
        .retryStrategy(StandardRetryStrategy.builder()
            .maxAttempts(o.maxRetries + 1)          // 默认 4
            .backoffStrategy(BackoffStrategy.exponentialDelay(100ms, 20s))
            .throttlingBackoffStrategy(BackoffStrategy.exponentialDelay(1s, 20s))
            .circuitBreakerEnabled(false)))
    [endpointOverride(URI.create(o.endpoint))]      // 存在时

asyncClient = S3AsyncClient.builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder()
        .maxConcurrency(o.maxConnections)           // 与同步共用 s3.connection.max
        .connectionTimeout(...) .readTimeout(o.socketTimeout)
        .connectionAcquisitionTimeout(o.connectionTimeout))
    ...（region/credentials/serviceConfiguration/override/endpoint 同上）

// Builder.build() 系统属性兜底（F7，测试依赖）：endpoint ← "s3.endpoint"，pathStyle ← "s3.path.style.access"
// close()：asyncClient.close() → syncClient.close() → credentialsProvider(若 SdkAutoCloseable).close()，
//          逐个 try/catch，Java 8 同步实现（无 orTimeout）
```

不构建 `S3TransferManager`（F8/D8）。

### 6.3 配置层（`S3NativeOptions` + `S3ConfigTranslator`）

原生键（Paimon options），默认值全部与 Flink F3 核实值一致：

| 原生键 | 默认 | 兼容别名（值优先级：`s3.*` > `s3a.*` > `fs.s3a.*`） |
| --- | --- | --- |
| `s3.access-key` | 必填* | `s3.access.key`、`fs.s3a.access.key`、`fs.s3a.access-key` |
| `s3.secret-key` | 必填* | `s3.secret.key`、`fs.s3a.secret.key`、`fs.s3a.secret-key` |
| `s3.endpoint` | 无 | `fs.s3a.endpoint` |
| `s3.region` | 无（F4 探测） | `fs.s3a.endpoint.region` |
| `s3.path-style-access` | false | `s3.path.style.access`、`fs.s3a.path.style.access`、`fs.s3a.path-style-access` |
| `s3.upload.min.part.size` | 5MB（钳制 5MB–5GB） | `fs.s3a.multipart.size` |
| `s3.upload.max.concurrent.uploads` | CPU 核数 | —（`fs.s3a.uploads.max` 等未识别键按未知键 WARN 丢弃） |
| `s3.read.buffer.size` | 256KB（下限钳制 256KB） | `fs.s3a.readahead.range` |
| `s3.connection.max` | 50 | `fs.s3a.connection.maximum` |
| `s3.connection.timeout` | 60s | `fs.s3a.connection.timeout` |
| `s3.socket.timeout` | 60s | `fs.s3a.socket.timeout` |
| `s3.connection.max-idle-time` | 60s | — |
| `s3.retry.max-num-retries` | 3 | `fs.s3a.attempts.maximum`（值 = attempts，映射为 attempts-1） |
| `s3.chunked-encoding.enabled` | true | — |
| `s3.checksum-validation.enabled` | true | — |
| `s3.upload.tmp.dir` | `java.io.tmpdir` | — |

\* `requiredOptions()` 要求静态 key（与 `S3Loader` 一致，保证 loader 不被跳过）；凭证链内仍含 `DefaultCredentialsProvider` 兜底（静态 key 错误时由 SDK 报错）。

规则：

- `S3ConfigTranslator` 遍历 `CatalogContext` 全量 options：`s3.*`/`s3a.*` 前缀直收（键归一为 `s3.*`）；白名单别名按上表映射；**未识别的 `fs.s3a.*` 键打 WARN 并忽略**（无法透传 S3A 全语义，显式丢弃优于静默错配）。
- 数值解析失败 → `IllegalArgumentException`（fail fast，含键名）。
- 客户迁移验收点：现有 `s3.endpoint` / `s3.access-key` / `s3.secret-key` / `s3.path-style-access` 配置**换 jar 后零改动可用**。
- 测试基建依赖系统属性兜底（F7）：`S3NativeOptions` 解析时 options 缺省则读 `-Ds3.endpoint` / `-Ds3.path.style.access`。

### 6.4 `S3NativeObjectOperations`（R5 移植扩展）

同步操作（直接移植）：`getObjectMetadata`(HeadObject)、`putObject`(PutObject, `RequestBody.fromFile`)、`startMultiPartUpload`(CreateMultipartUpload)、`commitMultiPartUpload`（CompleteMultipartUpload + `NoSuchUploadException` → HeadObject 兜底语义，F/R5 要点）、`abortMultiPartUpload`、`deleteObject`（404→false）、`extractKey/extractBucketName`（移入 `S3PathUtils`）。

新增：

- `uploadPartAsync(key, uploadId, partNumber, File, length)` → `CompletableFuture<CompletedPart>`：`asyncClient.uploadPart(UploadPartRequest, AsyncRequestBody.fromFile(file))`，映射 eTag（D3）。
- `putObjectAsync(key, File)` → `CompletableFuture<String>`（eTag）：小文件捷径用。
- `deleteBatch(List<String> keys)`：`DeleteObjects`（≤1000/批，D6）。
- `copyObjectMultipart(srcKey, dstKey, length)`：`>5GB` 的 `UploadPartCopy` 链（D7）。
- marker 工具：`putMarker(key)` / `headMarker(key)`。

### 6.5 `S3NativePositionOutputStream`（写路径核心）

缓冲模式 = R4（每 part 临时文件 + 64KB `BufferedOutputStream`），上传模式 = D3（有界并发 async），收尾模式 = R3（小文件单 PUT）：

```
write(b)  -> 64KB 缓冲 -> 当前 part 临时文件（s3.upload.tmp.dir/paimon-s3-native-<uuid>-part<N>）
             currentPartSize >= minPartSize 时:
               首次触发: createMultipartUpload 得 uploadId
               submitPart(): asyncClient.uploadPart(...fromFile)  // in-flight ≤ max.concurrent.uploads
                             完成时收集 CompletedPart(partNumber, eTag) 并删该临时文件
close()   -> 尾部数据:
             无已上传 part 且总字节 < minPartSize -> 单条 asyncClient.putObject(fromFile(整个临时文件))  // R3 捷径
             否则 -> 尾 part 作为最后一个 part submit -> 等待全部 in-flight future
                     -> commitMultiPartUpload(按 partNumber 升序的 CompletedPart 列表)   // F/R5 语义
失败路径  -> 任一 future 异常: AbortMultipartUpload + 删除未删临时文件 + 抛 IOException（因果链完整）
getPos()  -> 已写字节计数
flush()   -> 刷 64KB 缓冲到临时文件（无害；对象存储无中途可见性，语义与 S3A 一致）
write after close -> IOException("Stream is closed")   // R3 的 fileUploaded 恰一次语义
线程模型  -> ReentrantLock 守护 write/flush/close（R3/R4 同款；Paimon 单写者假设仍加锁防误用）
```

- `newOutputStream(path, false)` 前置校验：HeadObject(key) 命中非 marker → `IOException("File already exists: " + path)`。
- part 上传失败语义：首个异常即触发 abort 路径；SDK 层重试已由 `StandardRetryStrategy` 承担（客户端不做额外重试）。
- 临时文件泄漏防护：实例持全部未删临时文件路径，close/abort 的 finally 里逐一 best-effort 删除。

### 6.6 `S3NativeSeekableInputStream`（读路径 + 向量读）

- 基础流：R2 近乎原样移植（`nextReadPos`/`streamPos`、`lazySeek`、缓冲内 skip 阈值 = `readBufferSize`、Range `bytes=<pos>-` 重开、EOF 释放、`abort()` on close、`lockInterruptibly` 全方法守护、`available()`、`skip()` 不发 IO）。
- 构造时 `contentLength` 由 `newInputStream` 的 HeadObject 提供（Flink `open()` 同款）。
- **VectoredReadable**（`paimon-common/src/main/java/org/apache/paimon/fs/VectoredReadable.java`，签名以下列为准）：
  - 必须实现：`int pread(long position, byte[] buffer, int offset, int length)` —— 独立发一条 `GetObject(range="bytes=position-(position+length-1)")` 同步请求，读满入 buffer；**不改动 `nextReadPos`/`streamPos`，不复用当前流**（线程安全天然成立）；越界按 SPI 抛 EOF/IO 语义。
  - 继承 default：`preadFully`、`minSeekForVectorReads()`=256KB、`batchSizeForVectorReads()`=4MB、`parallelismForVectorReads()`=4、`readVectored(List<? extends FileRange>)`（default 走 `VectoredReadUtils.readVectored(this, ranges)`，内部按上述参数合并/并行调用 `pread`）——**v1 不必重写 `readVectored`**。
  - M3 优化项（可选，基准驱动）：重写 `readVectored` 用 `asyncClient` 批量 Range GET；仅当基准显示 default 实现成为瓶颈才做。

### 6.7 shade 与打包（`paimon-s3-native-impl/pom.xml`）

mirror `paimon-s3/pom.xml` 的结构（dependency-plugin 把 impl jar unpack 进 `target/classes/paimon-plugin-s3-native/`，壳模块整体 shade）。impl 侧：

1. 依赖：`software.amazon.awssdk:s3`、`netty-nio-client`、`apache-client`（版本属性 `fs.s3native.awssdk.version`，初值 **2.44.4**）。全部 compile 打包。**不引入** `s3-transfer-manager`/`sts`/`aws-crt-client`。`paimon-common` 为 provided。零 hadoop 依赖。
2. 重定位（F14 + F15）：`software.amazon.awssdk` → `org.apache.paimon.s3native.shaded.software.amazon.awssdk`（排除 `software.amazon.awssdk.crt.**`）；`io.netty`、`com.typesafe.netty`、`org.reactivestreams`、`org.apache.http`、`org.apache.commons.logging`、**`com.fasterxml.jackson`**（F15，Paimon classloader parent-first 所必需）→ 统一前缀 `org.apache.paimon.s3native.shaded.`。
3. Transformer：`ServicesResourceTransformer`（必须，SDK HTTP 连接器 SPI）+ `ManifestResourceTransformer`；剔除 `META-INF/*.SF|*.RSA|*.DSA` 与 `META-INF/maven/**`。
4. 不设 Multi-Release 特殊处理（Netty 4.x 基线 Java 8；若 M0 发现必须再补）。

### 6.8 文档（`docs/content/maintenance/filesystems.md` 追加节）

追加 "S3 Native (paimon-s3-native)"：能力差异表（vs paimon-s3）；**部署互斥警告**（与 `paimon-s3` 二选一，同放会 duplicate-scheme 报错，报错文案摘录）；完整选项表（§6.3）；marker 语义说明（与 S3A 一致）；回滚步骤（换回旧 jar）；基准数字（M5 后回填）。

## 7. 溯源注释规范（实现强制）

每个移植/参考 Flink 的源文件，头部 javadoc 后追加：

```java
// [PORTED] Derived from Apache Flink flink-filesystems/flink-s3-fs-native (FLINK-38592,
// Apache License 2.0), class org.apache.flink.fs.s3native.S3ClientProvider.
// Local reference: /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/
// src/main/java/org/apache/flink/fs/s3native/S3ClientProvider.java
```

方法级标注：

- `[PORTED] <FlinkClassName>#<method>`：逻辑近乎照搬（允许机械适配，如 Flink `Path` → Paimon `Path`）。
- `[ADAPTED] <FlinkClassName>#<method>`：基于参考逻辑修改（注释说明改动点）。
- `[DEVIATION Dn]`：登记过的偏离（§5.5），必须写明理由编号。
- 完全新写的类/方法：`[NEW]`，不标 Flink 出处。

`NOTICE`（impl 模块根）：追加一段说明 derived from Apache Flink FLINK-38592（Apache-2.0）。

## 8. 测试计划

### 8.1 行为测试（验收底线）

`S3NativeFileIOBehaviorTest extends FileIOBehaviorTestBase`（`paimon-common/src/test/java/org/apache/paimon/fs/FileIOBehaviorTestBase.java` 的全部用例必须全绿），MinIO via `MinioTestContainer`（`paimon-filesystems/paimon-s3/src/test/java/org/apache/paimon/s3/MinioTestContainer.java`，以 test-jar 依赖引入；不可行则复制并注明来源）。测试配置在 `getS3ConfigOptions()` 基础上追加 `s3.region=us-east-1`（F4：region 探测在测试环境必然失败）。重点用例与语义映射：

| 行为套件用例 | 我们实现的关键路径 |
| --- | --- |
| `testMkdirsReturnsTrueWhenCreatingDirectory` / `testMkdirsCreatesParentDirectories` | D1 marker 落盘 + exists ② |
| `testMkdirsFailsForExistingFile` / `testMkdirsFailsWithExistingParentFile` | §5.3 步骤 1/2 |
| `testExistingEmptyDirectoryDeletion` | §5.2 情形 3 的空目录分支（D5） |
| `testExistingNonEmptyDirectoryDeletion` | §5.2 情形 3 抛异常 |
| `testExistingNonEmptyDirectoryWithSubDirRecursiveDeletion` | §5.2 情形 4（含子目录 marker 的批量删除） |
| `testNotExistingFileDeletion` | §5.2 情形 1 |
| `testListFilesIterative*` | `FileIO.listFilesIterative` default（基于 listStatus） |
| `testFileDoesNotExist` / `testRenameToNotExistingFile` 等 | §5.1 各行 |

### 8.2 单元测试（对照 Flink 测试移植，§3.2）

| 测试类 | 移植自 | 关键用例 |
| --- | --- | --- |
| `S3NativeSeekableInputStreamTest`（stub `S3Client`） | `NativeS3InputStreamTest` | seek 后 read 才发 IO（惰性）；前向小步缓冲内 skip；跨缓冲 Range 重开；EOF；close 后读抛错；`skip()` 不发 IO；`pread` 不动游标、并发 `pread` 正确 |
| `S3NativeClientProviderTest` | `S3ClientProviderTest` | 静态凭证优先于 Default；region 显式/探测失败抛错；endpoint+pathStyle 透传；系统属性兜底 |
| `S3NativePositionOutputStreamTest` | `writer/NativeS3RecoverableFsDataOutputStreamTest` | 写满恰一个 part 触发 submit；跨越 part 边界切分正确；总字节 < minPartSize 走单 PUT 捷径；part future 异常触发 abort；close 后写抛错；getPos 单调 |
| `S3ConfigTranslatorTest` | 新写 | 别名优先级、未知 `fs.s3a.*` WARN、数值解析 fail fast |
| `S3PathUtilsTest` | `NativeS3FileIoUtilsTest` 相关用例 | key 提取（含头部 `/`、无 authority 抛错） |
| `S3NativeObjectOperationsTest`（MinIO，@Tag IT） | `NativeS3FileSystemITCase` 相关用例 | multipart 全链路、NoSuchUpload 兜底、batch delete、UploadPartCopy >5GB（MinIO 支持）、marker put/head |

### 8.3 加固约定

- 所有 S3 集成测试统一 `@Tag("s3-it")`，CI 可选执行；MinIO 容器复用 `MinioTestContainer`。
- 断言风格与 Paimon 现状一致（AssertJ，JUnit 5）。

### 8.4 性能基准（验收硬指标）

- `tools/s3-native-bench/`：独立 Java main，同一参数分别驱动 `paimon-s3`（S3A）与 `paimon-s3-native` 两个 jar 的 FileIO：写 1GB × 5（part 16MB、并发 64——两实现均显式配置对等参数）、读回校验；记录 MB/s 与 p99。真实 S3 端点（客户 staging）出验收数字；MinIO 本地仅功能冒烟（磁盘是瓶颈，数字只做回归对比）。
- **门槛（默认，可调）**：写吞吐 ≥ **1.5×** paimon-s3，读吞吐 ≥ **0.9×**。达标后回填 §6.8 文档。

## 9. M0 技术验证 Spike（门禁，`tools/s3-native-bench/spike/`，不进主工程）

> **执行记录（2026-09-04，全部通过，进入 M1）**：S1——awssdk 2.44.4 全部 jar、netty、jackson 2.19.4、httpclient5 基础 class 均 major 52；唯一 >52 的是各 MR-JAR 的 `META-INF/versions/**` 条目（Java 8 不加载）与 `module-info.class`（Java 8 忽略），shade 时排除 `module-info.class`、保留 `META-INF/versions/**`。S2——JDK 8（zulu 1.8.0_504）对 MinIO：`S3AsyncClient.uploadPart + AsyncRequestBody.fromFile` ×8 并发 + Complete + HeadObject 长度校验 + 异步小文件 PutObject + Range GET + marker put/head/delete 全部 PASS。S3——按 §6.7 全部重定位（含 jackson）+ ServicesResourceTransformer 的 shaded jar 在 JDK 8 与 JDK 11 上全链路 PASS，重定位经异常栈确认生效。

| # | 验证 | 通过标准 | 不过的备选路径 |
| --- | --- | --- | --- |
| S1 | SDK 2.44.4 及 netty-nio-client 传递依赖的字节码版本（`javap -v` 抽查 jar 内 class） | 全部 major version ≤ 52（Java 8） | 沿 2.x 线降级至最后兼容版，更新 `fs.s3native.awssdk.version` 并重跑 S1 |
| S2 | **JDK 8 运行时**对 MinIO：`S3AsyncClient.uploadPart + AsyncRequestBody.fromFile` ×8 并发 + Complete + GetObject 全链路 | 全链路成功 | 若 async part 路径在 8 上不可用：降级为同步并发（线程池 + sync uploadPart），语义不变 |
| S3 | 按 §6.7 重定位（含 jackson）+ `ServicesResourceTransformer` 打 demo fat jar，**JDK 8** 下 async client 经 SPI 正常构建、发请求 | 请求成功且无 `NoSuchMethodError`/`ServiceConfigurationError` | 逐项排查重定位缺口；jackson 版本显式钉住 |

## 10. 里程碑

| 里程碑 | 内容 | 预估 | 完成定义 |
| --- | --- | --- | --- |
| M0 | §9 三项 Spike | 0.5d | S1/S2/S3 全过并留记录 |
| M1 | 模块骨架（两 pom + loader + services 文件）+ `S3NativeOptions`/`S3ConfigTranslator` + `S3NativeClientProvider` + `S3NativeObjectOperations` 同步部分 + FileIO 元数据方法（exists/getFileStatus/listStatus/mkdirs/delete）+ marker 语义 | 2d | `S3NativeFileIOBehaviorTest` 中 mkdirs/delete/exists/list 相关用例全绿；单测绿 |
| M2 | rename（含 D7）+ 输入流 + 输出流（D3 并发 multipart + 小文件捷径 + abort） | 2d | 行为套件全绿；输出流/输入流单测绿 |
| M3 | `pread` + default `readVectored` 联调（Parquet 读路径冒烟）；可选：async `readVectored` 重写（基准驱动） | 1.5d | 向量读路径可用；对比记录留档 |
| M4 | shade 打包 + filesystems.md 文档 + 部署/回滚手册 + NOTICE | 1d | fat jar 在干净 Flink/Spark 环境装载成功（手动冒烟记录） |
| M5 | 真实 S3 端点基准 + 验收报告；（可选）`S3EncryptionConfig` 移植（SSE-S3/SSE-KMS） | 1.5d | §8.4 门槛达标，数字回填文档 |

依赖链：M0 → M1 → M2 → M3/M4 并行 → M5。

## 11. 风险与开放问题

| # | 风险 | 缓解 |
| --- | --- | --- |
| R1 | SDK 2.44.4 字节码 > Java 8 | M0-S1 门禁 + 降级路径已定义 |
| R2 | shade 后 SPI/类冲突（jackson、netty） | M0-S3 门禁；F15 重定位；Flink 已验证 awssdk/netty 重定位可行 |
| R3 | 两个 s3 jar 同放 → duplicate scheme 异常 | `FileIO.get` 的 `discoverLoaders()` 对重复 scheme 抛错（Paimon `FileIO.java:587` 附近）；文档显眼警告 |
| R4 | Flink 上游 Experimental、API 漂移 | 一次性借鉴不持续 sync；包名独立（`s3native`） |
| R5 | 静态客户端缓存不回收（classloader 泄漏面） | 与现有 `S3FileIO.CACHE` 同模式同取舍；shutdown hook best-effort |
| R6 | rename 非原子（copy+delete）、目录 rename 中途失败留残迹 | 与 S3A 现状一致（弱原子性是对象存储固有）；Paimon 提交协议不依赖 rename 跨对象原子性（`tryToWriteAtomic` 的对象存储回退路径已存在，`readOverwrittenFileUtf8` 有重试兜底） |
| R7 | MinIO 基准不代表真实 S3 | 验收数字只取自真实 S3 端点 |
| R8 | jar 体积 ~30–40MB（Netty） | 接受并写入文档；去 Netty 的收益存疑（同步并发备份路径在 M0-S2 备选） |
| R9 | marker 与"无 marker"外部写入共存（如 Spark S3A 关闭 marker 写入的目录） | 语义只依赖"prefix 有内容"判定目录存在（§5.1 ③），marker 仅增强空目录可见性；与 S3A `fs.s3a.directory.marker.**` 默认行为一致 |
| O1 | 开放：IAM Role 场景 `requiredOptions` 仍要求静态 key（loader 才不被跳过）——与现 paimon-s3 行为一致 | v2 讨论 |
| O2 | 开放：`s3a://` scheme 注册 | 与现 paimon-s3 一致只注册 `s3`；客户未用 `s3a://` |

## 12. 上游化预留（非本 Spec 工作项）

- 溯源注释规范（§7）与 NOTICE 已按可上游化标准执行。
- 模块对结构遵循 `paimon-filesystems` 既有约定；提 PR 时仅需挪版本属性与文档节。
- marker 语义、目录 rename、批量删除在上游评审时是最可能被挑战的点，偏离清单（§5.5）的证据链（行为套件用例名）即答辩材料。
- 建议等 Flink 侧摘掉 `@Experimental` 后再推上游。

---

**验收清单**（对照已确认的双门槛）：

- [ ] M0 三项 Spike 全过（Java 8 字节码 / async part 全链路 / shade+SPI 冒烟）
- [ ] `S3NativeFileIOBehaviorTest` 全绿（`FileIOBehaviorTestBase` 全部用例，MinIO）
- [ ] §8.2 单测全绿（含从 Flink 移植的用例）
- [ ] 真实 S3 端点基准：写 ≥1.5× / 读 ≥0.9× vs paimon-s3，数字回填文档
- [ ] fat jar 在 Flink lib/ 与 Spark jars/ 各完成一次手动装载冒烟
- [ ] `docs/content/maintenance/filesystems.md` 新节 + 互斥警告 + 回滚步骤
- [ ] 现有 `paimon-s3` / `paimon-s3-impl` 零改动（`git diff` 验证）
- [ ] 全部偏离已在 §5.5 登记且代码内有 `[DEVIATION Dn]` 标注
