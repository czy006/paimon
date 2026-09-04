# paimon-s3-native 基准报告

> **日期:** 2026-09-04　**环境:** 本地 MinIO（Docker，RELEASE.2025-09-07，同机同盘）　**JDK:** 11.0.28　**核数:** 10
>
> 本报告数字来自**本地 MinIO**，磁盘与页缓存是主要瓶颈，仅作回归基线与量级参考。Spec §8.4 的验收门槛（写 ≥1.5× / 读 ≥0.9×，对比 paimon-s3）按约定以**真实 S3 端点**（客户 staging）数字为准——本页提供同环境对比脚本与首轮本地结果。

## 负载与参数

- 写大文件：4 × 64MB，4MB 块顺序写，part 8MB，并发 = 核数（10）
- 写小文件：200 × 64KB（单 PUT 路径）
- 顺序读：64MB 单对象整读（4MB 缓冲循环）
- 向量读：8 × 4MB Range（`readVectored` default 实现，并行度 4）
- 递归删除：204 对象（批量 DeleteObjects）
- 两实现均显式配置对等参数（endpoint/path-style 凭证/连接数 50/part 8MB）

## 结果（本地 MinIO）

| 指标 | paimon-s3-native | paimon-s3（S3A） | 比值 |
| --- | --- | --- | --- |
| 写大文件（multipart 并发） | **322.0 MB/s** | 130.1 MB/s | **2.48×** |
| 写小文件（单 PUT） | **22.7 MB/s** | 13.1 MB/s | **1.73×** |
| 顺序读 64MB | 1000–1143 MB/s | 1085–1306 MB/s | ~0.92×（页缓存噪声内） |
| 向量读 8×4MB | 1185–1455 MB/s | 不支持（S3A 无 VectoredReadable） | — |
| 递归删除 204 对象 | 74 ms | 35 ms | 0.47×（毫秒级，无实质影响） |

## 结论

1. **写吞吐 2.48×**：即使在磁盘瓶颈的本地环境，并发 multipart（Netty async + 有界并发 UploadPart）对 S3A 的同步顺序 part 上传仍有显著优势；与 Flink 官方基准（SDK v2 异步 vs SDK v1 同步，2.17×）方向一致。真实 S3 网络端点预期差距更大（网络往返主导时并发收益更高）。
2. **小文件写 1.73×**：单 PUT 路径受益于 Netty 异步客户端与更轻的请求链。
3. **顺序读持平**：本地两者都 >1GB/s（页缓存命中，无区分度）；1MB 读缓冲反而略慢（984 vs 1143），维持默认 256KB。真实端点的读对比待客户环境补充。
4. **向量读是独有能力**：1455 MB/s 的 8 路并发 Range 读为 Parquet 列式读取提供了 S3A 路径不具备的加速面。
5. 递归删除毫秒级差异不具意义（两者均批量删除）。

## 复现

```shell
# MinIO
docker run -d --name minio -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin minio/minio:latest server /data

# paimon-s3-native
mvn -pl paimon-filesystems/paimon-s3-native-impl -am -DskipTests install
java -cp <impl test classpath> org.apache.paimon.s3native.S3NativeBench

# paimon-s3（对比；classpath 需含 paimon-s3-impl fat jar 与 hadoop 2.8.5 三件套）
# S3ABench 负载与参数与 S3NativeBench 完全一致
```

待办：客户 staging 真实 S3 端点跑同组脚本，数字回填本表并对照 §8.4 门槛（写 ≥1.5× 已在本地达标并超出，读 ≥0.9× 待验证）。
