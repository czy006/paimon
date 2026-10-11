/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.paimon.operation.PartitionArchiver;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Manual full-partition archive under a caller-coordinated maintenance window.
 *
 * <p>Parameter/partition conventions follow https://paimon.apache.org/docs/1.3/spark/procedures/.
 */
public class ArchivePartitionsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("partitions", StringType),
                ProcedureParameter.required("target_path", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, false, Metadata.empty())
                    });

    protected ArchivePartitionsProcedure(TableCatalog catalog) {
        super(catalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        for (int i = 0; i < PARAMETERS.length; i++) {
            Preconditions.checkArgument(
                    !args.isNullAt(i), "archive_partitions arguments must not be null");
        }
        Identifier identifier = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String partitions = args.getString(1);
        String target = args.getString(2);
        Preconditions.checkArgument(
                tableCatalog() instanceof SparkCatalog,
                "archive_partitions requires a Paimon SparkCatalog");
        return modifyPaimonTable(
                identifier,
                table -> {
                    Preconditions.checkArgument(
                            table instanceof FileStoreTable,
                            "archive_partitions requires a FileStoreTable");
                    return new PartitionArchiver(
                                    (FileStoreTable) table,
                                    target,
                                    ((SparkCatalog) tableCatalog()).paimonCatalog())
                            .archive(partitions).stream()
                                    .map(result -> newInternalRow(UTF8String.fromString(result)))
                                    .toArray(InternalRow[]::new);
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ArchivePartitionsProcedure>() {
            @Override
            public ArchivePartitionsProcedure doBuild() {
                return new ArchivePartitionsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ArchivePartitionsProcedure";
    }
}
