import yargs from 'yargs';
import {hideBin} from 'yargs/helpers';
import {Glue} from 'aws-sdk';
import {Connection} from 'mongoose';
import {AsyncFunction, AwsS3StorageAdapter, ConnectionFactory, IBuildIdentifier, IStorage, NullLogger, batchPromise2, IStorageLocation2, S3Location} from "@sealights/sl-cloud-infra2";
import {FootprintsResolver} from "./footprints-resolver";
import {ExecutionsParquetMetadata, FootprintsParquetMetadata, ParquetWriter} from "./parquet-writer";
import {ExecutionResolver} from "./exeutions-resolver/execution-resolver";
import {BuildmapCsvMetadata, BuildmapsInCsvCopier} from "./buildmaps-in-csv-copier";
import {BuildsResolver} from "./builds-resolver/builds-resolver";

export interface ConsoleLogger {
    info: (msg: string) => void;
    error: (msg: string, error?: Error) => void
}

const logger: ConsoleLogger = {
    info: (msg: string) => console.info(`[INFO]: ${msg}`),
    error: (msg: string, err: Error) => console.error(`[ERROR]: ${msg}`, err)
}

const ensureDbExists = async (glue: Glue) => {
    try {
        await glue.createDatabase({
            DatabaseInput: {
                Name: "remove_integration_build"
            }
        }).promise();
        logger.info(`Created database 'remove_integration_build'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Database 'remove_integration_build' already exists`);
            return;
        }
        throw err;
    }
}

const ensureFpParquetTableExists = async (glue: Glue, baseDir: IStorageLocation2) => {
    try {
        await glue.createTable({
            DatabaseName: "remove_integration_build",
            TableInput: {
                Name: "footprints_hits_parquet",
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    },
                    Columns: [
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'hit_start', Type: 'timestamp'},
                        {Name: 'hit_end', Type: 'timestamp'}
                    ],
                    BucketColumns: ['hit_start', 'hit_end'],
                    Location: baseDir.toString()
                },
                PartitionKeys: [
                    {Name: "customer_id", Type: 'string'},
                    {Name: "app_name", Type: 'string'},
                    {Name: "branch_name", Type: 'string'},
                    {Name: "build_name", Type: 'string'},
                    {Name: 'build_session_id', Type: 'string'},
                    {Name: "lab_id", Type: 'string'},
                    {Name: "start", Type: 'bigint'},
                    {Name: "end", Type: 'bigint'}
                ],
                Parameters: {'EXTERNAL': 'TRUE'},
                TableType: 'EXTERNAL_TABLE'
            },
            PartitionIndexes: [{
                IndexName: "time_index",
                Keys: ["customer_id", "app_name", "branch_name", "build_name", "start", "end"]
            }]
        }).promise();
        logger.info(`Created table 'footprints_hits_parquet'`);
    } catch (err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Table 'footprints_hits_parquet' already exists`);
            return;
        }
        throw err;
    }
}

const ensureExecutionsTableExists = async (glue: Glue, baseDir: IStorageLocation2) => {
    try {
        await glue.createTable({
            DatabaseName: "remove_integration_build",
            TableInput: {
                Name: "executions_parquet",
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    },
                    Columns: [
                        {Name: 'execution_id', Type: 'string'},
                        {Name: 'test_stage', Type: 'string'},
                        {Name: 'start_time', Type: 'timestamp'},
                        {Name: 'end_time', Type: 'timestamp'}
                    ],
                    BucketColumns: ['start_time', 'end_time'],
                    Location: baseDir.toString()
                },
                PartitionKeys: [
                    {Name: "customer_id", Type: 'string'},
                    {Name: "app_name", Type: 'string'},
                    {Name: "branch_name", Type: 'string'},
                    {Name: "build_name", Type: 'string'},
                    {Name: 'lab_id', Type: 'string'},
                    {Name: "start", Type: 'bigint'},
                    {Name: "end", Type: 'bigint'}
                ],
                Parameters: {'EXTERNAL': 'TRUE'},
                TableType: 'EXTERNAL_TABLE'
            },
            PartitionIndexes: [{
                IndexName: "time_index",
                Keys: ["customer_id", "app_name", "branch_name", "build_name", "start", "end"]
            },
            {
                IndexName: "lab_index",
                Keys: ["customer_id", "lab_id"]
            }]
        }).promise();
        logger.info(`Created table 'executions_parquet'`);
    } catch (err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Table 'executions_parquet' already exists`);
            return;
        }
        throw err;
    }
}

const ensureBuildmapsTableExists = async (glue: Glue, baseDir: IStorageLocation2) => {
    try {
        await glue.createTable({
            DatabaseName: "remove_integration_build",
            TableInput: {
                Name: "buildmaps",
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'code_element_id', Type: 'string'},
                        {Name: 'global_id', Type: 'string'},
                        {Name: 'fixed_unique_id', Type: 'string'},
                    ],
                    Location: baseDir.toString()
                },
                PartitionKeys: [
                    {Name: 'customer_id', Type: 'string'},
                    {Name: 'app_name', Type: 'string'},
                    {Name: 'branch_name', Type: 'string'},
                    {Name: 'build_name', Type: 'string'},
                    {Name: 'build_session_id', Type: 'string'},
                ],
                Parameters: {'EXTERNAL': 'TRUE', skipHeaderLineCount: "0"},
                TableType: 'EXTERNAL_TABLE'
            },
            PartitionIndexes: [{
                IndexName: "bsid_index",
                Keys: ["customer_id", "build_session_id"]
            }],
        }).promise();
        logger.info(`Created table 'buildmaps'`);
    } catch (err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Table 'buildmaps' already exists`);
            return;
        }
        throw err;
    }
}

const ensureFpPartitionExists = async (glue: Glue, file: FootprintsParquetMetadata) => {
    try {
        const partitionValues = [
            file.customerId,
            file.appName,
            file.branchName,
            file.buildName,
            file.buildSessionId,
            file.labId,
            file.from.valueOf().toString(),
            file.to.valueOf().toString()
        ];
        await glue.createPartition({
            DatabaseName: "remove_integration_build",
            TableName: "footprints_hits_parquet",
            PartitionInput: {
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    },
                    BucketColumns: ['hit_start', 'hit_end'],
                    Columns: [
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'hit_start', Type: 'timestamp'},
                        {Name: 'hit_end', Type: 'timestamp'}
                    ],
                    Location: file.folder.toString()
                },
                Values: partitionValues
            }
        }).promise();
        logger.info(`Created partition '${partitionValues.join('/')}'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            return;
        }
        throw err;
    }
}

const ensureExecutionPartitionExists = async (glue: Glue, file: ExecutionsParquetMetadata) => {
    try {
        const partitionValues = [
            file.customerId,
            file.appName,
            file.branchName,
            file.buildName,
            file.labId,
            file.from.valueOf().toString(),
            file.to.valueOf().toString()
        ];
        await glue.createPartition({
            DatabaseName: "remove_integration_build",
            TableName: "executions_parquet",
            PartitionInput: {
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    },
                    BucketColumns: ['lab_id'],
                    Columns: [
                        {Name: 'execution_id', Type: 'string'},
                        {Name: 'test_stage', Type: 'string'},
                        {Name: 'start_time', Type: 'timestamp'},
                        {Name: 'end_time', Type: 'timestamp'}
                    ],
                    Location: file.folder.toString()
                },
                Values: partitionValues
            }
        }).promise();
        logger.info(`Created partition '${partitionValues.join('/')}'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            return;
        }
        throw err;
    }
}

const ensureBuildmapPartitionExists = async (glue: Glue, file: BuildmapCsvMetadata) => {
    try {
        const partitionValues = [
            file.customerId,
            file.appName,
            file.branchName,
            file.buildName,
            file.bsid
        ];
        await glue.createPartition({
            DatabaseName: "remove_integration_build",
            TableName: "buildmaps",
            PartitionInput: {
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'code_element_id', Type: 'string'},
                        {Name: 'global_id', Type: 'string'},
                        {Name: 'fixed_unique_id', Type: 'string'},
                    ],
                    Location: file.folder.toString()
                },
                Values: partitionValues
            }
        }).promise();
        logger.info(`Created partition '${partitionValues.join('/')}'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            return;
        }
        throw err;
    }
}

const ensureCoreV2BuildmapsTableExists = async (glue: Glue, baseDir: IStorageLocation2) => {
    try {
        await glue.createTable({
            DatabaseName: "remove_integration_build",
            TableInput: {
                Name: "core_build_maps_in_csv",
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'code_element_id', Type: 'string'},
                        {Name: 'global_id', Type: 'string'},
                        {Name: 'fixed_unique_id', Type: 'string'},
                    ],
                    Location: baseDir.toString()
                },
                PartitionKeys: [
                    {Name: 'app_name', Type: 'string'},
                    {Name: 'branch_name', Type: 'string'},
                    {Name: 'build_name', Type: 'string'},
                    {Name: 'build_session_id', Type: 'string'},
                ],
                Parameters: {'EXTERNAL': 'TRUE', skipHeaderLineCount: "0"},
                TableType: 'EXTERNAL_TABLE'
            }
        }).promise();
        logger.info(`Created table 'core_build_maps_in_csv'`);
    } catch (err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Table 'core_build_maps_in_csv' already exists`);
            return;
        }
        throw err;
    }
}

const ensureCoreV2TestStageHitsTableExists = async (glue: Glue, baseDir: IStorageLocation2) => {
    try {
        await glue.createTable({
            DatabaseName: "remove_integration_build",
            TableInput: {
                Name: "partitioned_test_stage_hits_in_csv",
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'test_stage', Type: 'string'},
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'fixed_unique_id', Type: 'string'},
                        {Name: 'line_number', Type: 'int'}
                    ],
                    Location: baseDir.toString()
                },
                PartitionKeys: [
                    {Name: 'app_name', Type: 'string'},
                    {Name: 'branch_name', Type: 'string'},
                    {Name: 'build_name', Type: 'string'},
                    {Name: 'footprints_partition_id', Type: 'string'},
                    {Name: 'build_session_id', Type: 'string'}
                ],
                Parameters: {'EXTERNAL': 'TRUE', skipHeaderLineCount: "0"},
                TableType: 'EXTERNAL_TABLE'
            }
        }).promise();
        logger.info(`Created table 'partitioned_test_stage_hits_in_csv'`);
    } catch (err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Table 'partitioned_test_stage_hits_in_csv' already exists`);
            return;
        }
        throw err;
    }
}

const ensureCoreV2ProcessedBuildCoverageExists = async (glue: Glue, baseDir: IStorageLocation2) => {
    try {
        await glue.createTable({
            DatabaseName: "remove_integration_build",
            TableInput: {
                Name: "processed_build_coverage",
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'app_bsid', Type: 'string'},
                        {Name: 'code_element_id', Type: 'string'},
                        {Name: 'test_stage', Type: 'string'},
                        {Name: 'line_number', Type: 'int'}
                    ],
                    Location: baseDir.toString()
                },
                PartitionKeys: [
                    {Name: 'app_name', Type: 'string'}, //For debugging
                    {Name: 'branch_name', Type: 'string'}, //For debugging
                    {Name: 'build_name', Type: 'string'}, //For debugging
                    {Name: 'build_session_id', Type: 'string'}
                ],
                Parameters: {'EXTERNAL': 'TRUE', skipHeaderLineCount: "1"},
                TableType: 'EXTERNAL_TABLE'
            }
        }).promise();
        logger.info(`Created table 'processed_build_coverage'`);
    } catch (err) {
        if (err.name === 'AlreadyExistsException') {
            logger.info(`Table 'processed_build_coverage' already exists`);
            return;
        }
        throw err;
    }
}

const ensureCoreV2BuildmapsPartitionExists = async (glue: Glue, buildIdentifier: IBuildIdentifier, folder: IStorageLocation2) => {
    try {
        const partitionValues = [
            buildIdentifier.appName,
            buildIdentifier.branchName,
            buildIdentifier.buildName,
            buildIdentifier.bsid
        ];
        await glue.createPartition({
            DatabaseName: "remove_integration_build",
            TableName: "core_build_maps_in_csv",
            PartitionInput: {
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'code_element_id', Type: 'string'},
                        {Name: 'global_id', Type: 'string'},
                        {Name: 'fixed_unique_id', Type: 'string'},
                    ],
                    Location: folder.toString()
                },
                Values: partitionValues
            }
        }).promise();
        logger.info(`Created partition '${partitionValues.join('/')}'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            return;
        }
        throw err;
    }
}

const ensureCoreV2TestStageHitsPartitionExists = async (glue: Glue, buildIdentifier: IBuildIdentifier, fpPartitionId: string, folder: IStorageLocation2) => {
    try {
        const partitionValues = [
            buildIdentifier.appName,
            buildIdentifier.branchName,
            buildIdentifier.buildName,
            fpPartitionId,
            buildIdentifier.bsid
        ];
        await glue.createPartition({
            DatabaseName: "remove_integration_build",
            TableName: "partitioned_test_stage_hits_in_csv",
            PartitionInput: {
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'test_stage', Type: 'string'},
                        {Name: 'unique_id', Type: 'string'},
                        {Name: 'fixed_unique_id', Type: 'string'},
                        {Name: 'line_number', Type: 'int'}
                    ],
                    Location: folder.toString()
                },
                Values: partitionValues
            }
        }).promise();
        logger.info(`Created partition '${partitionValues.join('/')}'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            return;
        }
        throw err;
    }
}

const ensureCoreV2ProcessedBuildCoveragePartitionExists = async (glue: Glue, buildIdentifier: IBuildIdentifier, folder: IStorageLocation2) => {
    try {
        const partitionValues = [
            buildIdentifier.appName,
            buildIdentifier.branchName,
            buildIdentifier.buildName,
            buildIdentifier.bsid
        ];
        await glue.createPartition({
            DatabaseName: "remove_integration_build",
            TableName: "processed_build_coverage",
            PartitionInput: {
                StorageDescriptor: {
                    InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    SerdeInfo: {
                        SerializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        Parameters: {escapeChar: '\\', quoteChar: '"', separatorChar: ','}
                    },
                    Columns: [
                        {Name: 'app_bsid', Type: 'string'},
                        {Name: 'code_element_id', Type: 'string'},
                        {Name: 'test_stage', Type: 'string'},
                        {Name: 'line_number', Type: 'int'}
                    ],
                    Location: folder.toString()
                },
                Values: partitionValues
            }
        }).promise();
        logger.info(`Created partition '${partitionValues.join('/')}'`);
    } catch(err) {
        if (err.name === 'AlreadyExistsException') {
            return;
        }
        throw err;
    }
}

yargs(hideBin(process.argv))
    .command(
        'copy-core-v2-as-parquet',
        'create parqueted core-v2 data',
        (yargs) =>
            yargs
                .option('customer-id', {
                    alias: 'c',
                    describe: 'customer id',
                    demandOption: true,
                    type: "string"
                })
                .option('app-name', {
                    alias: 'a',
                    describe: 'app to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('branch-name', {
                    alias: 'b',
                    describe: 'branch to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-name', {
                    alias: 'b',
                    describe: 'first build to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-session-id', {
                    alias: 'bsid',
                    describe: 'bsid for first build to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-history-limit', {
                    alias: 'bhl',
                    describe: 'number of builds to copy',
                    default: 50,
                    number: true
                })
                .option('source-region', {
                    alias: 'sr',
                    describe: 'aws region for source bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('source-access-key', {
                    alias: 'sak',
                    describe: 'aws access key for source bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('source-secret-access-key', {
                    alias: 'ssak',
                    describe: 'aws secret access key for source bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('source-bucket', {
                    alias: 'sb',
                    describe: 'aws bucket to copy from',
                    demandOption: true,
                    type: "string"
                })
                .option('source-env', {
                    alias: 'se',
                    describe: 'envrionment to copy from (ex: DEV-staging)',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-host', {
                    alias: 'dbh',
                    describe: 'hostname for the mongo with the requested build entries',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-name', {
                    alias: 'dbn',
                    describe: 'db name for the mongo with the requested build entries',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-port', {
                    alias: 'sdp',
                    describe: 'db port for the mongo with the requested build entries',
                    number: true,
                    default: 27017
                })
                .option('destination-region', {
                    alias: 'dr',
                    describe: 'aws region for destination bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-access-key', {
                    alias: 'dak',
                    describe: 'aws access key for destination bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-secret-access-key', {
                    alias: 'dsak',
                    describe: 'aws secret access key for destination bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-bucket', {
                    alias: 'db',
                    describe: 'aws bucket to copy to',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-env', {
                    alias: 'de',
                    describe: 'envrionment to copy to (ex: DEV-staging)',
                    demandOption: true,
                    type: "string"
                })
                .option('footprints-files-per-parquet', {
                    alias: 'fpf',
                    describe: 'number of raw footprints files to put in a single parquet file',
                    default: 100,
                    number: true
                })
                .option('max-parallel-processing', {
                    alias: 'mpp',
                    describe: 'number of builds to process in parallel',
                    default: 10,
                    number: true
                }),
        async (argv) => {
            let connection: Connection;
            try {
                const glue = new Glue({
                    region: argv.destinationRegion,
                    accessKeyId: argv.destinationAccessKey,
                    secretAccessKey: argv.destinationSecretAccessKey
                });
                
                const sourceStorage: IStorage = new AwsS3StorageAdapter({
                    accessKeyId: argv.sourceAccessKey,
                    secretAccessKey: argv.sourceSecretAccessKey,
                    region: argv.sourceRegion
                }, 3, new NullLogger());
                
                const destinationStorage: IStorage = new AwsS3StorageAdapter({
                    accessKeyId: argv.destinationAccessKey,
                    secretAccessKey: argv.destinationSecretAccessKey,
                    region: argv.destinationRegion
                }, 3, new NullLogger());
                
                const buildmapsCopier = new BuildmapsInCsvCopier({
                    sourceBucket: argv.sourceBucket,
                    sourceEnv: argv.sourceEnv,
                    destinationBucket: argv.destinationBucket,
                    destinationEnv: argv.destinationEnv,
                    sourceStorage, destinationStorage,
                    logger
                });
                
                const footprintsResolver = new FootprintsResolver({
                    storage: sourceStorage,
                    envName: argv.sourceEnv,
                    bucket: argv.sourceBucket,
                    logger
                });
                
                const connectionFactory = new ConnectionFactory({
                    host: argv.sourceDbHost,
                    port: argv.sourceDbPort,
                    dbName: argv.sourceDbName
                }, "footprints-parquet-poc", undefined, new NullLogger());
                connection = connectionFactory.createConnection();
                const executionResolver = new ExecutionResolver(connection);
                const buildsResolver = new BuildsResolver(connection);
                
                const buildIdentifier: IBuildIdentifier = {
                    customerId: argv.customerId,
                    appName: argv.appName,
                    branchName: argv.branchName,
                    buildName: argv.buildName,
                    bsid: argv.buildSessionId
                }
                
                const parquetWriter = new ParquetWriter({
                    storage: destinationStorage,
                    envName: argv.destinationEnv,
                    executionResolver,
                    buildIdentifier,
                    bucket: argv.destinationBucket,
                    logger
                });
    
                const processed = new Set<string>([
                    '9b94bf40-97fb-4015-bde5-71034c8e27cb',
                    '4ced649a-7797-41cc-9bcf-94585f47aea0',
                    '42d15e2c-26fa-48cd-a766-37873a480e25',
                    '763a5184-9532-469b-a165-d67f19d9bfb2',
                    'f521dcb4-f092-4c75-a00f-dd236c6170bd',
                    'e39a638b-1689-4de1-ac77-f58484df374c',
                    '817e248f-151e-4a5d-aeae-1bdb3a36b552',
                    'fe6657c9-9115-45da-bc62-220d5d4c8c00',
                    'aa67a554-dba9-4a1e-a0f4-b39c6672f688',
                    'ab1ab2ba-2475-4039-893c-6fd74935d780',
                    '41bc849e-e6c3-4675-b365-68d9c9cb7462',
                    '1163a669-638c-47e1-9287-b826d3026685',
                    'e99d71fd-94a0-4286-bad5-ca7912075272',
                    '361ef7c2-58b0-4768-b5f3-55e30bd82fce',
                    '9ac7f1c0-4760-40fc-b0a9-88b20f70b88c',
                    'cb81af2a-fd48-4197-a457-14374a1b7810',
                    '1c08e9c7-f3c2-4162-a763-de3dbb105c30'
                ]);
                const processBuild = async (buildIdentifier: IBuildIdentifier, buildmapsCopier: BuildmapsInCsvCopier, footprintsResolver: FootprintsResolver, parquetWriter: ParquetWriter, glue: Glue) => {
                    if (processed.has(buildIdentifier.bsid)) {
                        logger.info(`Skipping build: ${JSON.stringify(buildIdentifier, null, 2)}`);
                        return;
                    }
                
                    logger.info(`Writing data for build: ${JSON.stringify(buildIdentifier, null, 2)}`);
                    for await (let file of buildmapsCopier.copy(buildIdentifier)) {
                        await ensureBuildmapPartitionExists(glue, file);
                    }
                
                    for await (let footprintsFiles of footprintsResolver.resolve(buildIdentifier, argv.footprintsFilesPerParquet)) {
                        const files = await parquetWriter.writeFp(footprintsFiles);
                        for (let file of files) {
                            logger.info(`Created parquet file '${file.folder.storageKey}/${file.fileName}'`);
                            await ensureFpPartitionExists(glue, file);
                        }
                    }
                    processed.add(buildIdentifier.bsid);
                }
                
                await ensureDbExists(glue);
                await ensureFpParquetTableExists(glue, parquetWriter.fpBaseDir);
                await ensureExecutionsTableExists(glue, parquetWriter.executionsBaseDir);
                await ensureBuildmapsTableExists(glue, buildmapsCopier.destinationBaseDir);
                for await (let buildInHistory of buildsResolver.iterateHistory(buildIdentifier, argv.buildHistoryLimit)) {
                    const p: AsyncFunction<void>[] = [];
                    p.push(
                        () => processBuild(buildInHistory, buildmapsCopier, footprintsResolver, parquetWriter, glue),
                        ...(await buildsResolver.resolveDependencies(buildInHistory)).map(dep => {
                            return () => processBuild(dep, buildmapsCopier, footprintsResolver, parquetWriter, glue)
                        })
                    );
                    await batchPromise2(p, argv.maxParallelProcessing);
                    const executionsFiles = await parquetWriter.writeExecutions();
                    for (let executionsFile of executionsFiles) {
                        await ensureExecutionPartitionExists(glue, executionsFile);
                    }
                }
            
    
                logger.info("Finished converting footprints to parquet")
            } catch (err) {
                logger.error("Footprints conversion to parquet failed", err)
            } finally {
                if (connection) {
                    await connection.close();
                }
            }
        }
    )
    .command(
        'copy-core-v2',
        'copy core-v2 data as is',
        (yargs) => 
            yargs
                .option('customer-id', {
                    alias: 'c',
                    describe: 'customer id',
                    demandOption: true,
                    type: "string"
                })
                .option('app-name', {
                    alias: 'a',
                    describe: 'app to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('branch-name', {
                    alias: 'b',
                    describe: 'branch to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-name', {
                    alias: 'b',
                    describe: 'first build to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-session-id', {
                    alias: 'bsid',
                    describe: 'bsid for first build to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-history-limit', {
                    alias: 'bhl',
                    describe: 'number of builds to copy',
                    default: 50,
                    number: true
                })
                .option('source-region', {
                    alias: 'sr',
                    describe: 'aws region for source bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('source-access-key', {
                    alias: 'sak',
                    describe: 'aws access key for source bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('source-secret-access-key', {
                    alias: 'ssak',
                    describe: 'aws secret access key for source bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('source-bucket', {
                    alias: 'sb',
                    describe: 'aws bucket to copy from',
                    demandOption: true,
                    type: "string"
                })
                .option('source-env', {
                    alias: 'se',
                    describe: 'envrionment to copy from (ex: DEV-staging)',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-host', {
                    alias: 'dbh',
                    describe: 'hostname for the mongo with the requested build entries',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-name', {
                    alias: 'dbn',
                    describe: 'db name for the mongo with the requested build entries',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-port', {
                    alias: 'sdp',
                    describe: 'db port for the mongo with the requested build entries',
                    number: true,
                    default: 27017
                })
                .option('destination-region', {
                    alias: 'dr',
                    describe: 'aws region for destination bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-access-key', {
                    alias: 'dak',
                    describe: 'aws access key for destination bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-secret-access-key', {
                    alias: 'dsak',
                    describe: 'aws secret access key for destination bucket',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-bucket', {
                    alias: 'db',
                    describe: 'aws bucket to copy to',
                    demandOption: true,
                    type: "string"
                })
                .option('destination-env', {
                    alias: 'de',
                    describe: 'envrionment to copy to (ex: DEV-staging)',
                    demandOption: true,
                    type: "string"
                })
                .option('max-parallel-processing', {
                    alias: 'mpp',
                    describe: 'number of builds to process in parallel',
                    default: 10,
                    number: true
                }),
        async (argv) => {
            let connection: Connection;
            try {
                const glue = new Glue({
                    region: argv.destinationRegion,
                    accessKeyId: argv.destinationAccessKey,
                    secretAccessKey: argv.destinationSecretAccessKey
                });
                
                const sourceStorage: IStorage = new AwsS3StorageAdapter({
                    accessKeyId: argv.sourceAccessKey,
                    secretAccessKey: argv.sourceSecretAccessKey,
                    region: argv.sourceRegion
                }, 3, new NullLogger());
                
                const destinationStorage: IStorage = new AwsS3StorageAdapter({
                    accessKeyId: argv.destinationAccessKey,
                    secretAccessKey: argv.destinationSecretAccessKey,
                    region: argv.destinationRegion
                }, 3, new NullLogger());
    
                const connectionFactory = new ConnectionFactory({
                    host: argv.sourceDbHost,
                    port: argv.sourceDbPort,
                    dbName: argv.sourceDbName
                }, "footprints-parquet-poc", undefined, new NullLogger());
                connection = connectionFactory.createConnection();

                const buildsResolver = new BuildsResolver(connection);

                const copyFile = async (fileKey: string) => {
                    const sourceLocation = new S3Location(argv.sourceBucket, fileKey);
                    const destinationLocation = new S3Location(argv.destinationBucket, fileKey.replace(argv.sourceEnv, argv.destinationEnv));
                    logger.info(`Copying file: '${fileKey}' to '${destinationLocation.storageKey}'`);
                    await destinationStorage.uploadFileAsStream(
                        destinationLocation,
                        sourceStorage.downloadFileAsStream(sourceLocation)
                    );
                }

                const copyPath = async (sourceFolders: string[], fileCb?: (storageKey: string) => Promise<void>) => {
                    const sourceFolder = new S3Location(argv.sourceBucket, argv.sourceEnv).add(...sourceFolders);
                    const sourceFiles = await sourceStorage.listFiles(sourceFolder.bucket, {
                        prefix: `${sourceFolder.storageKey}`,
                        delimiter: ''
                    });
                    await batchPromise2(
                        sourceFiles.map(file => async () => {
                            await copyFile(file.key);
                            if (fileCb) {
                                await fileCb(file.key);
                            }
                        }),
                        10
                    );
                }

                const processed = new Set<string>();
                const processBuild = async (buildIdentifier: IBuildIdentifier) => {
                    if (processed.has(buildIdentifier.bsid)) {
                        return;
                    }
                
                    logger.info(`Writing data for build: ${JSON.stringify(buildIdentifier, null, 2)}`);
                    await copyPath([
                        'core-v2',
                        'partitioned-test-stages-hits-in-csv',
                        buildIdentifier.customerId,
                        buildIdentifier.appName,
                        buildIdentifier.branchName,
                        buildIdentifier.buildName
                    ], async (storageKey: string) => {
                        const partitionId = storageKey.split('/')[7];
                        const folderParts = storageKey.split('/');
                        folderParts.pop();
                        const folder = new S3Location(argv.destinationBucket, folderParts);
                        await ensureCoreV2TestStageHitsPartitionExists(glue, buildIdentifier, partitionId, folder);
                    });

                    await copyPath([
                        'core-v2',
                        'build-maps-in-csv',
                        buildIdentifier.customerId,
                        buildIdentifier.appName,
                        buildIdentifier.branchName,
                        buildIdentifier.buildName
                    ]);
                    await ensureCoreV2BuildmapsPartitionExists(glue, buildIdentifier, new S3Location(argv.destinationBucket, argv.destinationEnv).add(...[
                        'core-v2',
                        'build-maps-in-csv',
                        buildIdentifier.customerId,
                        buildIdentifier.appName,
                        buildIdentifier.branchName,
                        buildIdentifier.buildName
                    ]));

                    await copyPath([
                        'core-v2',
                        'processed-build-coverage-csv',
                        buildIdentifier.customerId,
                        buildIdentifier.appName,
                        buildIdentifier.branchName,
                        buildIdentifier.buildName
                    ]);
                    await ensureCoreV2ProcessedBuildCoveragePartitionExists(glue, buildIdentifier, new S3Location(argv.destinationBucket, argv.destinationEnv).add(...[
                        'core-v2',
                        'processed-build-coverage-csv',
                        buildIdentifier.customerId,
                        buildIdentifier.appName,
                        buildIdentifier.branchName,
                        buildIdentifier.buildName
                    ]));

                    processed.add(buildIdentifier.bsid);
                }

                const buildIdentifier: IBuildIdentifier = {
                    customerId: argv.customerId,
                    appName: argv.appName,
                    branchName: argv.branchName,
                    buildName: argv.buildName,
                    bsid: argv.buildSessionId
                }

                await ensureDbExists(glue);
                await ensureCoreV2BuildmapsTableExists(glue, new S3Location(argv.destinationBucket, argv.destinationEnv).add('core-v2', 'build-maps-in-csv'));
                await ensureCoreV2TestStageHitsTableExists(glue, new S3Location(argv.destinationBucket, argv.destinationEnv).add('core-v2', 'partitioned-test-stages-hits-in-csv'));
                await ensureCoreV2ProcessedBuildCoverageExists(glue, new S3Location(argv.destinationBucket, argv.destinationEnv).add('core-v2', 'processed-build-coverage-csv'));

                for await (let buildInHistory of buildsResolver.iterateHistory(buildIdentifier, argv.buildHistoryLimit)) {
                    const p: AsyncFunction<void>[] = [];
                    p.push(
                        () => processBuild(buildInHistory),
                        ...(await buildsResolver.resolveDependencies(buildIdentifier)).map(dep => {
                            return () => processBuild(dep)
                        })
                    );
                    await batchPromise2(p, argv.maxParallelProcessing);
                }
            } catch (err) {
                logger.error("Footprints copy failed", err)
            } finally {
                if (connection) {
                    await connection.close();
                }
            }
        }
    )
    .command(
        'generate-query',
        'generate coverage query',
        (yargs) =>
            yargs
                .option('customer-id', {
                    alias: 'c',
                    describe: 'customer id',
                    demandOption: true,
                    type: "string"
                })
                .option('app-name', {
                    alias: 'a',
                    describe: 'app to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('branch-name', {
                    alias: 'b',
                    describe: 'branch to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-name', {
                    alias: 'b',
                    describe: 'first build to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('build-session-id', {
                    alias: 'bsid',
                    describe: 'bsid for first build to copy',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-host', {
                    alias: 'dbh',
                    describe: 'hostname for the mongo with the requested build entries',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-name', {
                    alias: 'dbn',
                    describe: 'db name for the mongo with the requested build entries',
                    demandOption: true,
                    type: "string"
                })
                .option('source-db-port', {
                    alias: 'sdp',
                    describe: 'db port for the mongo with the requested build entries',
                    number: true,
                    default: 27017
                }),
        async (argv) => {
            let connection: Connection;
            try {
                const connectionFactory = new ConnectionFactory({
                    host: argv.sourceDbHost,
                    port: argv.sourceDbPort,
                    dbName: argv.sourceDbName
                }, "footprints-parquet-poc", undefined, new NullLogger());
                connection = connectionFactory.createConnection();
                const buildsResolver = new BuildsResolver(connection);

                const buildIdentifier: IBuildIdentifier = {
                    customerId: argv.customerId,
                    appName: argv.appName,
                    branchName: argv.branchName,
                    buildName: argv.buildName,
                    bsid: argv.buildSessionId
                }
                const dependencies = await buildsResolver.resolveDependencies(buildIdentifier);

                logger.info(`GENERATED QUERY:
SELECT '"' || code_element_id || '"' as code_element_id,
       '"' || CAST('${buildIdentifier.customerId}_${buildIdentifier.bsid}' AS VARCHAR) || '"' AS customer_bsid,
    build_session_id as app_bsid,
    '"' || CASE hit_test_stages
            WHEN '' THEN '{"""isCovered""":false,"""testStages""":[]}'
            ELSE concat('{"""isCovered""":true,"""testStages""":[', hit_test_stages, ']}')
    END || '"' AS coverage
FROM (
    SELECT bm.code_element_id, fp.build_session_id, array_join(array_agg(distinct('"""' || ex.test_stage || '"""')),',') AS hit_test_stages
    FROM footprints_hits_parquet fp
    INNER JOIN executions_parquet ex ON (
        ex.customer_id = fp.customer_id AND
        ex.lab_id = fp.lab_id AND
        fp.hit_start >= ex.start_time AND
        fp.hit_end <= ex.end_time
    )
    INNER JOIN buildmaps bm ON (
        bm.customer_id = fp.customer_id AND
        bm.build_session_id = fp.build_session_id AND
        bm.unique_id = fp.unique_id
    )
    WHERE (fp.app_name = '${buildIdentifier.appName}' and fp.branch_name = '${buildIdentifier.branchName}' and fp.build_name = '${buildIdentifier}')
    ${dependencies.map(dep => `       OR (fp.app_name = '${dep.appName}' and fp.branch_name = '${dep.branchName}' and fp.build_name = '${dep.buildName}')`).join('\n')}    
    GROUP BY bm.code_element_id, fp.build_session_id
) hits
`)
            } catch (err) {
                logger.error("Failed to create query", err);
            } finally {
                if (connection) {
                    await connection.close();
                }
            }
        }
    )
    .help()
    .parse();
