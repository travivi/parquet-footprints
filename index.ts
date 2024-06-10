import {AwsS3StorageAdapter, ConnectionFactory, IBuildIdentifier, IStorage, NullLogger} from "@sealights/sl-cloud-infra2";
import {FootprintsResolver} from "./footprints-resolver";
import {ExecutionsParquetMetadata, FootprintsParquetMetadata, ParquetWriter} from "./parquet-writer";
import {ExecutionResolver} from "./exeutions-resolver/execution-resolver";
import {Glue} from 'aws-sdk';
import { IStorageLocation2 } from "@sealights/sl-cloud-infra2";
import { BuildmapCsvMetadata, BuildmapsInCsvCopier } from "./buildmaps-in-csv-copier";
import { DependenciesResolver } from "./dependencies-resolver/dependencies-resolver";

export interface ConsoleLogger {
    info: (msg: string) => void;
    error: (msg: string, error?: Error) => void
}

const CUSTOMER_ID = process.env.CUSTOMER_ID;
const APP_NAME = process.env.APP_NAME;
const BRANCH_NAME = process.env.BRANCH_NAME;
const BUILD_NAME = process.env.BUILD_NAME;
const BUILD_SESSION_ID = process.env.BUILD_SESSION_ID;

const SOURCE_REGION = process.env.SOURCE_REGION;
const SOURCE_ACCESS_KEY = process.env.SOURCE_ACCESS_KEY;
const SOURCE_SECRET_ACCESS_KEY = process.env.SOURCE_SECRET_ACCESS_KEY;
const SOURCE_BUCKET = process.env.SOURCE_BUCKET;
const SOURCE_ENV = process.env.SOURCE_ENV_NAME;
const SOURCE_DB_HOST = process.env.SOURCE_DB_HOST;
const SOURCE_DB_PORT = process.env.SOURCE_DB_PORT ? Number.parseInt(process.env.SOURCE_DB_PORT) : undefined;
const SOURCE_DB_NAME = process.env.SOURCE_DB_NAME;

const DESTINATION_REGION = process.env.DESTINATION_REGION;
const DESTINATION_ACCESS_KEY = process.env.DESTINATION_ACCESS_KEY;
const DESTINATION_SECRET_ACCESS_KEY = process.env.DESTINATION_SECRET_ACCESS_KEY;
const DESTINATION_BUCKET = process.env.DESTINATION_BUCKET;
const DESTINATION_ENV = process.env.DESTINATION_ENV_NAME;

const FOOTPRINT_FILES_PER_PARQUET = Number.parseInt(process.env.FOOTPRINT_FILES_PER_PARQUET);

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

const ensureFpTableExists = async (glue: Glue, baseDir: IStorageLocation2) => {
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
            }
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

const run = async (buildIdentifier: IBuildIdentifier, buildmapsCopier: BuildmapsInCsvCopier, footprintsResolver: FootprintsResolver, dependenciesResolver: DependenciesResolver, parquetWriter: ParquetWriter, glue: Glue) => {
    await ensureDbExists(glue);
    await ensureFpTableExists(glue, parquetWriter.fpBaseDir);
    await ensureExecutionsTableExists(glue, parquetWriter.executionsBaseDir);
    await ensureBuildmapsTableExists(glue, buildmapsCopier.destinationBaseDir);
    for await (let file of buildmapsCopier.copy(buildIdentifier)) {
        await ensureBuildmapPartitionExists(glue, file);
    }
    for await (let dependency of dependenciesResolver.resolve(buildIdentifier)) {
        for await (let file of buildmapsCopier.copy(dependency)) {
            await ensureBuildmapPartitionExists(glue, file);
        }
    }

    for await (let footprintsFiles of footprintsResolver.resolve(buildIdentifier, FOOTPRINT_FILES_PER_PARQUET)) {
        const files = await parquetWriter.writeFp(footprintsFiles);
        for (let file of files) {
            logger.info(`Created parquet file '${file.folder.storageKey}/${file.fileName}'`);
            await ensureFpPartitionExists(glue, file);
        }
    }
    const executionsFiles = await parquetWriter.writeExecutions();
    for (let executionsFile of executionsFiles) {
        await ensureExecutionPartitionExists(glue, executionsFile);
    }
}

const glue = new Glue({
    region: DESTINATION_REGION,
    accessKeyId: DESTINATION_ACCESS_KEY,
    secretAccessKey: DESTINATION_SECRET_ACCESS_KEY
})

const sourceStorage: IStorage = new AwsS3StorageAdapter({
    accessKeyId: SOURCE_ACCESS_KEY,
    secretAccessKey: SOURCE_SECRET_ACCESS_KEY,
    region: SOURCE_REGION
}, 3, new NullLogger());

const destinationStorage: IStorage = new AwsS3StorageAdapter({
    accessKeyId: DESTINATION_ACCESS_KEY,
    secretAccessKey: DESTINATION_SECRET_ACCESS_KEY,
    region: DESTINATION_REGION
}, 3, new NullLogger());

const buildMapsCopier = new BuildmapsInCsvCopier({
    sourceBucket: SOURCE_BUCKET,
    sourceEnv: SOURCE_ENV,
    destinationBucket: DESTINATION_BUCKET,
    destinationEnv: DESTINATION_ENV,
    sourceStorage, destinationStorage,
    logger
});

const footprintsResolver = new FootprintsResolver({
    storage: sourceStorage,
    envName: SOURCE_ENV,
    bucket: SOURCE_BUCKET,
    logger
});

const connectionFactory = new ConnectionFactory({
    host: SOURCE_DB_HOST,
    port: SOURCE_DB_PORT || 27017,
    dbName: SOURCE_DB_NAME
}, "footprints-parquet-poc", undefined, new NullLogger());
const connection = connectionFactory.createConnection();
const executionResolver = new ExecutionResolver(connection);
const dependenciesResolver = new DependenciesResolver(connection);

const buildIdentifier: IBuildIdentifier = {
    customerId: CUSTOMER_ID,
    appName: APP_NAME,
    branchName: BRANCH_NAME,
    buildName: BUILD_NAME,
    bsid: BUILD_SESSION_ID
}

const parquetWriter = new ParquetWriter({
    storage: destinationStorage,
    envName: DESTINATION_ENV,
    executionResolver,
    buildIdentifier,
    bucket: DESTINATION_BUCKET,
    logger
});

run(buildIdentifier, buildMapsCopier, footprintsResolver, dependenciesResolver, parquetWriter, glue)
    .then(() => logger.info("Finished converting footprints to parquet"))
    .catch((err) => logger.error("Footprints conversion to parquet failed", err))
    .finally(() => connection.close());

