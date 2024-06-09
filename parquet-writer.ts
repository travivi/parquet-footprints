import {IFootprintsV6File} from "../footprints-sender/worker";
import {ParquetSchema, ParquetTransformer} from 'parquetjs';
import {IBuildIdentifier, IStorage, IStorageLocation2, S3Location} from "@sealights/sl-cloud-infra2";
import {ExecutionResolver} from "./exeutions-resolver/execution-resolver";
import {fromArray} from 'object-stream';

import * as uuid from 'uuid';

export type FootprintsParquetWriterArgs = {
    storage: IStorage,
    executionResolver: ExecutionResolver;
    bucket: string,
    envName: string,
    buildIdentifier: IBuildIdentifier
}

export interface FootprintsParquetMetadata {
    customerId: string;
    appName: string;
    branchName: string;
    buildName: string;
    from: Date;
    to: Date;

    folder: IStorageLocation2;
    fileName: string;
}

export interface ExecutionsParquetMetadata {
    customerId: string;
    appName: string;
    branchName: string;
    buildName: string;
    folder: IStorageLocation2;
    fileName: string;
}

export interface ParquetFootprintsRow {
    unique_id: string,
    lab_id: string,
    hit_start: Date,
    hit_end: Date
}

export interface ParquetExectutionRow {
    execution_id: string;
    lab_id: string;
    test_stage: string;
    start_time: Date;
    end_time: Date;
}

export class ParquetWriter {
    private static readonly fpSchema: ParquetSchema = new ParquetSchema({
        unique_id: { type: "UTF8" },
        lab_id: { type: "UTF8" },
        hit_start: { type: 'TIMESTAMP_MILLIS' },
        hit_end: { type: 'TIMESTAMP_MILLIS' },
    });
    private static readonly executionSchema: ParquetSchema = new ParquetSchema({
        execution_id: { type: "UTF8" },
        lab_id: { type: "UTF8" },
        test_stage: { type: "UTF8" },
        start_time: { type: 'TIMESTAMP_MILLIS' },
        end_time: { type: 'TIMESTAMP_MILLIS' },
    });
    private storage: IStorage;
    private executionResolver: ExecutionResolver;
    private bucket: string;
    private envName: string;
    private buildIdentifier: IBuildIdentifier;
    constructor(args: FootprintsParquetWriterArgs) {
        this.storage = args.storage;
        this.executionResolver = args.executionResolver;
        this.bucket = args.bucket;
        this.envName = args.envName;
        this.buildIdentifier = args.buildIdentifier;
    }

    public async writeFp(footprintsFiles: IFootprintsV6File[]): Promise<FootprintsParquetMetadata> {
        const from = new Date(
            Math.min(...footprintsFiles.map(file => file.executions.map(execution => execution.hits.map(hit => hit.start))).flat().flat()) * 1000
        );
        const to = new Date(
            Math.min(...footprintsFiles.map(file => file.executions.map(execution => execution.hits.map(hit => hit.end))).flat().flat()) * 1000
        );
        
        const rows: ParquetFootprintsRow[] = [];
        for (let footprintsFile of footprintsFiles) {
            rows.push(
                ...(await this.getRowsFromFpFile(footprintsFile))
            );
        }
        const {fileName, folder} = await this.uploadFpFile(rows, from, to);

        return {
            customerId: this.buildIdentifier.customerId,
            appName: this.buildIdentifier.appName,
            branchName: this.buildIdentifier.branchName,
            buildName: this.buildIdentifier.buildName,

            from, to,
            folder, fileName
        }
    }

    public async writeExecutions(): Promise<ExecutionsParquetMetadata> {
        const rows: ParquetExectutionRow[] = [];
        for (let execution of this.executionResolver) {
            rows.push({
                execution_id: execution.executionId,
                lab_id: execution.labId,
                test_stage: execution.testStage,
                start_time: execution.timestamp,
                end_time: execution.timestampDelete
            });
        }
        const {fileName, folder} = await this.uploadExecutionsFile(rows);

        return {
            customerId: this.buildIdentifier.customerId,
            appName: this.buildIdentifier.appName,
            branchName: this.buildIdentifier.branchName,
            buildName: this.buildIdentifier.buildName,

            folder, fileName
        }
    }

    public get fpBaseDir(): IStorageLocation2 {
        return new S3Location(this.bucket, [
            this.envName,
            'parquet-footprints',
        ]);
    }

    public get executionsBaseDir(): IStorageLocation2 {
        return new S3Location(this.bucket, [
            this.envName,
            'parquet-executions',
        ]);
    }

    private async getRowsFromFpFile(footprintsFile: IFootprintsV6File): Promise<ParquetFootprintsRow[]> {
        const rows: ParquetFootprintsRow[] = [];
        for (let executionHits of footprintsFile.executions) {
            const exectuion = await this.executionResolver.resolve(this.buildIdentifier.customerId, executionHits.executionId);
            const labId = exectuion.labId;
            for (let hits of executionHits.hits) {
                const hitStart = new Date(hits.start * 1000);
                const hitEnd = new Date(hits.end * 1000);

                if (hits.methods) {
                    rows.push(
                        ...this.getRowsFromHits(labId, hitStart, hitEnd, hits.methods, footprintsFile.methods)
                    );
                }
                if (hits.branches) {
                    rows.push(
                        ...this.getRowsFromHits(labId, hitStart, hitEnd, hits.branches, footprintsFile.branches)
                    );
                }
                if (hits.lines) {
                    rows.push(
                        ...this.getRowsFromHits(labId, hitStart, hitEnd, hits.lines, footprintsFile.lines)
                    );
                }
            }
        }
        return rows;
    }

    private getRowsFromHits(labId: string, hitStart: Date, hitEnd: Date, idxArray: number[], uniqueIds: string[]): ParquetFootprintsRow[] {
        let rows: ParquetFootprintsRow[] = []
        for (let idx of idxArray) {
            const uniqueId = uniqueIds[idx];
            rows.push({
                lab_id: labId,
                unique_id: uniqueId,
                hit_start: hitStart,
                hit_end: hitEnd
            });
        }
        return rows;
    }

    private async uploadFpFile(rows: ParquetFootprintsRow[], from: Date, to: Date): Promise<{fileName: string, folder: IStorageLocation2}> {
        const fileName = `${uuid.v4()}.parquet`;
        const folder = this.fpBaseDir.clone()
            .partition('customer_id', this.buildIdentifier.customerId)
            .partition('app_name', this.buildIdentifier.appName)
            .partition('branch_name', this.buildIdentifier.branchName)
            .partition('build_name', this.buildIdentifier.buildName)
            .partition('from',  from.toISOString().split('T')[0])
            .partition('to',  to.toISOString().split('T')[0]);
        
        const outputStream = new ParquetTransformer(ParquetWriter.fpSchema);
        await this.storage.uploadFileAsStream(folder.clone().add(fileName), fromArray(rows).pipe(outputStream));
        return {fileName, folder};
    }

    private async uploadExecutionsFile(rows: ParquetExectutionRow[]): Promise<{fileName: string, folder: IStorageLocation2}> {
        const fileName = `${uuid.v4()}.parquet`;
        const fileLocation = this.executionsBaseDir.clone()
            .partition('customer_id', this.buildIdentifier.customerId)
            .partition('app_name', this.buildIdentifier.appName)
            .partition('branch_name', this.buildIdentifier.branchName)
            .partition('build_name', this.buildIdentifier.buildName);

        const outputStream = new ParquetTransformer(ParquetWriter.executionSchema);
        await this.storage.uploadFileAsStream(fileLocation.clone().add(fileName), fromArray(rows).pipe(outputStream));
        return {fileName, folder: fileLocation};
    }
}
