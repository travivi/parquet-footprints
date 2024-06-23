import {IFootprintsV6File} from "./contracts";
import {ParquetSchema, ParquetTransformer} from 'parquetjs';
import {IBuildIdentifier, IStorage, IStorageLocation2, S3Location, mapGetOrAdd} from "@sealights/sl-cloud-infra2";
import {ExecutionResolver} from "./exeutions-resolver/execution-resolver";
import {fromArray} from 'object-stream';
import * as uuid from 'uuid';
import { FootprintsResolverBatchItem } from "./footprints-resolver";
import { ConsoleLogger } from ".";

export type FootprintsParquetWriterArgs = {
    storage: IStorage,
    executionResolver: ExecutionResolver;
    bucket: string,
    envName: string,
    buildIdentifier: IBuildIdentifier
    logger: ConsoleLogger;
}

export interface FootprintsParquetMetadata {
    customerId: string;
    appName: string;
    branchName: string;
    buildName: string;
    buildSessionId: string;
    labId: string;
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
    labId: string;
    from: Date;
    to: Date;
    folder: IStorageLocation2;
    fileName: string;
}

export interface ParquetFootprintsRow {
    unique_id: string,
    hit_start: Date,
    hit_end: Date
}

export interface ParquetExectutionRow {
    execution_id: string;
    test_stage: string;
    start_time: Date;
    end_time: Date;
}

export class ParquetWriter {
    private static readonly fpSchema: ParquetSchema = new ParquetSchema({
        unique_id: { type: "UTF8" },
        hit_start: { type: 'TIMESTAMP_MILLIS' },
        hit_end: { type: 'TIMESTAMP_MILLIS' },
    });
    private static readonly executionSchema: ParquetSchema = new ParquetSchema({
        execution_id: { type: "UTF8" },
        test_stage: { type: "UTF8" },
        start_time: { type: 'TIMESTAMP_MILLIS' },
        end_time: { type: 'TIMESTAMP_MILLIS' },
    });
    private storage: IStorage;
    private executionResolver: ExecutionResolver;
    private writtenExecutions: Set<string>;
    private bucket: string;
    private envName: string;
    private buildIdentifier: IBuildIdentifier;
    private logger: ConsoleLogger;
    constructor(args: FootprintsParquetWriterArgs) {
        this.storage = args.storage;
        this.executionResolver = args.executionResolver;
        this.bucket = args.bucket;
        this.envName = args.envName;
        this.buildIdentifier = args.buildIdentifier;
        this.writtenExecutions = new Set();
        this.logger = args.logger;
    }

    public async writeFp(footprintsFiles: FootprintsResolverBatchItem[]): Promise<FootprintsParquetMetadata[]> {        
        const rowsPerLabAndComponent: Map<string, {
            rows: ParquetFootprintsRow[],
            buildIdentifier: IBuildIdentifier,
            from: number,
            to: number
        }> = new Map();
        for (let footprintsFile of footprintsFiles) {
            for await (let {labId, rows, start, end} of this.getRowsFromFpFile(footprintsFile.footprints)) {
                const mapKey = [
                    labId,
                    footprintsFile.buildIdentifier.bsid,
                    footprintsFile.buildSessionId
                ].join('$$');
                const item = mapGetOrAdd(rowsPerLabAndComponent, mapKey, {
                    rows: [],
                    buildIdentifier: null,
                    from: null,
                    to: null
                });
                item.rows.push(...rows);
                item.buildIdentifier = footprintsFile.buildIdentifier;
                item.from = item.from === null ? start : Math.min(item.from, start);
                item.to = item.to === null ? end : Math.max(item.to, end);
                rowsPerLabAndComponent.set(mapKey, item);
            }
        }

        const files: FootprintsParquetMetadata[] = [];
        for (let [mapKey, data] of rowsPerLabAndComponent) {
            const [labId, mainBuildSessionId ,componentBuildSessionId] = mapKey.split('$$');
            const from = new Date(data.from);
            const to = new Date(data.to);
            const {fileName, folder} = await this.uploadFpFile(data.rows, data.buildIdentifier, componentBuildSessionId, labId, from, to);
            files.push({
                customerId: data.buildIdentifier.customerId,
                appName: data.buildIdentifier.appName,
                branchName: data.buildIdentifier.branchName,
                buildName: data.buildIdentifier.buildName,
                buildSessionId: componentBuildSessionId,
                labId,

                from,
                to,
                folder, fileName
            })
        }

        return files;
    }

    public async writeExecutions(): Promise<ExecutionsParquetMetadata[]> {
        const rowsPerLab: Map<string, {
            rows: ParquetExectutionRow[],
            from: number,
            to: number
        }> = new Map();
        for (let execution of this.executionResolver) {
            if (this.writtenExecutions.has(execution.executionId)) {
                continue;
            }
            const item = mapGetOrAdd(rowsPerLab, execution.labId, {
                rows: [],
                from: null,
                to: null
            });
            item.rows.push({
                execution_id: execution.executionId,
                test_stage: execution.testStage,
                start_time: execution.timestamp,
                end_time: execution.timestampDelete
            });
            item.from = item.from === null ? execution.timestamp.valueOf() : Math.min(item.from, execution.timestamp.valueOf());
            item.to = item.to === null ? execution.timestampDelete.valueOf() : Math.max(item.to, execution.timestampDelete.valueOf());
            rowsPerLab.set(execution.labId, item);
        }

        const files: ExecutionsParquetMetadata[] = [];
        for (let [labId, data] of rowsPerLab) {
            const from = new Date(data.from);
            const to = new Date(data.to);
            const {fileName, folder} = await this.uploadExecutionsFile(labId, from, to, data.rows);
            files.push({
                customerId: this.buildIdentifier.customerId,
                appName: this.buildIdentifier.appName,
                branchName: this.buildIdentifier.branchName,
                buildName: this.buildIdentifier.buildName,
                labId,

                from,
                to,
                folder, fileName
            })
        }

        for (let lab of rowsPerLab.values()) {
            lab.rows.forEach(row => this.writtenExecutions.add(row.execution_id));
        }

        return files;
    }

    public get fpBaseDir(): IStorageLocation2 {
        return new S3Location(this.bucket, [
            this.envName,
            'footprints-parquet',
        ]);
    }

    public get executionsBaseDir(): IStorageLocation2 {
        return new S3Location(this.bucket, [
            this.envName,
            'executions-parquet',
        ]);
    }

    private async *getRowsFromFpFile(footprintsFile: IFootprintsV6File): AsyncGenerator<{labId: string, start: number, end: number, rows: ParquetFootprintsRow[]}> {
        for (let executionHits of footprintsFile.executions) {
            const exectuion = await this.executionResolver.resolve(this.buildIdentifier.customerId, executionHits.executionId);
            const labId = exectuion.labId;
            const rows: ParquetFootprintsRow[] = [];
            let start: number = null;
            let end: number = null;
            for (let hits of executionHits.hits) {
                const hitStart = new Date(hits.start * 1000);
                const hitEnd = new Date(hits.end * 1000);

                if (hits.methods) {
                    rows.push(
                        ...this.getRowsFromHits(hitStart, hitEnd, hits.methods, footprintsFile.methods)
                    );
                }
                if (hits.branches) {
                    rows.push(
                        ...this.getRowsFromHits(hitStart, hitEnd, hits.branches, footprintsFile.branches)
                    );
                }
                if (hits.lines) {
                    rows.push(
                        ...this.getRowsFromHits(hitStart, hitEnd, hits.lines, footprintsFile.lines)
                    );
                }
                start = start === null ? hitStart.valueOf() : Math.min(start, hitStart.valueOf());
                end = end === null ? hitEnd.valueOf() : Math.max(end, hitEnd.valueOf());
            }
            yield {labId, rows, start, end};
        }
    }

    private getRowsFromHits(hitStart: Date, hitEnd: Date, idxArray: number[], uniqueIds: string[]): ParquetFootprintsRow[] {
        let rows: ParquetFootprintsRow[] = []
        for (let idx of idxArray) {
            const uniqueId = uniqueIds[idx];
            rows.push({
                unique_id: uniqueId,
                hit_start: hitStart,
                hit_end: hitEnd
            });
        }
        return rows;
    }

    private async uploadFpFile(rows: ParquetFootprintsRow[], buildIdentifier: IBuildIdentifier, buildSessionId: string, labId: string, from: Date, to: Date): Promise<{fileName: string, folder: IStorageLocation2}> {
        const fileName = `${uuid.v4()}.parquet`;
        const folder = this.fpBaseDir.clone()
            .partition('customer_id', buildIdentifier.customerId)
            .partition('app_name', buildIdentifier.appName)
            .partition('branch_name', buildIdentifier.branchName)
            .partition('build_name', buildIdentifier.buildName)
            .partition('build_session_id', buildSessionId)
            .partition('lab_id', labId)
            .partition('from', from.toISOString())
            .partition('to', to.toISOString());
        
        const outputStream = new ParquetTransformer(ParquetWriter.fpSchema);
        await this.storage.uploadFileAsStream(folder.clone().add(fileName), fromArray(rows).pipe(outputStream));
        return {fileName, folder};
    }

    private async uploadExecutionsFile(labId: string, from: Date, to: Date, rows: ParquetExectutionRow[]): Promise<{fileName: string, folder: IStorageLocation2}> {
        const fileName = `${uuid.v4()}.parquet`;
        const fileLocation = this.executionsBaseDir.clone()
            .partition('customer_id', this.buildIdentifier.customerId)
            .partition('app_name', this.buildIdentifier.appName)
            .partition('branch_name', this.buildIdentifier.branchName)
            .partition('build_name', this.buildIdentifier.buildName)
            .partition('lab_id', labId)
            .partition('from', from.toISOString())
            .partition('to', to.toISOString());

        const outputStream = new ParquetTransformer(ParquetWriter.executionSchema);
        await this.storage.uploadFileAsStream(fileLocation.clone().add(fileName), fromArray(rows).pipe(outputStream));
        return {fileName, folder: fileLocation};
    }
}
