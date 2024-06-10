import { CONVERSION_FORMAT, IBuildIdentifier, IStorage, S3Location, StorageKeyBuilder } from "@sealights/sl-cloud-infra2";
import { IFootprintsV6File, SquishedFootprintsFileV6 } from "./contracts";
import { ConsoleLogger } from ".";

export type FootrpintsResolverArgs = {
    storage: IStorage,
    bucket: string,
    envName: string,
    logger: ConsoleLogger
}

export type FootprintsResolverBatchItem = (SquishedFootprintsFileV6[number] & {buildSessionId: string});

export class FootprintsResolver {
    private storage: IStorage;
    private bucket: string;
    private envName: string;
    private logger: ConsoleLogger;
    constructor(args: FootrpintsResolverArgs) {
        this.storage = args.storage;
        this.bucket = args.bucket;
        this.envName = args.envName;
        this.logger = args.logger;
    }

    public async *resolve(buildIdentifier: IBuildIdentifier, batchSize: number): AsyncGenerator<FootprintsResolverBatchItem[]> {
        const baseFolders = [
            'test-footprint-files-v6',
            buildIdentifier.customerId,
            buildIdentifier.appName,
            buildIdentifier.branchName,
            buildIdentifier.buildName
        ];
        const baseLocation = new S3Location(this.bucket, this.envName).add(...baseFolders);
        const files = await this.storage.listFiles(baseLocation.bucket, {
            prefix: `${baseLocation.storageKey}`,
            delimiter: ''
        });
        this.logger.info(`Found ${files.length} files for '${baseLocation.storageKey}'`);
        let batch: FootprintsResolverBatchItem[] = [];
        for (let file of files) {
            const fileLocation = new S3Location(this.bucket, file.key);
            const componentBuildSessionId = fileLocation.keyParts[7];
            try {
                const squishedFileRaw = await this.storage.downloadAndDecompressFile<string>(fileLocation, {
                    output: CONVERSION_FORMAT.STRING
                });
                const footprints: SquishedFootprintsFileV6 = squishedFileRaw.split('\n').filter(raw => raw).map(raw => JSON.parse(raw));
                this.logger.info(`Adding footprints file '${fileLocation.storageKey}'`)
                for (let fp of footprints) {
                    batch.push({
                        ...fp,
                        buildSessionId: componentBuildSessionId
                    });
                    if (batch.length === batchSize) {
                        yield batch;
                        batch = [];
                    }
                }
            } catch (err) {
                this.logger.error(`Download failed, skipping file '${fileLocation.storageKey}'`, err)
            }
        }
        if (batch.length > 0) {
            yield batch;
            batch = [];
        }
    }
}
