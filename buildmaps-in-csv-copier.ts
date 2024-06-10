import { IBuildIdentifier, IStorage, IStorageLocation2, S3Location } from "@sealights/sl-cloud-infra2";
import { ConsoleLogger } from ".";

export type BuildmapsInCsvCopierArgs = {
    sourceStorage: IStorage,
    destinationStorage: IStorage,
    sourceBucket: string,
    sourceEnv: string,
    destinationBucket: string,
    destinationEnv: string,
    logger: ConsoleLogger
}

export interface BuildmapCsvMetadata {
    customerId: string;
    appName: string;
    branchName: string;
    buildName: string;
    bsid: string;

    folder: IStorageLocation2;
    fileName: string;
}

export class BuildmapsInCsvCopier {
    private sourceStorage: IStorage;
    private destinationStorage: IStorage;
    private sourceBucket: string;
    private sourceEnv: string;
    private destinationBucket: string;
    private destinationEnv: string;
    private logger: ConsoleLogger;
    constructor(args: BuildmapsInCsvCopierArgs) {
        this.sourceStorage = args.sourceStorage;
        this.destinationStorage = args.destinationStorage;
        this.sourceBucket = args.sourceBucket;
        this.sourceEnv = args.sourceEnv;
        this.destinationBucket = args.destinationBucket;
        this.destinationEnv = args.destinationEnv;
        this.logger = args.logger;
    }

    public get destinationBaseDir(): IStorageLocation2 {
        return new S3Location(this.destinationBucket, [
            'core-v2',
            'build-maps-in-csv',
        ])
    }

    public async *copy(buildIdentifier: IBuildIdentifier): AsyncGenerator<BuildmapCsvMetadata> {
        const baseFolders = [
            'core-v2',
            'build-maps-in-csv',
            buildIdentifier.customerId,
            buildIdentifier.appName,
            buildIdentifier.branchName,
            buildIdentifier.buildName,
            buildIdentifier.bsid
        ];
        const sourceLocation = new S3Location(this.sourceBucket, this.sourceEnv).add(...baseFolders);
        const destinationLocation = new S3Location(this.destinationBucket, this.destinationEnv).add(...baseFolders);

        const files = await this.sourceStorage.listFiles(sourceLocation.bucket, {
            prefix: `${sourceLocation.storageKey}`,
            delimiter: ''
        });
        this.logger.info(`Found ${files.length} files for '${sourceLocation.storageKey}'`);
        for (let file of files) {
            const fileName = file.key.split('/').pop();
            const fileReadLocation = new S3Location(this.sourceBucket, file.key);
            const fileWriteLocation = destinationLocation.clone().add(fileName);
            const fileStream = this.sourceStorage.downloadFileAsStream(fileReadLocation);
            await this.destinationStorage.uploadFileAsStream(fileWriteLocation, fileStream)
            yield {
                customerId: buildIdentifier.customerId,
                appName: buildIdentifier.appName,
                branchName: buildIdentifier.branchName,
                buildName: buildIdentifier.buildName,
                bsid: buildIdentifier.bsid,
                folder: fileWriteLocation, fileName
            };
        }
    }
}
