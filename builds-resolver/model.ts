import { BuildSessionType, IBuildIdentifier } from "@sealights/sl-cloud-infra2";
import {Model} from "mongoose";
import mongoose = require("mongoose");

export enum BuildScanStatus {
    InProgress = "In Progress",
    Completed = "Completed",
    Failed = "Failed"
}

export enum BuildMethodology {
    Methods = "Methods",
    MethodLines = "MethodLines"
}

export enum BuildType {
    COMPONENT,
    INTEGRATED,
}

export interface IBuildDiffDependencyInsight {
    appName: string;
    branchName: string;
    buildName: string;
    isNewer?: boolean;
    isDifferentBranch?: boolean;
}

export interface IBuildDiffDependencyInsights {
    previousBuild?: IBuildDiffDependencyInsight;
    referenceBuild?: IBuildDiffDependencyInsight;
}

export interface IBuildDependency {
    appName: string;
    branch: string;
    build: string;
    status: "added" | "modified" | "unchanged" | "deleted";
    insights?: IBuildDiffDependencyInsights;
}

export interface IBranchBuild {
    branchName: string;
    buildName: string;
}

export interface IReferenceBuild extends IBranchBuild {
    buildsAgo: number
}

export interface IBuildSessionAlias {
    labId: string;
    alias: string;
}

export interface ILab {
    labId: string;
    appName: string;
    branchName: string;
    testEnv?: string;
    labAlias?: string;
    isMultiLab: boolean;
    cdOnly: boolean;
}

export interface IBuildSession {
    _id?: any;
    customerId: string;
    appName: string;
    branchName: string;
    buildName: string;
    buildSessionId: string;
    trackingId?: string;
    additionalParams?: any;
    created?: number;
    buildSessionType: BuildSessionType,
    aliases?: IBuildSessionAlias[];
    checksum?: string;
    lab?: ILab;
    tsrProductPipeline?: string;
    tsrTestStage?: string;
}

export interface IBuildSessionPullRequestParams {
    repositoryUrl: string;
    pullRequestNumber: number;
    latestCommit: string; //Head SHA
    targetBranch: string;

    latestBuildName?: string; //Added by the server after finding the latest build on the target branch
}

export interface IPullRequestBuildSessionData extends IBuildSession {
    pullRequestParams: IBuildSessionPullRequestParams;
    buildSessionType: BuildSessionType.PULLREQUEST;
}

export interface IScm {
    provider: string,
    type: string,
    version: string,
    baseUrl: string,
}

export enum EndOfBuildTrigger {
    Agent = "Agent",
    Server = "Server"
}

export enum BuildEntryUpdateStatus {
    Full = 'FULL',
    Partial = 'PARTIAL'
}

export enum BuildStatus {
    NO_TESTS = "noTests",
    RUNNING = "running",
    ENDED = "ended"
}

export enum QualityStatusResult {
    Passed = "passed",
    Failed = "failed",
    MissingData = "missingData",
}

export interface IBuildMetaData<T = Date> {
    generated: T; // can be string only when received from API save build
    buildType: BuildType,
    buildSourceBranch: string;
    technology: string;
    logsUrl: string;
    scmBaseUrl: string; // Deprecated
    repositoryUrl: string;
    isReference: boolean;
    deployedDate: Date;
    commit: string;
    commits: string[];
    commitLog: string[];
    isHidden: boolean;
    dependencies: IBuildDependency[];
    /* The N last builds prior to this ons*/
    previousBuilds: IBranchBuild[];
    previousBuild: IBranchBuild;
    referenceBuild: IReferenceBuild;
    etlId?: string;
    buildScanStatus: BuildScanStatus;
    buildFailureMessage: string;
    buildDuration: number;
    buildSession: IBuildSession | IPullRequestBuildSessionData;
    buildMethodology: BuildMethodology;

    buildScan?: {
        status: BuildScanStatus;
        date: Date;
        failureMessage: string;
        duration: number;
    };

    scm: IScm;
    endOfBuildTrigger?: EndOfBuildTrigger;
    numOfMethods: number;
    updateStatus: BuildEntryUpdateStatus;
    isGitDataCommitted: boolean;
    status: BuildStatus;

    forceBuildScanStatus?: boolean;

    tags?: string[];

    qualityGate?: QualityStatusResult;
}

export interface IBuildEntry extends IBuildIdentifier, IBuildMetaData {
    _id?: any;
}

export interface IBuildDocument extends Model<IBuildEntry> {
}

export const buildSchema = new mongoose.Schema<IBuildEntry>({
    customerId: {type: String, index: true, required: true},
    appName: {type: String, required: true},
    branchName: {type: String, required: true},
    buildName: {type: String, required: true},
    buildSourceBranch: {type: String, default: null},
    generated: {type: Date, required: true},
    buildType: {type: Number, default: 0}, // referring to enum BuildType: COMPONENT = 0, INTEGRATED = 1
    technology: {type: String},
    logsUrl: {type: String},
    jobName: {type: String},
    scmBaseUrl: {type: String},
    repositoryUrl: {type: String},
    commit: {type: String},
    commits: {type: []},
    commitLog: {type: [], default: []},
    isHidden: {type: Boolean, default: false},
    isReference: {type: Boolean, default: false},
    deployedDate: {type: Date, default: null},
    previousBuild: {
        type: {
            buildName: {type: String},
            branchName: {type: String},
        }, default: null
    },
    previousBuilds: {
        // we changed it to Array instead of a typed object since we ran into an issue that is detailed
        // here - https://github.com/Automattic/mongoose/issues/6540
        type: Array, default: []
    },
    referenceBuild: {
        type: {
            buildName: {type: String},
            branchName: {type: String},
            buildsAgo: {type: Number},
        }, default: null
    },
    dependencies: {
        // we changed it to Array instead of a typed object since we ran into an issue that is detailed
        // here - https://github.com/Automattic/mongoose/issues/6540
        type: Array, default: []
    },
    scm: {
        provider: {type: String},
        type: {type: String},
        version: {type: String},
        scmBaseUrl: {type: String},
    },
    etlId: {type: String, default: null},
    buildScanStatus: {type: String, default: BuildScanStatus.InProgress},
    buildScan: {
        status: {type: String, default: BuildScanStatus.InProgress},
        date: {type: Date},
        failureMessage: {type: String},
        duration: {type: Number},
    },
    buildFailureMessage: {type: String},
    buildDuration: {type: Number},
    buildSession: {type: Object},
    buildMethodology: {type: String, enum: Object.values(BuildMethodology)},
    endOfBuildTrigger: {type: String},
    numOfMethods: {type: Number},
    updateStatus: {type: String},
    isGitDataCommitted: {type: Boolean},
    status: {type: String},
    tags : {type: Array, default: []}
}, {strict: false}/*SLDEV-9463*/);

//Optimized for quick lookups
buildSchema.index({customerId: 1, appName: 1, branchName: 1, buildName: 1}, {unique: true});
buildSchema.index({customerId: 1, appName: 1, branchName: 1});
buildSchema.index({customerId: 1, repositoryUrl: 1}, {unique: false});
buildSchema.index({scmBaseUrl: "text", repositoryUrl: "text"});
buildSchema.index({customerId: 1, generated: -1, buildType: -1}, {unique: false});
buildSchema.index({customerId: 1, "buildSession.buildSessionId": 1});

//optimized for getLatestReferenceBuild
buildSchema.index({isReference: 1, customerId: 1, appName: 1, branchName: 1, generated: -1});
// optimized for getPreviousBuildByCommitsOrBranch
buildSchema.index({customerId: 1, appName: 1, branchName: 1, generated: 1});
//Optimization for getPreviousBuild
buildSchema.index({customerId: 1, appName: 1, commit: 1, buildSession: 1}, {unique: false});
//Optimization for getIntegBuilds
buildSchema.index({customerId: 1, buildType: 1, generated: -1, "dependencies.appName": 1, "dependencies.branch": 1, "dependencies.status": 1}, {unique: false});

buildSchema.index({customerId: 1, appName: 1, branchName: 1, tags: 1});
buildSchema.index({customerId: 1, appName: 1, tags: 1});

// optimize for etls
buildSchema.index({etlId: 1, customerId: 1},
    {
        unique: false,
        partialFilterExpression: {
            'buildSession.buildSessionType': BuildSessionType.BUILD
        }
    }
);

export function getModelName() {
    return "buildEntry";
}
