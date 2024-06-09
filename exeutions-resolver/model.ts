import {Model} from "mongoose";
import mongoose = require("mongoose");

export enum ExecutionType {
    Color = "color",
    Anonymous = "anonymous"
}

export enum ExecutionStatus {
    /**
     * Execution is open
     */
    Created = "created",

    /**
     * Execution is pending deletion. It will be returned to querying agents, and footprints will be accepted and processed
     */
    PendingDelete = "pendingDelete",

    /**
     * Execution is being deleted. It will NOT be returned to querying agents, but footprints will still be accpeted and processed
     */
    Deleting = "deleting",

    /**
     * Execution is closed. It will NOT be returned to querying agents, and footprints will NOT be accpeted
     */
    Ended = "ended",
}

export interface IExecutionTrigger {
    agentId?: string;
    txId: string;
}

export enum TriggerBy {
    Agent = "Agent",
    API = "API",
    Server = "Server",
    ServerNotAllowedRange = "ServerNotAllowedRange",
    SameLabAndStage = "SameLabAndStage",
    Cockpit = "Cockpit"
}

export enum EndReason {
    Conflict = "Conflict",
    Timeout = "Timeout",
    ContinuousTestStage = "ContinuousTestStage"
}

export interface IExecutionContext {
    bsid: string;
    appName: string;
    branchName: string;
    buildName: string;
    testStage: string;
}

export interface IStartExecutionTrigger extends IExecutionTrigger {

}

export interface IEndExecutionTrigger extends IExecutionTrigger {
    triggerBy: TriggerBy;
    endReason?: EndReason;
    force: boolean;
    daily?: boolean;
    triggerByContext?: IExecutionContext;
}


export interface IExecutionEntry {
    customerId: string;
    appName?: string;
    branchName?: string;
    buildName?: string;
    buildSessionId?: string;
    tsrId?: string;

    labId: string;
    testStage: string;
    testGroupId?: string;
    executionId: string;
    executionType?: ExecutionType;
    eventType: ExecutionStatus;

    timestamp: Date;
    timestampPendingDelete?: Date;
    timestampDelete?: Date;
    timestampDeleting?: Date;
    endExecutionTrigger?: IEndExecutionTrigger;
    startExecutionTrigger?: IStartExecutionTrigger;
    timeout?: number;
    continuousTestStage?: boolean;
}

export interface IExecutionDbDocument extends Model<IExecutionEntry> {
}

export const executionSchema = new mongoose.Schema<IExecutionEntry, IExecutionDbDocument>({
    customerId: {type: String, index: true},
    appName: {type: String},
    branchName: {type: String},
    buildName: {type: String},
    buildSessionId: {type: String},

    labId: {type: String},
    testStage: {type: String},
    testGroupId: {type: String, required: false},
    executionId: {type: String},
    executionType: {type: String}, // colored or anonymous
    eventType: {type: String},

    timestamp: {type: Date},
    timestampPendingDelete: {type: Date},
    timestampDelete: {type: Date},
    timestampDeleting: {type: Date},
    endExecutionTrigger: {
        triggerBy: {type: String},
        endReason: {type: String},
        force: {type: Boolean},
        agentId: {type: String},
        txId: {type: String},
        triggerByContext: {
            bsid: {type: String},
            appName: {type: String},
            branchName: {type: String},
            buildName: {type: String},
            testStage: {type: String}
        },
    },
    startExecutionTrigger: {agentId: {type: String}, txId: {type: String}},
    timeout: {type: Number},
    continuousTestStage: {type: Boolean},
}, {strict: false});

executionSchema.index({customerId: 1, labId: 1, timestamp: 1, timestampPendingDelete: 1, timestampDelete: 1});
executionSchema.index({customerId: 1, labId: 1});
executionSchema.index({customerId: 1, appName: 1, branchName: 1, buildName: 1});
executionSchema.index({customerId: 1, buildSessionId: 1});
executionSchema.index({customerId: 1, labId: 1, executionType: 1, eventType: 1}); // for getting irrelevant active anonymous executions
executionSchema.index({customerId: 1, buildSessionId: 1, timestamp: 1}, {background: true});
executionSchema.index({customerId: 1, appName: 1, branchName:1, buildName: 1, timestamp: 1}, {background: true});
executionSchema.index({timestampDelete: 1, customerId: 1, labId: 1}, {sparse: true}); // timestampDelete can be null
executionSchema.index({customerId: 1, labId: 1, continuousTestStage: 1}, {partialFilterExpression: {continuousTestStage: true}}); //index for documents with contnuousTestStage field set to true
executionSchema.index({eventType: 1, timestamp: 1}); //index for daily mapping cleanup
executionSchema.index({customerId: 1, executionId: 1});
