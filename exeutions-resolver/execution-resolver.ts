import {IExecutionDbDocument, IExecutionEntry, executionSchema} from "./model";
import {Connection, Model} from "mongoose";

export class ExecutionResolver {
    private executionsModel: Model<IExecutionEntry>;
    private resolvedExecutions: Map<string, IExecutionEntry>;

    public constructor(private dbConnection: Connection) {
        this.executionsModel = this.dbConnection.model<IExecutionEntry, IExecutionDbDocument>("testsStateTracker", executionSchema);
        this.resolvedExecutions = new Map();
    }

    public async resolve(customerId: string, executionId: string): Promise<IExecutionEntry> {
        if (this.resolvedExecutions.has(executionId)) {
            return this.resolvedExecutions.get(executionId);
        }
        const execution = await this.executionsModel.findOne({
            customerId,
            executionId
        });
        this.resolvedExecutions.set(executionId, execution);
        return execution;
    }

    public close(): Promise<void> {
        return this.dbConnection.close();
    }

    [Symbol.iterator]() {
        return this.resolvedExecutions.values();
    }
}
