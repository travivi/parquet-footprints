import {IBuildEntry, IBuildDocument} from "./model";
import {Connection, Model} from "mongoose";
import * as BuildEntryModel from "./model";
import { IBuildIdentifier } from "@sealights/sl-cloud-infra2";

export class BuildsResolver {
    private buildModel: Model<IBuildEntry>;

    public constructor(dbConnection: Connection) {
        this.buildModel = dbConnection.model<IBuildEntry, IBuildDocument>(BuildEntryModel.getModelName(), BuildEntryModel.buildSchema);
    }

    public async resolveDependencies(buildIdentifier: IBuildIdentifier): Promise<IBuildIdentifier[]> {
        const buildEntry: Pick<IBuildEntry, 'dependencies'> = await this.buildModel.findOne({
            customerId: buildIdentifier.customerId,
            appName: buildIdentifier.appName,
            branchName: buildIdentifier.branchName,
            buildName: buildIdentifier.buildName,
            "buildSession.buildSessionId": buildIdentifier.bsid
        }, {
            dependencies: 1
        });
        const deps: IBuildIdentifier[] = [];
        if (buildEntry && buildEntry.dependencies) {
            for (let dep of buildEntry.dependencies) {
                const depEntry: Pick<IBuildEntry, 'buildSession'> = await this.buildModel.findOne({
                    customerId: buildIdentifier.customerId,
                    appName: dep.appName,
                    branchName: dep.branch,
                    buildName: dep.build
                }, {
                    "buildSession": 1
                });
    
                deps.push({
                    customerId: depEntry.buildSession.customerId,
                    appName: depEntry.buildSession.appName,
                    branchName: depEntry.buildSession.branchName,
                    buildName: depEntry.buildSession.buildName,
                    bsid: depEntry.buildSession.buildSessionId
                });
            }
        }
        return deps;
    }

    public async *iterateHistory(buildIdentifier: IBuildIdentifier, limit: number = 50): AsyncGenerator<IBuildIdentifier> {
        yield buildIdentifier;
        let yielded = 1;

        let lastIdentifier = buildIdentifier;
        while (yielded < limit) {
            for await (let buildInHistory of this.getHistory(lastIdentifier, limit - yielded)) {
                yield buildInHistory;
                lastIdentifier = buildInHistory;
                yielded++;
            }
        }
    }

    private async *getHistory(buildIdentifier: IBuildIdentifier, limit: number): AsyncGenerator<IBuildIdentifier> {
        let yielded = 0;

        const {previousBuilds} = await this.buildModel.findOne({
            customerId: buildIdentifier.customerId,
            appName: buildIdentifier.appName,
            branchName: buildIdentifier.branchName,
            buildName: buildIdentifier.buildName,
            "buildSession.buildSessionId": buildIdentifier.bsid
        }, {
            previousBuilds: 1
        });
        for (let prevBuild of previousBuilds) {
            const {buildSession} = await this.buildModel.findOne({
                customerId: buildIdentifier.customerId,
                appName: buildIdentifier.appName,
                branchName: prevBuild.branchName,
                buildName: prevBuild.buildName
            }, {
                buildSession: 1
            });
            yield {
                customerId: buildSession.customerId,
                appName: buildSession.appName,
                branchName: buildSession.branchName,
                buildName: buildSession.buildName,
                bsid: buildSession.buildSessionId
            };
            yielded++;
            if (yielded >= limit) {
                return;
            }
        }
    }
}
