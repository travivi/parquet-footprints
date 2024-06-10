import {IBuildDependency, IBuildEntry, IBuildDocument} from "./model";
import {Connection, Model} from "mongoose";
import * as BuildEntryModel from "./model";
import { IBuildIdentifier, MapCfgKey2 } from "@sealights/sl-cloud-infra2";

export class DependenciesResolver {
    private buildModel: Model<IBuildEntry>;

    public constructor(private dbConnection: Connection) {
        this.buildModel = dbConnection.model<IBuildEntry, IBuildDocument>(BuildEntryModel.getModelName(), BuildEntryModel.buildSchema);
    }

    public async *resolve(buildIdentifier: IBuildIdentifier): AsyncGenerator<IBuildIdentifier> {
        const buildEntry: Pick<IBuildEntry, 'dependencies'> = await this.buildModel.findOne({
            customerId: buildIdentifier.customerId,
            appName: buildIdentifier.appName,
            branchName: buildIdentifier.branchName,
            buildName: buildIdentifier.buildName,
            "buildSession.buildSessionId": buildIdentifier.bsid
        }, {
            dependencies: 1
        });
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
    
                const depIdentifier: IBuildIdentifier = {
                    customerId: depEntry.buildSession.customerId,
                    appName: depEntry.buildSession.appName,
                    branchName: depEntry.buildSession.branchName,
                    buildName: depEntry.buildSession.buildName,
                    bsid: depEntry.buildSession.buildSessionId
                }
                yield depIdentifier;
            }
        }
    }
}
