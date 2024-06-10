export interface IFootprintsV6File {
    formatVersion: "6.0";    
    moduleName?: string;
    meta: IFootprintsV6Meta;
    methods: string[];
    branches: string[];
    lines: string[];
    executions: IFootprintsV6TestExecution[];
}

export interface IFootprintsV6Meta {
    agentId: string;
    labId: string;

    intervals: {
        timedFootprintsCollectionIntervalSeconds: number;
    }
}

export interface IFootprintsV6TestExecution {
    executionId: string; 
    hits: IFootprintsV6Hit[];
}

export interface IFootprintsV6Hit {
    methods?: number[];
    branches?: number[];
    methodLines?: MethodLineHits;
    lines?: number[];
    testName?: string; //For colored footprints
    testVariantId?: string; //For colored footprints (DDT)
    start: number,
    end: number,
    isInitFootprints?: boolean;
}

export interface MethodLineHits {
    [methodIdx: string]: number[];
}

export type SquishedFootprintsFileV6 = Array<{footprints: IFootprintsV6File}>;
