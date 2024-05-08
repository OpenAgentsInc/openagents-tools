import Logger from "./Logger";
import OpenAgentsNode from "./OpenAgentsNode";
import RunnerConfig from "./RunnerConfig";
import { RunnerMetaData,JobFilter,SocketsSchema } from "./RunnerConfig";
import goodbyes from "graceful-goodbye";

import JobContext from "./JobContext";
export default abstract class JobRunner {
    private meta: RunnerMetaData;
    private filter: JobFilter;
    private sockets: SocketsSchema;
    private template: string;
    private runInParallel: boolean = false;
    private logger: Logger;
    private selfLogger: boolean = false;
    public initialized: boolean=false;
    constructor(config: RunnerConfig) {
        this.meta = config.getMeta();
        this.filter = config.getFilter();
        this.sockets = config.getSockets();
        this.template = config.getTemplate();
        
    }

    public getMeta(): RunnerMetaData {
        return this.meta;
    }

    public getFilter(): JobFilter {
        return this.filter;
    }

    public getSockets(): SocketsSchema {
        return this.sockets;
    }

    public getTemplate(): string {
        return this.template;
    }

    public setRunInParallel(runInParallel: boolean) {
        this.runInParallel = runInParallel;
    }

    public isRunInParallel(): boolean {
        return this.runInParallel;
    }

    public async loop(node: OpenAgentsNode) {}
    public async init(node: OpenAgentsNode) {}

    public async canRun(ctx: JobContext): Promise<boolean> {
        return true;
    }

    public async preRun(ctx: JobContext): Promise<void> {}
    public abstract run(ctx: JobContext): Promise<string>;
    public async postRun(ctx: JobContext): Promise<void> {}
}