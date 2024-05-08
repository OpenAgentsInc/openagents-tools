import { Job, JobInput, JobStatus } from "openagents-grpc-proto";

import JobRunner from './JobRunner';
import OpenAgentsNode from './OpenAgentsNode';
import Fs from "fs";
import Disk from "./Disk";
import Logger from './Logger';

type KindRange = {
    min:number;
    max:number;
}


type NearbyDiscoveryFilter = {
    kinds?: number[];
    nodes?: string[];
    tags?: string[];
    kindRanges?: KindRange[];
};

type DiscoveryFilter = NearbyDiscoveryFilter& {    
    pools?: string[];
};
export default class JobContext {
    private job: Job;
    private runner: JobRunner;
    private node: OpenAgentsNode;
    private cachePath: string;
    private openedDiskByUrl: { [key: string]: Disk } = {};
    private openedDiskById: { [key: string]: Disk } = {};
    private openedDiskByName: { [key: string]: Disk } = {};
    private logger: Logger;

    constructor(node: OpenAgentsNode, runner: JobRunner, job: Job) {
        this.job = job;
        this.runner = runner;
        this.node = node;
        this.cachePath = process.env.CACHE_PATH || "cache";
        if (!Fs.existsSync(this.cachePath)) {
            Fs.mkdirSync(this.cachePath, { recursive: true });
        }
        this.logger = new Logger(
            node.getMeta().name + "." + runner.getMeta().name,
            node.getMeta().version,
            this.job.id,
            (log: string) => {
                this.node.logForJob(log, job.id);
            }
        );
    }

    public getLogger() {
        return this.logger;
    }

    public getRunner() {
        return this.runner;
    }

    public getNode() {
        return this.node;
    }

    public getJob() {
        return this.job;
    }

    public async cacheSet(
        key: string,
        value: any,
        version: number = 0,
        expireAt: number = 0,
        local: boolean = true,
        chunkSize: number = 1024 * 1024 * 15
    ): Promise<boolean> {
        try {
            if (local) {
                const cacheFile = `${this.cachePath}/${key}.json`;
                const metaFile = `${this.cachePath}/${key}.meta.json`;
                await Promise.all([
                    Fs.promises.writeFile(cacheFile, JSON.stringify(value), "utf-8"),
                    Fs.promises.writeFile(metaFile, JSON.stringify({ version, expireAt }), "utf-8"),
                ]);
                return true;
            } else {
                const client = this.node.getClient();
                const stream = await client.rC(client.cacheSet());
                const buffer = Buffer.from(JSON.stringify(value), "utf-8");
                let i = 0;
                while (i < buffer.length) {
                    const chunk = buffer.subarray(i, Math.min(i + chunkSize, buffer.length));
                    i += chunkSize;
                    await stream.write({
                        key: key,
                        data: chunk,
                        version: version,
                        expireAt: expireAt,
                    });
                }
                const res = await stream.end();
                return res.success;
            }
        } catch (e) {
            this.getLogger().error("Error in cacheSet", e);
            return false;
        }
    }

    public async cacheGet(key: string, lastVersion: number = 0, local: boolean = true) {
        try {
            if (local) {
                const cacheFile = `${this.cachePath}/${key}.json`;
                const metaFile = `${this.cachePath}/${key}.meta.json`;
                const cacheFileExists = Fs.existsSync(cacheFile);
                const metaFileExists = Fs.existsSync(metaFile);
                if (cacheFileExists && metaFileExists) {
                    const meta = JSON.parse(await Fs.promises.readFile(metaFile, "utf-8"));
                    if (meta.version > lastVersion) {
                        return JSON.parse(await Fs.promises.readFile(cacheFile, "utf-8"));
                    }
                }
            } else {
                const client = this.node.getClient();
                const stream = await client.rS(
                    client.cacheGet({
                        key: key,
                        lastVersion: lastVersion,
                    })
                );
                const bytes = [];
                for await (const res of stream) {
                    if (!res.exists) {
                        return undefined;
                    }
                    bytes.push(res.data);
                }
                return JSON.parse(Buffer.concat(bytes).toString("utf-8"));
            }
        } catch (e) {
            this.getLogger().error("Error in cacheGet", e);
            return undefined;
        }
    }

    public async openStorage(url: string): Promise<Disk> {
        if (this.openedDiskByUrl[url]) {
            return this.openedDiskByUrl[url];
        }
        const client = this.node.getClient();
        const diskR = await client.r(client.openDisk({ url }));
        const diskId = diskR.diskId;
        const disk = new Disk(diskId, url, this.node);
        this.openedDiskByUrl[url] = disk;
        this.openedDiskById[diskId] = disk;
        return disk;
    }

    public async createStorage(
        name?: string,
        encryptionKey?: string,
        includeEncryptionKeyInUrl?: boolean
    ): Promise<Disk> {
        if (this.openedDiskByName[name]) {
            return this.openedDiskByName[name];
        }
        const client = this.node.getClient();
        const diskR = await client.r(client.createDisk({ name, encryptionKey, includeEncryptionKeyInUrl }));
        const diskUrl = diskR.url;
        const disk = await this.openStorage(diskUrl);
        this.openedDiskByName[name] = disk;
        return disk;
    }

    public async close() {
        for (const d of Object.values(this.openedDiskByUrl)) {
            await d.close();
        }
        for (const d of Object.values(this.openedDiskById)) {
            await d.close();
        }
        for (const d of Object.values(this.openedDiskByName)) {
            await d.close();
        }
        this.openedDiskByUrl = {};
        this.openedDiskById = {};
        this.openedDiskByName = {};
        await this.logger.close();
    }

    public getJobParamValue(key: string, defaultValue?: string): string {
        if (this.job.param) {
            for (const param of this.job.param) {
                if (param.key === key) {
                    return param.value[0];
                }
            }
        }
        return defaultValue;
    }

    public getJobParamValues(key: string, defaultValue: string[] = []): string[] {
        if (this.job.param) {
            for (const param of this.job.param) {
                if (param.key === key) {
                    return param.value;
                }
            }
        }
        return defaultValue;
    }

    public getJobInput(marker?: string): JobInput {
        if (this.job.input) {
            for (const input of this.job.input) {
                if (!marker || input.marker === marker) {
                    return input;
                }
            }
        }
        return undefined;
    }

    public getJobInputs(marker?: string): JobInput[] {
        const inputs: JobInput[] = [];
        if (this.job.input) {
            for (const input of this.job.input) {
                if (!marker || input.marker === marker) {
                    inputs.push(input);
                }
            }
        }
        return inputs;
    }

    public getOutputFormat(): string {
        return this.job.outputFormat;
    }

    public async discoverActions(
        filter: DiscoveryFilter
    ): Promise<Array<{ template: string; meta: any; sockets: any }>> {
        const client = this.node.getClient();
        const actionsStr = await client.r(
            client.discoverActions({
                filterByKinds: filter.kinds,
                filterByNodes: filter.nodes,
                filterByTags: filter.tags,
                filterByPools: filter.pools,
                filterByKindRanges: filter.kindRanges.map((kr) => {
                    return "" + kr.min + "-" + kr.max;
                }),
            })
        );
        const actions = Array<{ template: string; meta: any; sockets: any }>();
        for (const actionStr of actionsStr.actions) {
            actions.push(JSON.parse(actionStr));
        }
        return actions;
    }

    public async discoverNearbyActions(
        filter: NearbyDiscoveryFilter
    ): Promise<Array<{ template: string; meta: any; sockets: any }>> {
        const client = this.node.getClient();
        const actionsStr = await client.r(
            client.discoverNearbyActions({
                filterByKinds: filter.kinds||[],
                filterByNodes: filter.nodes||[],
                filterByTags: filter.tags||[],
                filterByKindRanges: filter.kindRanges?filter.kindRanges.map((kr) => {
                    return "" + kr.min + "-" + kr.max;
                }):[],
            })
        );
        const actions = Array<{ template: string; meta: any; sockets: any }>();
        for (const actionStr of actionsStr.actions) {
            actions.push(JSON.parse(actionStr));
        }
        return actions;
    }

    public async sendJobRequest(event: string, provider?: string, encrypted?: boolean): Promise<Job> {
        const client = this.node.getClient();
        const res = await client.r(
            client.sendJobRequest({
                event: event,
                provider: provider,
                encrypted: encrypted,
            })
        );
        return res;
    }

    public async sendSubJobRequest(event: string) {
        const client = this.node.getClient();
        const res = await client.r(
            client.sendJobRequest({
                event: event,
                provider: this.job.provider,
                encrypted: this.job.encrypted,
            })
        );
        return res;
    }

    public async waitForContent(job:Job|Promise<Job>){
        const waited=await this.waitFor(job);
        return waited.result.content;
    }

    public async waitFor(job: Job|Promise<Job>): Promise<Job | undefined> {
        job=await job;
        const logger = this.getLogger();
        const client = this.node.getClient();
        const jobId = job.id;
        const logPassthrough = true;
        let lastLog = 0;
        while (true) {
            try {
                const job = await client.r(client.getJob({ jobId, wait: 1000 }));
                if (job) {
                    if (logPassthrough) {
                        for (const log of job.state.logs) {
                            if (log.timestamp > lastLog) {
                                logger.info(log.log);
                                lastLog = log.timestamp;
                            }
                        }
                    }
                    if (job.state.status == JobStatus.SUCCESS && job.result.timestamp) {
                        return job;
                    }
                }
            } catch (e) {
                // console.error(e);
            }
            await new Promise((res) => setTimeout(res, 100));
        }
        return undefined;
    }
}