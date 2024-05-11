
import {NodeMetaData} from "./NodeConfig";
import NodeConfig from "./NodeConfig";
import JobRunner from "./JobRunner";
import Logger from "./Logger";
import { PoolConnectorClient  } from "openagents-grpc-proto";
import * as GRPC from "@grpc/grpc-js";
import type { UnaryCall, RpcOutputStream, ServerStreamingCall, RpcOptions, ClientStreamingCall, RpcInputStream } from "@protobuf-ts/runtime-rpc";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import RunnerConfig from "./RunnerConfig";
import JobContext from "./JobContext";

type GrpcClient = PoolConnectorClient & {
    rS<T extends object>(c: ServerStreamingCall<object, T>): Promise<RpcOutputStream<T>>;
    rC<T extends object,O extends object>(c: ClientStreamingCall<T, O>): Promise<{write: (message:T)=>void, end: ()=>Promise<O>}>;
    r<T extends object>(c: UnaryCall<object, T> | Promise<UnaryCall<object, T>>): Promise<T>;
};

export default class OpenAgentsNode {
    private meta: NodeMetaData;
    private registeredRunners: {
        runner: JobRunner;
        nextAnnouncementTimestamp: number;
    }[] = [];
    private logger: Logger;
    private client: GrpcClient;
    private channel: GrpcTransport;
    protected poolAddress: string;
    protected poolPort: number;
    protected poolSsl: boolean;
    protected loopInterval: number = 1000.0 / 10;
    private nextNodeAnnounce: number = 0;
    private runnerTasks: Map<JobRunner, any> = new Map();
    private lockedJobs: Array<{
        jobId: string;
        timestamp: number;
    }> = [];

    constructor(config: NodeConfig) {
        this.meta = config.getMeta();
        this.logger = new Logger(this.meta.name, this.meta.version);
    }

    public getMeta(): NodeMetaData {
        return this.meta;
    }

    public getLogger(): Logger {
        return this.logger;
    }

    public registerRunner(  runner: JobRunner) {
        this.registeredRunners.push({
            runner: runner,
            nextAnnouncementTimestamp: 0,
        });
    }

    public getClient() {
        if (!this.client || !this.channel) {
            const clientOptions = {
                // 20 MB
                "grpc.max_send_message_length": 20 * 1024 * 1024,
                "grpc.max_receive_message_length": 20 * 1024 * 1024,
            };
            const channelCredentials = !this.poolSsl
                ? GRPC.ChannelCredentials.createInsecure()
                : GRPC.ChannelCredentials.createSsl();

            this.channel = new GrpcTransport({
                host: `${this.poolAddress}:${this.poolPort}`,
                channelOptions: clientOptions,
                channelCredentials: channelCredentials,
                meta: {
                    authorization: process.env.NODE_TOKEN || undefined,
                },
            });

            this.getLogger().info("Connecting to pool at ", this.poolAddress, ":", this.poolPort);

            this.client = new PoolConnectorClient(this.channel) as GrpcClient;

            this.client.r = async <T extends object>(
                c: UnaryCall<object, T> | Promise<UnaryCall<object, T>>
            ) => {
                const cc = await c;
                const rpcStatus = await cc.status;
                if (!(rpcStatus.code.toString() == "0" || rpcStatus.code.toString() == "OK")) {
                    throw new Error(`rpc failed with status ${rpcStatus.code}: ${rpcStatus.detail}`);
                }
                return cc.response;
            };
            this.client.rS = async <T extends object>(c: ServerStreamingCall<object, T>) => {
                const rpcStatus = await c.status;
                if (!(rpcStatus.code.toString() == "0" || rpcStatus.code.toString() == "OK")) {
                    throw new Error(`rpc failed with status ${rpcStatus.code}: ${rpcStatus.detail}`);
                }
                return c.responses;
            };

            this.client.rC = async <T extends object, O extends object>(c: ClientStreamingCall<T, O>) => {
                return {
                    write: c.requests.send,
                    end: async () => {
                        await c.requests.complete();
                        const rpcStatus = await c.status;
                        if (!(rpcStatus.code.toString() == "0" || rpcStatus.code.toString() == "OK")) {
                            throw new Error(`rpc failed with status ${rpcStatus.code}: ${rpcStatus.detail}`);
                        }
                        return c.response;
                    },
                };
            };

 
          
         
        }
        return this.client;
    }

    public async logForJob(message: string, jobId: string) {
        return this.getClient().r(this.getClient().logForJob({ log: message, jobId: jobId }));
    }

    public async acceptJob(jobId: string) {
        return this.getClient().r(this.getClient().acceptJob({ jobId: jobId }));
    }

    public async cancelJob(jobId: string, reason: string) {
        return this.getClient().r(this.getClient().cancelJob({ jobId, reason }));
    }

    public async completeJob(jobId: string, output: string) {
        return this.getClient().r(this.getClient().completeJob({ jobId, output }));
    }

    protected async reannounce() {
        try {
            const time_ms = Date.now();
            if (time_ms >= this.nextNodeAnnounce) {
                try {
                    const client = this.getClient();
                    const res = await this.getClient().r(
                        client.announceNode({
                            iconUrl: this.meta.picture || "",
                            name: this.meta.name || "",
                            description: this.meta.description || "",
                        })
                    );
                    this.nextNodeAnnounce = Date.now() + res.refreshInterval;
                    this.getLogger().log(
                        "Node announced, next announcement in " + res.refreshInterval + " ms"
                    );
                } catch (e) {
                    this.getLogger().error("Error announcing node " + e);
                    this.nextNodeAnnounce = Date.now() + 5000;
                }
            }

            for (let reg of this.registeredRunners) {
                try {
                    if (time_ms >= reg.nextAnnouncementTimestamp) {
                        let client = this.getClient();
                        const res = await client.r(
                            client.announceEventTemplate({
                                meta: JSON.stringify(reg.runner.getMeta()),
                                template: reg.runner.getTemplate(),
                                sockets: JSON.stringify(reg.runner.getSockets()),
                            })
                        );
                        reg.nextAnnouncementTimestamp = time_ms + res.refreshInterval;
                        this.getLogger().log(
                            "Template announced, next announcement in " + res.refreshInterval + " ms"
                        );
                    }
                } catch (e) {
                    this.getLogger().error("Error announcing template " + e);
                }
            }
        } catch (e) {
            this.getLogger().error("Error reannouncing " + e);
        }
        setTimeout(() => this.reannounce(), 5000);
    }

    protected async loop(): Promise<void> {
        await Promise.all(this.registeredRunners.map((reg) => reg.runner.loop(this)));
        setTimeout(() => this.loop(), this.loopInterval);
    }

    protected async executePendingJobsForRunner(runner: JobRunner) {
        try {
            if (!this.registeredRunners.find((r) => r.runner == runner)) {               
                this.runnerTasks.delete(runner);
                return;
            }
            if (!runner.initialized){
                runner.initialized = true;
                await runner.init(this);
            } 
            let client = this.getClient();
            const jobs = [];
            const filter = runner.getFilter();

            this.lockedJobs = this.lockedJobs.filter((j) => {
                return Date.now() - j.timestamp < 60000;
            });

            const res = await client.r(
                client.getPendingJobs({
                    filterByRunOn: filter.filterByRunOn,
                    filterByCustomer: filter.filterByCustomer,
                    filterByDescription: filter.filterByDescription,
                    filterById: filter.filterById,
                    filterByKind: filter.filterByKind,
                    wait: 60000,
                    excludeId: this.lockedJobs.map((j) => j.jobId),
                })
            );
            jobs.push(...res.jobs);

            if (jobs.length > 0) {
                this.getLogger().log("Found " + jobs.length + " jobs for runner " + runner.getMeta().name);
            } else {
                this.getLogger().log("No jobs for runner " + runner.getMeta().name);
            }

            for (const job of jobs) {
                let wasAccepted = false;
                const ctx=new JobContext(this,runner,job);

                const t = Date.now();
                try {
                    client = this.getClient(); // refresh client connection if needed
                    if (!runner.canRun(ctx)) {
                        continue;
                    }
                    this.lockedJobs.push({ jobId: job.id, timestamp: Date.now() });
                    await this.acceptJob(job.id);
                    wasAccepted = true;
                    ctx.getLogger().info("Job started on node " + this.meta.name);
                    const task = async () => {
                        try {
                            await runner.preRun(ctx);
                            const out = await runner.run(ctx);
                            await runner.postRun(ctx);
                            ctx.getLogger().info(
                                "Job completed in",
                                Math.floor((Date.now() - t) / 1000),
                                "seconds on node " + this.meta.name
                            );
                            await this.completeJob(job.id, out);
                        } catch (e) {
                            ctx.getLogger().error(
                                "Job failed in",
                                Math.floor((Date.now() - t) / 1000),
                                "seconds on node " + this.meta.name,
                                e
                            );
                            if (wasAccepted) {
                                await this.cancelJob(job.id, e.toString());
                            }
                        }
                        ctx.close();
                    };
                    if (runner.isRunInParallel()) {
                        task();
                    } else {
                        await task();
                    }
                } catch (e) {
                    ctx.getLogger().error(
                        "Job failed in",
                        Math.floor((Date.now() - t) / 1000),
                        "seconds on node " + this.meta.name,
                        e
                    );
                    ctx.close();
                    if (wasAccepted) {
                        await this.cancelJob(job.id, e.toString());
                    }
                }
            }
        } catch (e) {
            this.getLogger().error("Error executing jobs for runner" + e);
            await new Promise((resolve) => setTimeout(resolve, 5000));
        }
        setTimeout(() => {
            this.runnerTasks.set(runner, this.executePendingJobsForRunner(runner));
        }, 1);
    }

    protected async executePendingJobs() {
        for (let reg of this.registeredRunners) {
            if (!this.runnerTasks.has(reg.runner)) {
                this.runnerTasks.set(reg.runner, this.executePendingJobsForRunner(reg.runner));
            }
        }
        setTimeout(() => this.executePendingJobs(), 1000);
    }

    public async start(poolHost?: string, poolPort?: number, poolSsl?: boolean, loopInterval?: number) {
        this.poolAddress = poolHost || process.env.POOL_ADDRESS || "127.0.0.1";
        this.poolPort = poolPort || parseInt(process.env.POOL_PORT || "5000");
        this.poolSsl = poolSsl || process.env.POOL_SSL == "true";
        this.loopInterval = loopInterval || 1000.0 / parseInt(process.env.NODE_TPS || "10");

        this.reannounce();
        this.loop();
        this.executePendingJobs();
    }
}