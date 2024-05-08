import OpenAgentsNode from "./OpenAgentsNode";
import { Writable, Readable } from "stream";
import type {
    RpcOutputStream,
 
} from "@protobuf-ts/runtime-rpc";
import {RpcDiskReadFileResponse} from "openagents-grpc-proto";

export default class Disk {
    id: string;
    url: string;
    node: OpenAgentsNode;
    closed: boolean = false;

    constructor(id: string, url: string, node: OpenAgentsNode) {
        this.id = id;
        this.url = url;
        this.node = node;
    }

    async list(prefix: string = "/"): Promise<string[]> {
        const client = this.node.getClient();
        const diskR = await client.r(client.diskListFiles({ diskId: this.id, path: prefix }));
        return diskR.files;
    }

    async delete(path: string): Promise<boolean> {
        const client = this.node.getClient();
        const res = await client.r(client.diskDeleteFile({ diskId: this.id, path }));
        return res.success;
    }

    async close(): Promise<void> {
        if (this.closed) return;
        const client = this.node.getClient();
        await client.r(client.closeDisk({ diskId: this.id }));
        this.closed = true;
    }

    async getUrl(): Promise<string> {
        return this.url;
    }

    async openReadStream(path: string): Promise<Readable> {
        const client = this.node.getClient();
        const rq = await client.rS(client.diskReadFile({ diskId: this.id, path }));
        
        const _readFromIterator = async (
            iterator: RpcOutputStream<RpcDiskReadFileResponse>,
            stream: Readable
        ) => {
            for await (const res of iterator) {
                if(!res.exists) throw new Error("File not found");
                if (!stream.push(res.data)) {
                    break;
                }
            }
            stream.push(null);
        };

        const readable = new Readable({
            read() {
                _readFromIterator(rq, this).catch((err) => this.emit("error", err));
            },
        });

        return readable;
    }

    async openWriteStream(path: string, chunk_size: number): Promise<Writable> {
        const client = this.node.getClient();
        const rq = await client.rC(client.diskWriteFile({ diskId: this.id, path, chunkSize: chunk_size }));
        let buffer = Buffer.alloc(0);
        const self = this;
        const writable = new Writable({
            write(chunk, encoding, callback) {
                buffer = Buffer.concat([buffer, chunk]);
                if (buffer.length >= chunk_size) {
                    const chunkToSend = buffer.subarray(0, chunk_size);
                    buffer = buffer.subarray(chunk_size);
                    rq.write({
                        diskId: self.id,
                        path,
                        data: chunkToSend,
                    });
                }
                callback();
            },
            final(callback) {
                if (buffer.length > 0) {
                    rq.write({
                        diskId: self.id,
                        path,
                        data: buffer,
                    });
                }
                rq.end();
                callback();
            },
        });
        return writable;
    }
}
