
export type RunnerMetaData={
    kind?:number,
    name?:string,
    description?:string,
    tos?:string,
    privacy?:string,
    author?:string,
    web?:string,
    picture?:string,
    tags?:string[]
}

export type JobFilter = {
    filterByRunOn?: string;
    filterByCustomer?: string;
    filterByDescription?: string;
    filterById?: string;
    filterByKind?: string;
};

export type SocketsSchema = {
    in: {};
    out: {};
};

export default class RunnerConfig {
    meta: RunnerMetaData;
    template: string;
    filter: JobFilter;
    sockets: SocketsSchema;
    constructor(
        meta: RunnerMetaData | undefined,
        filter: JobFilter | undefined,
        template: string | undefined,
        sockets: SocketsSchema | undefined
    ) {
        this.meta = {
            kind: 5003,
            name: "An event template",
            description: "",
            tos: "",
            privacy: "",
            author: "",
            web: "",
            picture: "",
            tags: [],
        };
        this.template = ``;
        this.filter = {};
        this.sockets = {
            in: {},
            out: {},
        };

        if (meta) {
            for (const key in meta) {
                this.meta[key] = meta[key];
            }
        }

        if (filter) {
            this.filter = filter;
        }

        if (template) {
            this.template = template;
        }
        
        if (sockets) {
            this.sockets = sockets;
        }
        
    }


    getMeta() {
        return this.meta;
    }

    getFilter() {
        return this.filter;
    }

    getTemplate() {
        return this.template;
    }

    getSockets() {
        return this.sockets;
    }
}