export type NodeMetaData = {
    name?: string;
    description?: string;
    version?: string;
    picture?: string;
};

export default class NodeConfig {
    meta:NodeMetaData;
    constructor(meta:NodeMetaData|undefined){
        this.meta = { 
            name: "OpenAgents Node",
            description: "A new OpenAgents Node",
            version: "0.0.1"
        }
        if(meta){
            for(const key in meta){
                this.meta[key] = meta[key];
            }
        }
    }

    getMeta(){
        this.meta.name=process.env.NODE_NAME||this.meta.name;
        this.meta.description=process.env.NODE_DESCRIPTION||this.meta.description;
        this.meta.version=process.env.NODE_VERSION||this.meta.version;
        return this.meta;
    }

}