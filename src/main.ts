import {NodeConfig,RunnerConfig,OpenAgentsNode,JobContext,JobRunner} from "openagents-node-js-sdk";
import OpenAI from "openai";
import Mustache from "mustache";


class ToolSelector extends JobRunner {
    private discoveredActions = {
        actions: [] as Array<{ template: string; meta: any; sockets: any }>,
        timestamp: 0,
        tools: [],
    };

    private openai: OpenAI;
    

    constructor() {
        super(
            new RunnerConfig(
                {
                    kind: 5003,
                    name: "Tool Selector",
                    description: "Select tools to run based on the user input",
                    tos: "https://openagents.com/terms",
                    privacy: "https://openagents.com/privacy",
                    author: "OpenAgentsInc",
                    web: "https://github.com/OpenAgentsInc/openagents-tool-selector",
                    picture: "",
                    tags: ["tool-selector"],
                },
                {
                    filterByRunOn: "openagents/tool-selector",
                },
                `{
                    "kind": {{meta.kind}},
                    "created_at": {{sys.timestamp_seconds}},
                    "tags": [
                        ["param","run-on", "openagents/tool-selector"],
                        ["output", "{{in.outputType}}"],
                        {{#in.queries}}
                        ["i", "{{value}}", "text", "",  "query"],
                        {{/in.queries}}
                        ["i", "{{in.context}}", "text", "",  "context"],
                        ["expiration", "{{sys.expiration_timestamp_seconds}}"],
                    ],
                    "content":""
                }
                `,
                {
                    in: {
                        queries: {
                            title: "Queries",
                            description: "The queries",
                            type: "array",
                            items: {
                                type: "map",
                                properties: {
                                    value: {
                                        title: "Value",
                                        description: "The query value",
                                        type: "string",
                                    },
                                },
                            },
                        },
                        context: {
                            title: "Context",
                            description: "The context",
                            type: "string",
                            default: "",
                        },
                        outputType: {
                            title: "Output Type",
                            description: "The Desired Output Type",
                            type: "string",
                            default: "application/json",
                        },
                    },
                    out: {
                        output: {
                            title: "Output",
                            description: "A toolchain output",
                            type: "string",
                        },
                    },
                }
            )
        );
        this.openai = new OpenAI();
        this.setRunInParallel(true);
    }

    private buildTools(actions: Array<{ template: string; meta: any; sockets: any }>) {
        const tools = [];
        let i = 0;
        for (const action of actions) {
            const tool: any = {};
            tool.type = "function";
            tool.function = {};
            tool.function.description = action.meta.description;
            tool.function.name = action.meta.id;
            tool.function.parameters = {
                type: "object",
                properties: {},
                required: [],
            };
            for (const key in action.sockets.in) {
                const socket = action.sockets.in[key];
                const required=socket.required;
                delete socket.required ;

                tool.function.parameters.properties[key] = socket;
                if (typeof socket.default == "undefined") tool.function.parameters.required.push(key);
            }
            tools.push(tool);
        }
        return tools;
    }

    private async callAction(
        ctx: JobContext,
        actions: Array<{ template: string; meta: any; sockets: any }>,
        actionId,
        args: string
    ) {
        const logger=ctx.getLogger();
        logger.finest("Calling action", actionId);
        const action = actions.find((a) => a.meta.id == actionId);
        if (!action) throw new Error("Action not found");
        const template = action.template;
        const sockets = JSON.parse(JSON.stringify(action.sockets));
        const inSockets = JSON.parse(args);

        const params = {
            sys: {
                timestamp_seconds: Math.floor(Date.now() / 1000),
                expiration_timestamp_seconds: Math.floor((Date.now() + 1000 * 60 * 10) / 1000),
            },
            in: {},
            out: {},
            meta: action.meta,
        };

        for (const k in inSockets) {
            params.in[k] = inSockets[k];
        }

        for (const k in sockets.in) {
            if (!params.in[k]) {
                if (sockets.in[k].default) params.in[k] = sockets.in[k].default;
            }
        }

        for (const k in sockets.out) {
            if (sockets.out[k].default) params.out[k] = sockets.out[k].default;
        }

        logger.finer("Action params", params);
        logger.finer("Action template", template);

        Mustache.escape = function (text) {
            return text.replace(/"/g, '\\"');
        };
        
        const event = Mustache.render(template, params);
        logger.finer("Final event", event);

        const jobOut = await ctx.waitForContent(ctx.sendSubJobRequest(event));
        logger.finer("Action output", jobOut);
        return jobOut;        
    }

    private async callTools(
        ctx: JobContext,
        actions: Array<{ template: string; meta: any; sockets: any }>,
        tools: any,
        history: Array<OpenAI.ChatCompletionMessageParam>,
        newContext = [],
        n = 0
    ) {
        const logger = ctx.getLogger();
        logger.finest("Calling chat completion with history", history);
        const chatCompletion = await this.openai.chat.completions.create({
            model: "gpt-3.5-turbo",
            messages: history,
            temperature: 0,
            tools: tools,
            tool_choice: "auto",
            stream: false,
        });

        const message = chatCompletion.choices[0]?.message;
        if (message) {
            if (message.tool_calls && message.tool_calls.length > 0) {
                logger.finest("Got tool calls", message.tool_calls)
                history.push(message);
                newContext.push(message);
                for (const tool_call of message.tool_calls) {
                    if (tool_call.type != "function") continue;
                    const tool_call_id = tool_call.id;
                    const args = tool_call.function.arguments;
                    const tool_name = tool_call.function.name;

                    logger.finest("Calling tool", tool_call_id, tool_name, args);
                    const toolOut = await this.callAction(ctx,actions, tool_name, args);

                    const toolAnswer: OpenAI.ChatCompletionMessageParam = {
                        role: "tool",
                        tool_call_id: tool_call_id,
                        content: toolOut,
                    };

                    logger.finest("Tool answer", toolAnswer);
                    
                    history.push(toolAnswer);
                    newContext.push(toolOut);
                }
                if (n < 10) {
                    await this.callTools(ctx,actions, tools, history, newContext, n + 1);
                }
            }
        }
        return newContext;
    }

    public async run(ctx: JobContext): Promise<string> {
        const logger = ctx.getLogger();
        if (Date.now() - this.discoveredActions.timestamp > 1000 * 60 * 60 * 10) {
            const actions = await ctx.discoverNearbyActions({
                tags: ["tool"],
                kindRanges: [{ min: 5000, max: 5999 }],
            });
            if(actions.length==0){
                logger.warn("No actions found"); 
                return "";
            } 
            this.discoveredActions.actions= actions;
            this.discoveredActions.timestamp = Date.now();
            logger.finer("Discovered actions", this.discoveredActions.actions);
            const tools = this.buildTools(this.discoveredActions.actions);
            logger.finer("Available tools", JSON.stringify(tools, null, 2));
            this.discoveredActions.tools = tools;
        }

        const context = ctx.getJobInput("context")||"";

        let results=[];
        for(const query of ctx.getJobInputs("query")){
            results.push(...
                (await this.callTools(ctx, this.discoveredActions.actions, this.discoveredActions.tools, [
                    {
                        role: "system",
                        content: context ? `Answer to user using the following context:\n ${context}` : "Answer to user",
                    },
                    {
                        role: "user",
                        content: query.data,
                    },
                ]))
            );
        }
        return JSON.stringify(results);
    }
}



const node=new OpenAgentsNode(new NodeConfig({
    name: "OpenAgents Tool Selector",
    description: "A new OpenAgents Tool Selector",
    version: "0.0.1"
}));
node.registerRunner(new ToolSelector());
node.start();



