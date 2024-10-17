import amqplib, { Channel, Connection } from 'amqplib';
import { Service, ServiceContainer } from 'ck-ms-di';
import { v4 as uuidv4 } from 'uuid';

export type ExchangeType = 'direct' | 'topic' | 'fanout';

export interface ChannelOptions {
    exchange?: string;
    type?: ExchangeType;
    queue?: string;
    route?: string;
    durable?: boolean;
    prefetch?: number;
}

export interface ConsumerOptions {
    channelOptions: ChannelOptions;
    copyCount?: number;
}

interface Consumer {
    channel: Channel;
    options: ConsumerOptions;
    handler: (payload: Payload) => Promise<void>;
}

export interface Payload {
    message: any;
    sessionId: string;
    ack: () => void;
    nack: () => void;
    reject: () => void;
}

export interface QueueContainerOptions {
    RABBITMQ_DEFAULT_CHANNEL_NAME?: string;
    RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME?: string;
    RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE?: ExchangeType;
    RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_DURABLE?: boolean;
    RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME?: string;
    RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE?: boolean;
    RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE?: string;
    RABBITMQ_DEFAULT_PREFETCH?: number;
    RABBITMQ_DEFAULT_COPYCOUNT?: number;
    RABBITMQ_RECONNECT_DELAY?: number;
    RABBITMQ_PROTOCOL?: string;
    RABBITMQ_USERNAME?: string;
    RABBITMQ_PASSWORD?: string;
    RABBITMQ_HOST?: string;
    RABBITMQ_PORT?: string;
    RABBITMQ_HEARTBEAT?: number;
    RABBITMQ_VHOST?: string;
}

@Service({ lifecycle: 'singleton' })
export class QueueContainer {
    private connection: Connection | null = null;
    private consumers: Map<string, Consumer> = new Map();
    private isConnected = false;
    private isClosed = false;
    private maxReconnectAttempts = 10;
    private attempts = 0;
    private RABBITMQ_DEFAULT_CHANNEL_NAME: string = 'defaultChannel';
    private RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME: string = 'defaultExchange';
    private RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE: ExchangeType = 'direct';
    private RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_DURABLE: boolean = false;
    private RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME: string = 'defaultQueue';
    private RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE: boolean = false;
    private RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE: string = 'defaultRoute';
    private RABBITMQ_DEFAULT_PREFETCH: number = 1;
    private RABBITMQ_DEFAULT_COPYCOUNT: number = 1;
    private RABBITMQ_RECONNECT_DELAY: number = 1000;
    private RABBITMQ_PROTOCOL: string = 'amqp';
    private RABBITMQ_USERNAME: string = 'guest';
    private RABBITMQ_PASSWORD: string = 'guest';
    private RABBITMQ_HOST: string = 'localhost';
    private RABBITMQ_PORT: string = '5672';
    private RABBITMQ_HEARTBEAT: number = 60;
    private RABBITMQ_VHOST: string = '/';

    constructor(options?: QueueContainerOptions) {
        if (options) {
            this.applyOptions(options);
        }
    }

    public applyOptions(options: QueueContainerOptions) {
        this.RABBITMQ_DEFAULT_CHANNEL_NAME = options.RABBITMQ_DEFAULT_CHANNEL_NAME ?? process.env.RABBITMQ_DEFAULT_CHANNEL_NAME ?? this.RABBITMQ_DEFAULT_CHANNEL_NAME;
        this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME =
            options.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME ?? process.env.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME ?? this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME;
        this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE = (options.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE ??
            process.env.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE ??
            this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE) as ExchangeType;
        this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE =
            options.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE ?? process.env.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE === 'true' ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE;
        this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME = options.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME ?? process.env.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME;
        this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE = options.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE ?? process.env.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE;
        this.RABBITMQ_DEFAULT_PREFETCH = parseInt(options.RABBITMQ_DEFAULT_PREFETCH?.toString() ?? process.env.RABBITMQ_DEFAULT_PREFETCH ?? this.RABBITMQ_DEFAULT_PREFETCH.toString(), 10);
        this.RABBITMQ_DEFAULT_COPYCOUNT = parseInt(options.RABBITMQ_DEFAULT_COPYCOUNT?.toString() ?? process.env.RABBITMQ_DEFAULT_COPYCOUNT ?? this.RABBITMQ_DEFAULT_COPYCOUNT.toString(), 10);
        this.RABBITMQ_RECONNECT_DELAY = parseInt(options.RABBITMQ_RECONNECT_DELAY?.toString() ?? process.env.RABBITMQ_RECONNECT_DELAY ?? this.RABBITMQ_RECONNECT_DELAY.toString(), 10);
        this.RABBITMQ_PROTOCOL = options.RABBITMQ_PROTOCOL ?? process.env.RABBITMQ_PROTOCOL ?? this.RABBITMQ_PROTOCOL;
        this.RABBITMQ_USERNAME = options.RABBITMQ_USERNAME ?? process.env.RABBITMQ_USERNAME ?? this.RABBITMQ_USERNAME;
        this.RABBITMQ_PASSWORD = options.RABBITMQ_PASSWORD ?? process.env.RABBITMQ_PASSWORD ?? this.RABBITMQ_PASSWORD;
        this.RABBITMQ_HOST = options.RABBITMQ_HOST ?? process.env.RABBITMQ_HOST ?? this.RABBITMQ_HOST;
        this.RABBITMQ_PORT = options.RABBITMQ_PORT ?? process.env.RABBITMQ_PORT ?? this.RABBITMQ_PORT;
        this.RABBITMQ_HEARTBEAT = parseInt(options.RABBITMQ_HEARTBEAT?.toString() ?? process.env.RABBITMQ_HEARTBEAT ?? this.RABBITMQ_HEARTBEAT.toString(), 10);
        this.RABBITMQ_VHOST = options.RABBITMQ_VHOST ?? process.env.RABBITMQ_VHOST ?? '/';
    }

    public async init() {
        await this.connect();
    }

    private async createConnection() {
        this.isConnected = false;

        this.connection = await amqplib.connect({
            protocol: this.RABBITMQ_PROTOCOL,
            hostname: this.RABBITMQ_HOST,
            port: parseInt(this.RABBITMQ_PORT),
            username: this.RABBITMQ_USERNAME,
            password: this.RABBITMQ_PASSWORD,
            heartbeat: this.RABBITMQ_HEARTBEAT,
            vhost: this.RABBITMQ_VHOST ?? '/',
        });

        if (!this.connection) {
            throw new Error('Failed to create RabbitMQ connection');
        }

        this.isConnected = true;
        console.log('RabbitMQ connection established');

        this.connection.on('close', async () => {
            this.isConnected = false;
            if (this.isClosed) {
                return;
            }
            console.log('Connection lost unexpectedly. Trying to reconnect...');
            this.reconnect();
        });

        this.connection.on('error', async (err: any) => {
            this.isConnected = false;
            console.error('Connection error:', err);
            console.log('Connection lost unexpectedly. Trying to reconnect...');
            this.reconnect();
        });
    }

    async connect(): Promise<void> {
        this.isConnected = false;

        try {
            await this.createConnection();
        } catch (error: any) {
            console.error('Failed to connect to RabbitMQ:', error);
            await this.reconnect();
        }
    }

    private async reconnect(): Promise<void> {
        if (this.attempts >= this.maxReconnectAttempts) {
            console.error('Max reconnect attempts reached. Shutting down...');
            await this.shutdown();
            return;
        }

        this.attempts++;

        try {
            await new Promise((resolve) => setTimeout(resolve, this.RABBITMQ_RECONNECT_DELAY));

            try {
                const copyConsumers = new Map(this.consumers);

                await this.shutdown(true, true);
                await this.createConnection();

                copyConsumers.forEach(async (consumer) => {
                    const { options, handler } = consumer;

                    await this.addConsumer(options, handler);
                });
            } catch (error: any) {
                console.error('Reconnect attempt failed:', error);
                await this.reconnect();
            }
        } catch (error: any) {
            console.error('Error in reconnecting:', error);
            await this.reconnect();
        }
    }

    public async shutdown(deleteExchange: boolean = false, deleteQueue: boolean = false): Promise<void> {
        this.isConnected = false;
        this.isClosed = true;

        for (const consumer of this.consumers.values()) {
            const { exchange, queue } = consumer.options.channelOptions;

            if (deleteQueue && queue) {
                try {
                    await consumer.channel.deleteQueue(queue);
                    console.log('Queue deleted:', queue);
                } catch (error) {}
            }

            if (deleteExchange && exchange) {
                try {
                    await consumer.channel.deleteExchange(exchange);
                    console.log('Exchange deleted:', exchange);
                } catch (error) {}
            }

            try {
                await consumer.channel.close();
                console.log('Channel closed');
            } catch (error) {}
        }

        if (this.connection) {
            try {
                await this.connection.close();
                console.log('RabbitMQ connection closed');
            } catch (error) {}
        }

        this.connection = null;
        this.consumers.clear();
    }

    async addChannel(options: ChannelOptions): Promise<Channel> {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        const channel = await this.connection!.createConfirmChannel();
        await channel.assertExchange(options.exchange ?? this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME, options.type ?? this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE, {
            durable: options.durable ?? this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_DURABLE,
        });
        await channel.assertQueue(options.queue ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME, { durable: options.durable ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE });
        await channel.bindQueue(
            options.queue ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME,
            options.exchange ?? this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME,
            options.route ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE,
        );
        channel.prefetch(options.prefetch || this.RABBITMQ_DEFAULT_PREFETCH);

        console.log(`Channel added.`, options);

        return channel;
    }

    async addConsumerWithChannel(channel: Channel, consumer: Consumer) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        if (!channel) {
            throw new Error('Channel not found or failed to create.');
        }

        consumer.channel = channel;

        for (let i = 0; i < consumer.options.copyCount!; i++) {
            await this.consumeMessage(consumer, consumer.handler);
        }

        console.log(`Consumer added for channel.`);
    }

    async addConsumer(options: ConsumerOptions, handler: (payload: Payload) => Promise<void>) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        const channel = await this.addChannel(options.channelOptions);

        if (!channel) {
            throw new Error('Channel not found or failed to create.');
        }

        options.copyCount = options.copyCount || this.RABBITMQ_DEFAULT_COPYCOUNT;

        const consumer = { channel, options, handler } as Consumer;

        this.consumers.set(uuidv4(), consumer);

        for (let i = 0; i < options.copyCount; i++) {
            await this.consumeMessage(consumer, handler);
        }

        console.log(`Consumer added.`, options);
    }

    private async consumeMessage(consumer: Consumer, handler: (payload: Payload) => Promise<void>) {
        await consumer.channel.consume(
            consumer.options.channelOptions.queue ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME,
            async (msg: any | null) => {
                if (msg) {
                    let actionTaken = false;

                    const serviceContainer = ServiceContainer.getInstance();
                    const sessionId = serviceContainer.beginSession();

                    const payload: Payload = {
                        message: JSON.parse(msg.content.toString()),
                        sessionId,
                        ack: () => {
                            consumer.channel.ack(msg);
                            actionTaken = true;
                        },
                        nack: () => {
                            consumer.channel.nack(msg);
                            actionTaken = true;
                        },
                        reject: () => {
                            consumer.channel.reject(msg);
                            actionTaken = true;
                        },
                    };

                    try {
                        console.log('Message received:', payload);

                        await handler(payload);

                        serviceContainer.endSession(sessionId);

                        if (!actionTaken) {
                            payload.ack();
                        }
                    } catch (error) {
                        serviceContainer.endSession(sessionId);
                        console.error('Consumer handler error:', error);
                        if (!actionTaken) {
                            payload.nack();
                        }
                    }
                }
            },
            { noAck: false },
        );
    }

    public async publishMessage(channel: ChannelOptions, message: any, options: any): Promise<void> {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        try {
            const channelInstance = await this.connection!.createConfirmChannel();

            await new Promise<void>((resolve, reject) => {
                channelInstance.publish(
                    channel.exchange ?? this.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME,
                    channel.route ?? this.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE,
                    Buffer.from(JSON.stringify(message)),
                    options,
                    (err, ok) => {
                        if (err) {
                            console.error('Error while publishing message:', err);
                            return reject(err);
                        }
                        console.log('Message published successfully');
                        resolve();
                    },
                );
            });
        } catch (err: any) {
            console.error('Error while publishing message:', err);
            throw err;
        }
    }

    public get isConnectedToRabbitMQ() {
        return this.isConnected;
    }

    public get isClosedConnection() {
        return this.isClosed;
    }

    public get getAttempts() {
        return this.attempts;
    }

    public get getMaxReconnectAttempts() {
        return this.maxReconnectAttempts;
    }
}
