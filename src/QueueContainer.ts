import amqplib, { Channel, Connection, ConsumeMessage } from 'amqplib';
import * as Environments from './Environments';
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

export interface Consumer {
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

export class QueueContainer {
    private connection: Connection | null = null;
    private consumers: Map<string, Consumer> = new Map();
    private isConnected = false;
    private reconnectAttempts = 0;
    private maxReconnectAttempts = 5;
    private uri = Environments.RABBITMQ_URI;

    async connect(uri?: string): Promise<void> {
        if (uri) {
            this.uri = uri;
        }

        this.isConnected = false;

        try {
            this.connection = await amqplib.connect(this.uri);
            this.isConnected = true;
            this.reconnectAttempts = 0;
            console.log('RabbitMQ connection established');

            this.connection.on('close', async () => {
                this.isConnected = false;
                console.log('Connection lost unexpectedly. Trying to reconnect...');
                this.reconnect();
            });

            this.connection.on('error', async (err: any) => {
                this.isConnected = false;
                console.error('Connection error:', err);
                console.log('Connection lost unexpectedly. Trying to reconnect...');
                this.reconnect();
            });
        } catch (error) {
            console.error('RabbitMQ connection error:', error);
            this.reconnect();
        }
    }

    private async reconnect(): Promise<void> {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            console.log('Reconnect attempt', this.reconnectAttempts);
            setTimeout(async () => {
                try {
                    await this.connect(this.uri);

                    const copyConsumers = new Map(this.consumers);
                    this.consumers.clear();

                    copyConsumers.forEach(async (consumer) => {
                        await consumer.channel.close();

                        const channel = await this.addChannel(consumer.options.channelOptions);

                        for (let i = 0; i < (consumer.options.copyCount ?? Environments.RABBITMQ_DEFAULT_COPYCOUNT); i++) {
                            await this.addConsumerWithChannel(channel, consumer);
                        }
                    });
                } catch (error) {
                    console.error('Reconnect attempt failed:', error);
                    this.reconnect();
                }
            }, Environments.RABBITMQ_RECONNECT_DELAY);
        } else {
            console.error('Max reconnect attempts reached. Shutting down.');
            this.shutdown();
        }
    }

    public async shutdown(): Promise<void> {
        this.isConnected = false;

        await this.connection?.close();

        this.consumers.forEach(async (consumer) => {
            await consumer.channel.close();
        });

        this.connection = null;
        this.consumers.clear();
    }

    async addChannel(options: ChannelOptions): Promise<Channel> {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        const channel = await this.connection!.createChannel();
        await channel.assertExchange(options.exchange ?? Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME, options.type ?? Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE, {
            durable: options.durable ?? Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_DURABLE,
        });
        await channel.assertQueue(options.queue ?? Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME, { durable: options.durable ?? Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE });
        await channel.bindQueue(
            options.queue ?? Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME,
            options.exchange ?? Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME,
            options.route ?? Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE,
        );
        channel.prefetch(options.prefetch || Environments.RABBITMQ_DEFAULT_PREFETCH);

        console.log(`Channel added.`, options);

        return channel;
    }

    async closeChannel(channel: Channel): Promise<void> {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        await channel.close();

        console.log('Channel closed', channel);
    }

    async addConsumerWithChannel(channel: Channel, consumer: Consumer) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        if (!channel) {
            throw new Error('Channel not found or failed to create.');
        }

        for (let i = 0; i < consumer.options.copyCount!; i++) {
            await this.consumeMessage(consumer, consumer.handler);
        }

        console.log(`Consumer added for channel.`, channel, consumer);
    }

    async addConsumer(options: ConsumerOptions, handler: (payload: Payload) => Promise<void>) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        const channel = await this.addChannel(options.channelOptions);

        if (!channel) {
            throw new Error('Channel not found or failed to create.');
        }

        options.copyCount = options.copyCount || Environments.RABBITMQ_DEFAULT_COPYCOUNT;

        const consumer = { channel, options, handler } as Consumer;

        this.consumers.set(uuidv4(), consumer);

        for (let i = 0; i < options.copyCount; i++) {
            await this.consumeMessage(consumer, handler);
        }

        console.log(`Consumer added.`, options);
    }

    private async consumeMessage(consumer: Consumer, handler: (payload: Payload) => Promise<void>) {
        await consumer.channel.consume(
            consumer.options.channelOptions.queue ?? Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME,
            async (msg: any | null) => {
                if (msg) {
                    let actionTaken = false;

                    const payload: Payload = {
                        message: JSON.parse(msg.content.toString()),
                        sessionId: '',
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
                        await handler(payload);

                        if (!actionTaken) {
                            payload.ack();
                        }
                    } catch (error) {
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
            const channelInstance = await this.addChannel(channel);

            channelInstance.publish(
                channel.exchange ?? Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME,
                channel.route ?? Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE,
                Buffer.from(JSON.stringify(message)),
                options,
            );

            console.log('Message published', message);
        } catch (err: any) {
            console.error('Error while publishing message:', err);
            throw err;
        }
    }

    public get isConnectedToRabbitMQ() {
        return this.isConnected;
    }
}
