import amqplib, { Channel, Connection, ConsumeMessage } from 'amqplib';
import * as Environments from './Environments';

export type ExchangeType = 'direct' | 'topic' | 'fanout';

export interface ChannelOptions {
    name: string;
    exchange: string;
    type: ExchangeType;
    queue: string;
    route: string;
    durable: boolean;
}

export interface ConsumerOptions {
    channelName: string;
    consumerName: string;
    channelOptions?: ChannelOptions;
    prefetch?: number;
    copyCount?: number;
}

export interface Payload<T> {
    message: T;
    sessionId: string;
    ack: () => void;
    nack: () => void;
    reject: () => void;
}

export interface StoredChannelOptions {
    channel: Channel;
    options: ChannelOptions;
}

export interface StoredConsumerOptions<T> {
    options: ConsumerOptions;
    handler: (payload: Payload<T>) => Promise<void>;
}

export class QueueContainer {
    private connection: Connection | null = null;
    private channels: Map<string, StoredChannelOptions> = new Map();
    private channels_backup: Map<string, StoredChannelOptions> = new Map();
    private consumers: Map<string, StoredConsumerOptions<any>> = new Map();
    private consumers_backup: Map<string, StoredConsumerOptions<any>> = new Map();
    private isConnected = false;
    private reconnectAttempts = 0;
    private maxReconnectAttempts = 5;
    private readonly defaultChannelName = Environments.RABBITMQ_DEFAULT_CHANNEL_NAME;
    private uri = Environments.RABBITMQ_URI;

    async connect(uri?: string) {
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

            await this.addDefaultChannel();
        } catch (error) {
            console.error('RabbitMQ connection error:', error);
            this.reconnect();
        }
    }

    private async reconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            console.log('Reconnect attempt', this.reconnectAttempts);
            setTimeout(async () => {
                try {
                    this.channels_backup = new Map(this.channels);
                    this.consumers_backup = new Map(this.consumers);

                    await this.connect(this.uri);

                    this.channels_backup.forEach(async (channelOptions) => {
                        await this.addChannel(channelOptions.options);
                    });

                    this.consumers_backup.forEach(async (consumerOptions) => {
                        await this.addConsumer(consumerOptions.options, consumerOptions.handler);
                    });
                } catch (error) {
                    console.error('Reconnect attempt failed:', error);
                    this.reconnect();
                }
            }, 5000);
        } else {
            console.error('Max reconnect attempts reached. Shutting down.');
            this.shutdown();
        }
    }

    private shutdown() {
        this.isConnected = false;
        this.connection = null;
        this.channels.clear();
        this.consumers.clear();
    }

    private async addDefaultChannel() {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        if (this.channels.has(this.defaultChannelName)) {
            console.log('Default channel already exists');
            return;
        }

        const channel = await this.connection!.createChannel();
        await channel.assertExchange(Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME, Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE, {
            durable: Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_DURABLE,
        });
        await channel.assertQueue(Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME, { durable: Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE });
        await channel.bindQueue(Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME, Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME, Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE);

        this.channels.set(this.defaultChannelName, {
            channel,
            options: {
                name: this.defaultChannelName,
                exchange: Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_NAME,
                type: Environments.RABBITMQ_DEFAULT_CHANNEL_EXCHANGE_TYPE as ExchangeType,
                queue: Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_NAME,
                route: Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_ROUTE,
                durable: Environments.RABBITMQ_DEFAULT_CHANNEL_QUEUE_DURABLE,
            },
        });
        console.log(`Default channel "${this.defaultChannelName}" created.`);
    }

    async addChannel(options: ChannelOptions) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        if (this.channels.has(options.name)) {
            throw new Error(`Channel with name "${name}" already exists.`);
        }

        const channel = await this.connection!.createChannel();
        await channel.assertExchange(options.exchange, options.type, { durable: options.durable });
        await channel.assertQueue(options.queue, { durable: options.durable });
        await channel.bindQueue(options.queue, options.exchange, options.route);

        this.channels.set(options.name, { channel, options });
        console.log(`Channel "${name}" added and cached.`);
    }

    async closeChannel(name: string) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        const channelObj = this.channels.get(name);
        if (!channelObj) {
            throw new Error(`Channel with name "${name}" does not exist.`);
        }

        await this.removeConsumersByChannel(name);

        await channelObj.channel.close();
        this.channels.delete(name);
        console.log(`Channel "${name}" and its consumers have been removed.`);
    }

    private async removeConsumersByChannel(channelName: string) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        for (const [consumerName, storedConsumer] of this.consumers) {
            if (storedConsumer.options.channelName === channelName) {
                const channel = this.channels.get(channelName)?.channel;
                if (channel) {
                    await channel.cancel(consumerName);
                }

                this.consumers.delete(consumerName);
                console.log(`Consumer "${consumerName}" removed from channel "${channelName}".`);
            }
        }
    }

    async addConsumer<T>(options: ConsumerOptions, handler: (payload: Payload<T>) => Promise<void>) {
        if (!this.isConnected) {
            throw new Error('RabbitMQ connection is not established');
        }

        const channel = options.channelName ? this.channels.get(options.channelName)?.channel : await this.addChannel(options.channelOptions!);

        if (!channel) {
            throw new Error('Channel not found or failed to create.');
        }

        channel.prefetch(options.prefetch || Environments.RABBITMQ_DEFAULT_PREFETCH);

        const copyCount = options.copyCount || Environments.RABBITMQ_DEFAULT_COPYCOUNT;
        for (let i = 0; i < copyCount; i++) {
            await this.consumeMessage<T>(channel, options.consumerName, handler);
        }

        this.consumers.set(options.consumerName, { options, handler });
        console.log(`Consumer "${options.consumerName}" added.`);
    }

    private async consumeMessage<T>(channel: Channel, consumerName: string, handler: (payload: Payload<T>) => Promise<void>) {
        await channel.consume(
            consumerName,
            async (msg: ConsumeMessage | null) => {
                if (msg) {
                    let actionTaken = false;

                    const payload: Payload<T> = {
                        message: JSON.parse(msg.content.toString()),
                        sessionId: '',
                        ack: () => {
                            channel.ack(msg);
                            actionTaken = true;
                        },
                        nack: () => {
                            channel.nack(msg);
                            actionTaken = true;
                        },
                        reject: () => {
                            channel.reject(msg);
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
}
