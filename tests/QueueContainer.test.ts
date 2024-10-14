import { ChannelOptions, Consumer, ConsumerOptions, Payload, QueueContainer, QueueContainerOptions } from '../src/QueueContainer';
import axios from 'axios';

const createRandomString = (length: number = 12): string => {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
};

async function setupVhost(vhostName: string) {
    const rabbitApiUrl = 'http://localhost:15672/api/vhosts';
    const username = 'guest';
    const password = 'guest';

    try {
        // Check if vhost exists
        const vhostCheckResponse = await axios.get(`${rabbitApiUrl}/${vhostName}`, {
            auth: {
                username,
                password,
            },
        });

        if (vhostCheckResponse.status === 200) {
            console.log(`Vhost "${vhostName}" already exists. Deleting it...`);

            // If vhost exists, delete it
            const deleteVhostResponse = await axios.delete(`${rabbitApiUrl}/${vhostName}`, {
                auth: {
                    username,
                    password,
                },
            });

            if (deleteVhostResponse.status === 204) {
                console.log(`Vhost "${vhostName}" deleted successfully.`);
            } else {
                console.error(`Failed to delete vhost "${vhostName}", status code:`, deleteVhostResponse.status);
                return;
            }
        }
    } catch (error: any) {
        if (error.response?.status !== 404) {
            console.error('Error checking for vhost:', error);
            throw error;
        } else {
            console.log(`Vhost "${vhostName}" not found, creating a new one...`);
        }
    }

    // Create the vhost
    try {
        const createVhostResponse = await axios.put(
            `${rabbitApiUrl}/${vhostName}`,
            {},
            {
                auth: {
                    username,
                    password,
                },
            },
        );

        if (createVhostResponse.status === 201) {
            console.log(`Vhost "${vhostName}" successfully created.`);
        } else {
            console.error(`Failed to create vhost "${vhostName}", status code:`, createVhostResponse.status);
            return;
        }
    } catch (error: any) {
        console.error('Error creating vhost:', error);
        throw error;
    }

    // Set full permissions for the guest user
    try {
        const permissionUrl = `http://localhost:15672/api/permissions/${vhostName}/${username}`;
        const setPermissionResponse = await axios.put(
            permissionUrl,
            {
                configure: '.*',
                write: '.*',
                read: '.*',
            },
            {
                auth: {
                    username,
                    password,
                },
            },
        );

        if (setPermissionResponse.status === 204) {
            console.log(`Full access permissions granted to user "${username}" for vhost "${vhostName}".`);
        } else {
            console.error(`Failed to grant permissions to user "${username}" for vhost "${vhostName}", status code:`, setPermissionResponse.status);
        }
    } catch (error: any) {
        console.error(`Error setting permissions for user:`, error);
        throw error;
    }
}

describe('QueueContainer', () => {
    beforeAll(async () => {
        await setupVhost('test');
    });

    it('should establish and close a connection to RabbitMQ', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        expect(queueContainer.isConnectedToRabbitMQ).toBe(true);

        await queueContainer.shutdown(true, true);

        expect(queueContainer.isClosedConnection).toBe(true);
    });

    it('should publish and consume a message from RabbitMQ', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        const channelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: createRandomString(),
            durable: false,
        } as ChannelOptions;

        const message = { text: 'Hello, RabbitMQ!' };

        let consumedMessage: any = null;
        const consumerHandler = async (payload: Payload) => {
            consumedMessage = payload.message;
            payload.ack();
        };

        const consumerOptions: ConsumerOptions = {
            channelOptions,
            copyCount: 1,
        };

        await queueContainer.addConsumer(consumerOptions, consumerHandler);

        await queueContainer.publishMessage(channelOptions, message, { persistent: false });

        await new Promise((resolve) => setTimeout(resolve, 1000));
        expect(consumedMessage).toEqual(message);

        await queueContainer.shutdown(true, true);
    });

    it('should route messages to the correct queues based on the routing key', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        // Channel options for the two queues
        const channelOptions1: ChannelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: 'route1',
            durable: false,
        };

        const channelOptions2: ChannelOptions = {
            exchange: channelOptions1.exchange, // Same exchange but different route
            type: 'direct',
            queue: createRandomString(),
            route: 'route2',
            durable: false,
        };

        const message1 = { text: 'Message for route1' };
        const message2 = { text: 'Message for route2' };

        let consumedMessage1: any = null;
        let consumedMessage2: any = null;

        // Consumer for route1
        const consumerHandler1 = async (payload: Payload) => {
            consumedMessage1 = payload.message;
            payload.ack();
        };

        // Consumer for route2
        const consumerHandler2 = async (payload: Payload) => {
            consumedMessage2 = payload.message;
            payload.ack();
        };

        // Add consumers to different queues with different routes
        const consumerOptions1: ConsumerOptions = {
            channelOptions: channelOptions1,
            copyCount: 1,
        };

        const consumerOptions2: ConsumerOptions = {
            channelOptions: channelOptions2,
            copyCount: 1,
        };

        // Add consumers
        await queueContainer.addConsumer(consumerOptions1, consumerHandler1);
        await queueContainer.addConsumer(consumerOptions2, consumerHandler2);

        // Publish messages with different routes
        await queueContainer.publishMessage(channelOptions1, message1, { persistent: false });
        await queueContainer.publishMessage(channelOptions2, message2, { persistent: false });

        // Wait for the consumers to process the messages
        await new Promise((resolve) => setTimeout(resolve, 1000));

        // Check that the messages were routed correctly
        expect(consumedMessage1).toEqual(message1);
        expect(consumedMessage2).toEqual(message2);

        // Cleanup: Shutdown and delete queues/exchange
        await queueContainer.shutdown(true, true);
    });

    it('should reconnect to RabbitMQ when the connection is lost', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        // Channel and queue options
        const channelOptions: ChannelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: createRandomString(),
            durable: false,
        };

        const message = { text: 'Hello after reconnect!' };
        let consumedMessage: any = null;

        // Consumer handler
        const consumerHandler = async (payload: Payload) => {
            consumedMessage = payload.message;
            payload.ack();
        };

        // Add consumer
        const consumerOptions: ConsumerOptions = {
            channelOptions,
            copyCount: 1,
        };

        await queueContainer.addConsumer(consumerOptions, consumerHandler);

        // Simulate connection loss (manually close the connection)
        await queueContainer['connection']?.close(); // Close the connection

        // Check if the connection is lost
        expect(queueContainer.isConnectedToRabbitMQ).toBe(false);

        // Wait for reconnection to happen (it may take some time)
        await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait for 2 seconds

        // Check if the connection is re-established
        expect(queueContainer.isConnectedToRabbitMQ).toBe(true);

        // Publish a message after reconnection
        await queueContainer.publishMessage(channelOptions, message, { persistent: false });

        // Check if the message was consumed
        await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait for the message to be consumed
        expect(consumedMessage).toEqual(message);

        // Clean up: Shutdown and delete queues/exchange
        await queueContainer.shutdown(true, true);
    });

    it('should respect the prefetch limit', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
            RABBITMQ_DEFAULT_PREFETCH: 1, // Set prefetch to 1
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        // Channel options
        const channelOptions: ChannelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: createRandomString(),
            durable: false,
        };

        const messages = [{ text: 'Message 1' }, { text: 'Message 2' }, { text: 'Message 3' }];

        let consumedMessages: any[] = [];

        // Consumer handler
        const consumerHandler = async (payload: Payload) => {
            consumedMessages.push(payload.message);
            payload.ack();
            // Simulate processing time
            await new Promise((resolve) => setTimeout(resolve, 1000));
        };

        const consumerOptions: ConsumerOptions = {
            channelOptions,
            copyCount: 1,
        };

        // Add consumer
        await queueContainer.addConsumer(consumerOptions, consumerHandler);

        // Publish messages
        for (const message of messages) {
            await queueContainer.publishMessage(channelOptions, message, { persistent: false });
        }

        // Wait for a while to allow messages to be processed
        await new Promise((resolve) => setTimeout(resolve, 4000));

        // Since prefetch is set to 1, messages should be processed one by one.
        expect(consumedMessages.length).toBe(3);
        expect(consumedMessages).toEqual(messages);

        // Cleanup: Shutdown and delete queues/exchange
        await queueContainer.shutdown(true, true);
    });

    it('should persist messages when persistent flag is set', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        // Channel options
        const channelOptions: ChannelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: createRandomString(),
            durable: true, // Mark the queue as durable
        };

        const message = { text: 'Persistent Message' };
        let consumedMessage: any = null;

        // Consumer handler
        const consumerHandler = async (payload: Payload) => {
            consumedMessage = payload.message;
            payload.ack();
        };

        const consumerOptions: ConsumerOptions = {
            channelOptions,
            copyCount: 1,
        };

        // Add consumer
        await queueContainer.addConsumer(consumerOptions, consumerHandler);

        // Publish a persistent message
        await queueContainer.publishMessage(channelOptions, message, { persistent: true });

        // Simulate shutting down RabbitMQ connection after publishing but before consuming the message
        await queueContainer.shutdown(false, false); // Shutdown without deleting exchange or queue

        // Reconnect and consume the message
        await queueContainer.connect();
        await queueContainer.addConsumer(consumerOptions, consumerHandler);

        // Wait for the message to be consumed after reconnection
        await new Promise((resolve) => setTimeout(resolve, 1000));

        // Check if the persistent message was successfully consumed
        expect(consumedMessage).toEqual(message);

        // Cleanup: Shutdown and delete queues/exchange
        await queueContainer.shutdown(true, true);
    });

    it('should handle consumer error and requeue the message with nack()', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        const channelOptions: ChannelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: createRandomString(),
            durable: false,
        };

        const message = { text: 'Error message' };

        let attemptCount = 0;
        const consumerHandler = async (payload: Payload) => {
            attemptCount++;
            if (attemptCount < 2) {
                throw new Error('Simulated processing error');
            }
            payload.ack();
        };

        const consumerOptions: ConsumerOptions = {
            channelOptions,
            copyCount: 1,
        };

        await queueContainer.addConsumer(consumerOptions, consumerHandler);

        await queueContainer.publishMessage(channelOptions, message, { persistent: false });

        await new Promise((resolve) => setTimeout(resolve, 2000));

        // Check that the message was requeued and processed again
        expect(attemptCount).toBe(2);

        // Cleanup: Shutdown and delete queues/exchange
        await queueContainer.shutdown(true, true);
    });

    it('should distribute messages between multiple consumers', async () => {
        const options: QueueContainerOptions = {
            RABBITMQ_HOST: 'localhost',
            RABBITMQ_PORT: '5672',
            RABBITMQ_USERNAME: 'guest',
            RABBITMQ_PASSWORD: 'guest',
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(options);

        await queueContainer.connect();

        const channelOptions: ChannelOptions = {
            exchange: createRandomString(),
            type: 'direct',
            queue: createRandomString(),
            route: createRandomString(),
            durable: false,
        };

        const messages = [{ text: 'Message 1' }, { text: 'Message 2' }, { text: 'Message 3' }, { text: 'Message 4' }];

        const consumedMessagesConsumer1: any[] = [];
        const consumedMessagesConsumer2: any[] = [];

        // Consumer handler for consumer 1
        const consumerHandler1 = async (payload: Payload) => {
            consumedMessagesConsumer1.push(payload.message);
            payload.ack();
        };

        // Consumer handler for consumer 2
        const consumerHandler2 = async (payload: Payload) => {
            consumedMessagesConsumer2.push(payload.message);
            payload.ack();
        };

        const consumerOptions: ConsumerOptions = {
            channelOptions,
            copyCount: 1,
        };

        // Add two consumers to the same queue
        await queueContainer.addConsumer(consumerOptions, consumerHandler1);
        await queueContainer.addConsumer(consumerOptions, consumerHandler2);

        // Publish messages
        for (const message of messages) {
            await queueContainer.publishMessage(channelOptions, message, { persistent: false });
        }

        // Wait for the consumers to process the messages
        await new Promise((resolve) => setTimeout(resolve, 2000));

        // Check that all messages are consumed and distributed between both consumers
        const totalConsumedMessages = consumedMessagesConsumer1.length + consumedMessagesConsumer2.length;
        expect(totalConsumedMessages).toBe(messages.length);
        expect(consumedMessagesConsumer1.length).toBeGreaterThan(0);
        expect(consumedMessagesConsumer2.length).toBeGreaterThan(0);

        // Cleanup: Shutdown and delete queues/exchange
        await queueContainer.shutdown(true, true);
    });

    it('should throw an error with invalid RabbitMQ configuration', async () => {
        const invalidOptions: QueueContainerOptions = {
            RABBITMQ_HOST: 'invalid-host', // Invalid host
            RABBITMQ_PORT: '9999', // Invalid port
            RABBITMQ_USERNAME: 'wrong-user', // Invalid username
            RABBITMQ_PASSWORD: 'wrong-password', // Invalid password
            RABBITMQ_VHOST: 'test',
        };

        const queueContainer = new QueueContainer(invalidOptions);

        await queueContainer.connect();

        await new Promise((resolve) => setTimeout(resolve, 2000));

        expect(queueContainer.isConnectedToRabbitMQ).toBe(false);
        expect(queueContainer.isClosedConnection).toBe(true);
        expect(queueContainer.connectionErrorStatus).toBe('ENOTFOUND');
    });
});
