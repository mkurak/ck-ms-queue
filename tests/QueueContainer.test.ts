import { ChannelOptions, ConsumerOptions, Payload, QueueContainer } from './../src/QueueContainer';
describe('QueueContainer', () => {
    const container = new QueueContainer();

    beforeAll(async () => {
        await container.connect();
    });

    afterAll(() => {
        container.shutdown();
    });

    test('Bağlantı kurulabilmeli', () => {
        expect(container.isConnectedToRabbitMQ).toBeTruthy();
    });

    test('Kanal eklenebilmeli', async () => {
        const channelOptions = {
            name: 'vSdqzzu7bgF9',
            options: {
                durable: false,
            },
        } as ChannelOptions;

        await container.addChannel(channelOptions);

        const channel = container.getChannel(channelOptions.name);
        expect(channel).not.toBeNull();
    });

    test('Kanal silinebilmeli', async () => {
        const channelOptions = {
            name: 'vGR5UXJJ2yvM',
            options: {
                durable: false,
            },
        } as ChannelOptions;

        await container.addChannel(channelOptions);

        let addedChannel = container.getChannel(channelOptions.name);
        expect(addedChannel).not.toBeNull();

        await container.closeChannel(channelOptions.name);
        const channel = container.getChannel(channelOptions.name);
        expect(channel).toBeUndefined();
    });

    test('Aynı isimde 2 kanal eklenememeli', async () => {
        const channelOptions = {
            name: 'vGR5UXJJ2yvM',
            options: {
                durable: false,
            },
        } as ChannelOptions;

        await container.addChannel(channelOptions);

        let addedChannel = container.getChannel(channelOptions.name);
        expect(addedChannel).not.toBeNull();

        await expect(container.addChannel(channelOptions)).rejects.toThrow();
    });

    test('Mevcut bir kanala consumer eklenebilmeli', async () => {
        const channelOptions = {
            name: 'aX8Hd9GBr4hY',
            options: {
                durable: false,
            },
        } as ChannelOptions;

        await container.addChannel(channelOptions);

        let addedChannel = container.getChannel(channelOptions.name);
        expect(addedChannel).not.toBeNull();

        const consumerOptions = {
            channelName: channelOptions.name,
            consumerName: 'UqkqU8QfhLfB',
        } as ConsumerOptions;

        container.addConsumer(consumerOptions, async (payload: Payload<any>) => {
            console.log(payload);
        });
    });

    test("Mevcut bir kanala consumer eklenmeli ve eklenen consumer'ın copyCount değeri 10 olduğunda, 10 consumer eklenmiş olmalı.", async () => {
        const channelOptions = {
            name: 'khr8UEM95gHW',
            options: {
                durable: false,
            },
        } as ChannelOptions;

        await container.addChannel(channelOptions);

        let addedChannel = container.getChannel(channelOptions.name);
        expect(addedChannel).not.toBeNull();

        const consumerOptions = {
            channelName: channelOptions.name,
            consumerName: 'bumqgwNMk5bQ',
            copyCount: 10,
        } as ConsumerOptions;

        container.addConsumer(consumerOptions, async (payload: Payload<any>) => {
            console.log(payload);
        });

        expect(container.getConsumers(consumerOptions.consumerName).length).toBe(10);
    });
});
