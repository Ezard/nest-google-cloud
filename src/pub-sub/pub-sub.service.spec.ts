import {Message, PubSub, Subscription, Topic} from '@google-cloud/pubsub';
import {PubSubService} from './pub-sub.service';

describe('PubSubService', () => {
    let pubSubService: PubSubService;

    beforeEach(() => {
        pubSubService = new PubSubService()
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    describe('createTopic', () => {
        it('should return a topic with the given name', async () => {
            const topicName = 'topic-123';
            const createTopicMock = jest.fn()
                .mockReturnValue(Promise.resolve([{name: `projects/project-123/topics/${topicName}`} as Topic]));
            jest
                .spyOn(PubSub.prototype, 'createTopic')
                .mockImplementation(createTopicMock);

            const topic = await pubSubService.createTopic(topicName);

            expect(topic.name.endsWith(topicName)).toEqual(true);
        });
    });

    describe('getTopic', () => {
        it('should return a topic with the provided name when found', async () => {
            const topicName = 'topic-123';
            const getTopicsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[{name: `projects/project-123/topics/${topicName}`} as Topic]]));
            jest
                .spyOn(PubSub.prototype, 'getTopics')
                .mockImplementation(getTopicsMock);

            const topic = await pubSubService.getTopic(topicName);

            expect(topic).toBeDefined();
        });

        it('should return undefined when a topic with the provided name is not found', async () => {
            const topicName = 'topic-123';
            const getTopicsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[]]));
            jest
                .spyOn(PubSub.prototype, 'getTopics')
                .mockImplementation(getTopicsMock);

            const topic = await pubSubService.getTopic(topicName);

            expect(topic).toBeUndefined();
        });
    });

    describe('getOrCreateTopic', () => {
        it('should return an existing topic if one with the provided name is found', async () => {
            const existingTopicName = 'topic-123';
            const existingTopicFullName = `projects/project-123/topics/${existingTopicName}`;
            const newTopicName = 'topic-123';
            const getTopicsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[{name: existingTopicFullName} as Topic]]));
            const createTopicMock = jest.fn()
                .mockReturnValue(Promise.resolve([{name: `projects/project-123/topics/${newTopicName}`} as Topic]));
            jest
                .spyOn(PubSub.prototype, 'getTopics')
                .mockImplementation(getTopicsMock);
            jest
                .spyOn(PubSub.prototype, 'createTopic')
                .mockImplementation(createTopicMock);

            const topic = await pubSubService.getOrCreateTopic(existingTopicName);

            expect(topic.name).toEqual(existingTopicFullName);
            expect(getTopicsMock).toHaveBeenCalledTimes(1);
            expect(createTopicMock).toHaveBeenCalledTimes(0);
        });

        it('should create a new topic if one with the provided name is not found', async () => {
            const existingTopicName = 'topic-123';
            const newTopicName = 'topic-123';
            const newTopicFullName = `projects/project-123/topics/${newTopicName}`;
            const getTopicsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[]]));
            const createTopicMock = jest.fn()
                .mockReturnValue(Promise.resolve([{name: `projects/project-123/topics/${newTopicName}`} as Topic]));
            jest
                .spyOn(PubSub.prototype, 'getTopics')
                .mockImplementation(getTopicsMock);
            jest
                .spyOn(PubSub.prototype, 'createTopic')
                .mockImplementation(createTopicMock);

            const topic = await pubSubService.getOrCreateTopic(existingTopicName);

            expect(topic.name).toEqual(newTopicFullName);
            expect(getTopicsMock).toHaveBeenCalledTimes(1);
            expect(createTopicMock).toHaveBeenCalledTimes(1);
        });
    });

    describe('createSubscription', () => {
        it('should return a subscription with the given name', async () => {
            const subscriptionName = 'subscription-123';
            const topic = {
                createSubscription(_: string) {
                }
            } as Topic;
            const createSubscriptionMock = jest.fn()
                .mockReturnValue(Promise.resolve([{name: `projects/project-123/subscriptions/${subscriptionName}`} as Topic]));
            jest
                .spyOn(topic, 'createSubscription')
                .mockImplementation(createSubscriptionMock);

            const subscription = await pubSubService.createSubscription(topic, subscriptionName);

            expect(subscription.name.endsWith(subscriptionName)).toEqual(true);
        });
    });

    describe('getSubscription', () => {
        it('should return a subscription with the provided name when found', async () => {
            const subscriptionName = 'subscription-123';
            const topic = {
                getSubscriptions() {
                }
            } as Topic;
            const getSubscriptionsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[{name: `projects/project-123/subscriptions/${subscriptionName}`} as Topic]]));
            jest
                .spyOn(topic, 'getSubscriptions')
                .mockImplementation(getSubscriptionsMock);

            const subscription = await pubSubService.getSubscription(topic, subscriptionName);

            expect(subscription).toBeDefined();
        });

        it('should return undefined when a subscription with the provided name is not found', async () => {
            const subscriptionName = 'subscription-123';
            const topic = {
                getSubscriptions() {
                }
            } as Topic;
            const getSubscriptionsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[]]));
            jest
                .spyOn(topic, 'getSubscriptions')
                .mockImplementation(getSubscriptionsMock);

            const subscription = await pubSubService.getSubscription(topic, subscriptionName);

            expect(subscription).toBeUndefined();
        });
    });

    describe('getOrCreateSubscription', () => {
        it('should return an existing subscription if one with the provided name is found', async () => {
            const existingSubscriptionName = 'subscription-123';
            const existingSubscriptionFullName = `projects/project-123/subscriptions/${existingSubscriptionName}`;
            const newSubscriptionName = 'subscription-123';
            const topic = {
                createSubscription(_: string) {
                },
                getSubscriptions() {
                }
            } as Topic;
            const getSubscriptionsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[{name: existingSubscriptionFullName} as Topic]]));
            const createSubscriptionMock = jest.fn()
                .mockReturnValue(Promise.resolve([{name: `projects/project-123/subscriptions/${newSubscriptionName}`} as Topic]));
            jest
                .spyOn(topic, 'getSubscriptions')
                .mockImplementation(getSubscriptionsMock);
            jest
                .spyOn(topic, 'createSubscription')
                .mockImplementation(createSubscriptionMock);

            const subscription = await pubSubService.getOrCreateSubscription(topic, existingSubscriptionName);

            expect(subscription.name).toEqual(existingSubscriptionFullName);
            expect(getSubscriptionsMock).toHaveBeenCalledTimes(1);
            expect(createSubscriptionMock).toHaveBeenCalledTimes(0);
        });

        it('should create a new topic if one with the provided name is not found', async () => {
            const existingSubscriptionName = 'subscription-123';
            const newSubscriptionName = 'subscription-456';
            const newSubscriptionFullName = `projects/project-123/subscriptions/${newSubscriptionName}`;
            const topic = {
                createSubscription(_: string) {
                },
                getSubscriptions() {
                }
            } as Topic;
            const getSubscriptionsMock = jest.fn()
                .mockReturnValue(Promise.resolve([[]]));
            const createSubscriptionMock = jest.fn()
                .mockReturnValue(Promise.resolve([{name: newSubscriptionFullName} as Topic]));
            jest
                .spyOn(topic, 'getSubscriptions')
                .mockImplementation(getSubscriptionsMock);
            jest
                .spyOn(topic, 'createSubscription')
                .mockImplementation(createSubscriptionMock);

            const subscription = await pubSubService.getOrCreateSubscription(topic, existingSubscriptionName);

            expect(subscription.name).toEqual(newSubscriptionFullName);
            expect(getSubscriptionsMock).toHaveBeenCalledTimes(1);
            expect(createSubscriptionMock).toHaveBeenCalledTimes(1);
        });
    });

    describe('subscribe', () => {
        it('should trigger the returned observable when a message is received', done => {
            expect.assertions(2);

            const message = {
                ack: () => {
                }
            } as Message;
            jest
                .spyOn(pubSubService, 'getOrCreateTopic')
                .mockReturnValue(Promise.resolve({} as Topic));
            jest
                .spyOn(pubSubService, 'getOrCreateSubscription')
                .mockReturnValue(Promise.resolve({
                    on: (_, listener) => {
                        listener(message);
                    }
                } as Subscription));
            jest
                .spyOn(Subscription.prototype, 'on')
                .mockImplementation((_: string, __: (message: Message) => void) => ({} as Subscription));
            const ackMock = jest.fn();
            jest
                .spyOn(message, 'ack')
                .mockImplementation(ackMock);

            pubSubService
                .subscribe('', '')
                .subscribe(value => {
                    expect(value).toBe(message);
                    process.nextTick(() => {
                        expect(ackMock).toHaveBeenCalled();
                        done();
                    });
                });
        });
    });
});
