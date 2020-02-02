import {Message, PubSub, Subscription, Topic} from '@google-cloud/pubsub';
import {Injectable} from '@nestjs/common';
import {Observable} from 'rxjs';
import {fromPromise} from 'rxjs/internal-compatibility';
import {switchMap} from 'rxjs/operators';

@Injectable()
export class PubSubService {
    private readonly pubSub = new PubSub();

    async createTopic(name: string): Promise<Topic> {
        const [topic] = await this.pubSub.createTopic(name);
        return topic;
    }

    async getTopic(name: string): Promise<Topic | undefined> {
        const [topics] = await this.pubSub.getTopics();
        return topics.find(topic => topic.name.substr(topic.name.lastIndexOf('/') + 1) === name);
    }

    async getOrCreateTopic(name: string): Promise<Topic> {
        const topic = await this.getTopic(name);
        if (topic) {
            return topic;
        } else {
            return await this.createTopic(name);
        }
    }

    async createSubscription(topic: Topic, name: string): Promise<Subscription> {
        const [subscription] = await topic.createSubscription(name);
        return subscription;
    }

    async getSubscription(topic: Topic, name: string): Promise<Subscription | undefined> {
        const [subscriptions] = await topic.getSubscriptions();
        return subscriptions.find(subscription => subscription.name.endsWith(name));
    }

    async getOrCreateSubscription(topic: Topic, name: string): Promise<Subscription> {
        const subscription = await this.getSubscription(topic, name);
        if (subscription) {
            return subscription;
        } else {
            return this.createSubscription(topic, name);
        }
    }

    subscribe(topicName: string, subscriptionName: string): Observable<Message> {
        const getObservable = async (): Promise<Observable<Message>> => {
            const topic = await this.getOrCreateTopic(topicName);
            const subscription = await this.getOrCreateSubscription(topic, subscriptionName);
            return new Observable<Message>(subscriber => {
                subscription.on('message', (message: Message) => {
                    message.ack();
                    subscriber.next(message);
                });
            });
        };
        return fromPromise(getObservable()).pipe(switchMap(observable => observable));
    }
}
