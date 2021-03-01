const {Kafka} = require("kafkajs");

/**
 * Create asynchronous queue function.
 * @param {object} options
 * @param {string[]} options.brokers
 * @param {string} options.clientId
 * @param {string} options.groupId
 * @param {string} options.topic
 * @returns {KafkaQueue}
 */
function kafkaQueue({brokers, clientId, groupId, topic}) {
    const kafka = new Kafka({clientId, brokers});
    const producer = kafka.producer();
    const consumer = kafka.consumer({groupId});
    let consuming = false, producing = false, firstMessage = true;

    return {push, shift, disconnect};

    /**
     * Begin consuming.
     */
    async function consume() {
        // producing goes through 3 states: true, false, and Promise
        if (!consuming) {
            await consumer.connect();
            await consumer.subscribe({topic, fromBeginning: true});

            // TODO: verify assumptions about how this works
            // TODO: assumes serialized delivery that waits for handler
            consumer.run({eachMessage});

            consuming = true;
        }

        await consuming;
    }

    /**
     * Disconnect from Kafka.
     * @name {KafkaQueue#disconnect}
     */
    async function disconnect() {
        if (producing) try { await producer.disconnect(); } catch (err) {}
        if (consuming) try { await consumer.disconnect(); } catch (err) {}
    }

    /**
     * Callback for Kafka messages.
     * @param {object} info
     * @param {string} info.topic
     * @param {string} info.partition
     * @param {object} info.message
     */
    async function eachMessage({topic, partition, message: {value, offset}}) {
        value = value.toString("utf8");
        console.log("msg:", offset, value);
        await new Promise(go => setTimeout(go, 5000));
    }

    /**
     * Add document to end of queue.
     * @name {KafkaQueue#push}
     * @param {object} value
     */
    async function push(value) {
        value = JSON.stringify(value);

        await produce();

        try {
            await producer.send({topic, messages: [{value}]});
            firstMessage = false;
        } catch (err) {
            // topic may not exist, but should auto-create on message
            if (firstMessage && false) {
                // for first message only, retry once
                await producer.send({topic, messages: [{value}]});
                firstMessage = false;
            }
        }
    }

    /**
     * Begin producing.
     */
    async function produce() {
        // producing goes through 3 states: true, false, and Promise
        if (!producing) {
            firstMessage = true;
            promise = producing = producer.connect();
            promise.then(() => { if (producing === promise) producing = true; });
            promise.catch(e => { if (producing === promise) producing = false; });
        }

        await producing;
    }

    /**
     * Remove first document from queue.
     * @name {KafkaQueue#shift}
     * @returns {object}
     */
    async function shift() {
        await consume();

        // TODO: something useful
        // TODO: return JSON.parse(value);
    }

    return new KafkaQueue({clientId, brokers, topic});
}

module.exports = kafkaQueue;
