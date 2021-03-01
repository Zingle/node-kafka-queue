const expect = require("expect.js");
const sinon = require("sinon");
const {Kafka} = require("kafkajs");

describe("kafkaQueue({clientId, brokers, topic})", () => {
    let producer, was_producer;

    beforeEach(() => {
        was_producer = Kafka.prototype.producer;
        producer = sinon.spy();
        Kafka.prototype.producer = producer;
    });

    afterEach(() => {
        Kafka.prototype.producer = was_producer;
    });

    describe("return value", () => {
        it("should be a queue function", () => {

        });
    })
    it("should return queue function", () => {

    });
});
