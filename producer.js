import { Kafka } from "kafkajs";
import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

// console.table([['server', process.env.BOOTSTRAP],['key',process.env.KEY
// ],['secret', process.env.SECRET]])

const message = () => {
  return fetch("https://www.folgerdigitaltexts.org/WT/ftln/1201")
    .then((response) => response.text())
};

 const syncedMsg = message().then((e) => {
  return e
})


const kafka = new Kafka({
  clientId: "my-app",
  brokers: [process.env.BOOTSTRAP],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KEY,
    password: process.env.SECRET,
  },
});

const producer = kafka.producer();

const run = async () => {
  await producer.connect();
  await producer.send({
    topic: "test-topic",
    messages: [{ value: await syncedMsg}],
  });

  await producer.disconnect();
};

run().catch((e) =>
  kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack })
);
