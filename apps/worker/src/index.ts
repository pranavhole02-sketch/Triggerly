import "dotenv/config";
import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";
import fs from "fs";
import path from "path";

const prismaClient = new PrismaClient();
const TOPIC_NAME = process.env.TOPIC_NAME!;

const caPath = path.resolve(__dirname, "../certs/ca.pem");
console.log("Resolved CA path:", caPath); // Debug

const kafka = new Kafka({
  clientId: "outbox-processor-consumer",
  brokers: [process.env.KAFKA_BROKER!],
  ssl: {
    rejectUnauthorized: true,
    ca: [Buffer.from(process.env.KAFKA_CA_BASE64!, "base64").toString("utf-8")],
  },
  sasl: {
    mechanism: "scram-sha-256",
    username: process.env.KAFKA_USERNAME!,
    password: process.env.KAFKA_PASSWORD!,
  },
});

async function main() {
  const consumer = kafka.consumer({ groupId: "main-worker-2" });
  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();
  console.log("Consumer connected ✅");

  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      if (!message.value) return;

      console.log("Received raw:", message.value.toString());
      let parsed;
      try {
        parsed = JSON.parse(message.value.toString());
      } catch (err) {
        console.error("Invalid JSON:", message.value.toString(), err);
        return;
      }

      const { zapRunId, stage } = parsed;

      const zapRunDetails = await prismaClient.zapRun.findFirst({
        where: { id: zapRunId },
        include: {
          zap: {
            include: { actions: { include: { type: true } } },
          },
        },
      });

      const currentAction = zapRunDetails?.zap.actions.find(
        (x) => x.sortingOrder === stage
      );

      if (!currentAction) {
        console.log("No action found for zapRun:", zapRunId);
        return;
      }

      // ⚡ your business logic here (sendEmail, etc.)
      console.log("Processing zapRun:", zapRunId, "stage:", stage);

      const lastStage = (zapRunDetails?.zap.actions?.length || 1) - 1;
      if (stage < lastStage) {
        await producer.send({
          topic: TOPIC_NAME,
          messages: [
            {
              value: JSON.stringify({
                zapRunId,
                stage: stage + 1,
              }),
            },
          ],
        });
        console.log("Requeued for next stage");
      }
    },
  });
}

main().catch(console.error);
