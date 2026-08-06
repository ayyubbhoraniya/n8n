import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { INodeTypeBaseDescription, IRun } from 'n8n-workflow';
import { TriggerCloseError } from 'n8n-workflow';
import type { Mock, Mocked } from 'vitest';
import { mock } from 'vitest-mock-extended';

import { testTriggerNode } from '@test/nodes/TriggerHelpers';

import {
	KafkaTriggerV2,
	manualRunGroupId,
	toConsumerOptions,
	toEmitterOptions,
} from '../../v2/KafkaTriggerV2.node';
import {
	confluentKafkaModuleMock,
	getFakeConsumers,
	resetConfluentKafkaRecordings,
	type FakeConsumer,
} from '../mocks/confluent-kafka';

vi.mock('@confluentinc/kafka-javascript', () => confluentKafkaModuleMock());
vi.mock('@kafkajs/confluent-schema-registry');

// Wraps the real consumeTopic in a spy rather than replacing it, so the actual
// loop still drives these tests. Only used for options that reach the loop but
// leave no trace on the fake consumer, such as errorRetryDelay.
const { consumeTopicSpy } = vi.hoisted(() => ({ consumeTopicSpy: vi.fn() }));
vi.mock('../../v2/consumer', async (importOriginal) => {
	const actual = await importOriginal<typeof import('../../v2/consumer')>();
	return {
		...actual,
		consumeTopic: vi.fn(async (...args: Parameters<typeof actual.consumeTopic>) => {
			consumeTopicSpy(...args);
			return await actual.consumeTopic(...args);
		}),
	};
});

const baseDescription: INodeTypeBaseDescription = {
	displayName: 'Kafka Trigger',
	name: 'kafkaTrigger',
	icon: { light: 'file:kafka.svg', dark: 'file:kafka.dark.svg' },
	group: ['trigger'],
	defaultVersion: 1.3,
	description: 'Consume messages from a Kafka topic',
};

const credential = {
	brokers: 'localhost:9092',
	clientId: 'n8n-kafka',
	ssl: false,
	authentication: false,
};

async function lastFakeConsumer(): Promise<FakeConsumer> {
	const consumer = getFakeConsumers().at(-1);
	if (!consumer) throw new Error('the fake recorded no consumer');
	return consumer;
}

/**
 * Starts the trigger. Defaults to `immediately`, so the emitter does not wait on
 * an execution: tests about parsing and consumer settings can then deliver a
 * batch without also having to resolve a run.
 */
async function startTrigger(
	groupId: string,
	parameters: Record<string, unknown> = {},
	overrides: Parameters<typeof testTriggerNode>[1] = {},
) {
	return await testTriggerNode(new KafkaTriggerV2(baseDescription), {
		mode: 'trigger',
		node: {
			parameters: {
				topic: 'test-topic',
				groupId,
				useSchemaRegistry: false,
				resolveOffset: 'immediately',
				...parameters,
			},
		},
		credential,
		...overrides,
	});
}

describe('toConsumerOptions', () => {
	it("applies v1's consumer defaults when the user set nothing", () => {
		const result = toConsumerOptions({}, 'my-group', undefined);

		expect(result).toStrictEqual({
			groupId: 'my-group',
			sessionTimeout: 30000,
			// v1.3's default, not the 3000 v1 uses below 1.3
			heartbeatInterval: 10000,
			// No workflow timeout, so the Rebalance Timeout default stands in, halved
			rebalanceTimeout: 300000,
			maxBytesPerPartition: undefined,
			minBytes: undefined,
			maxInFlightRequests: undefined,
			fromBeginning: undefined,
			allowAutoTopicCreation: undefined,
		});
	});

	it('passes the user-set consumer options through', () => {
		const result = toConsumerOptions(
			{
				sessionTimeout: 20000,
				heartbeatInterval: 2000,
				fetchMaxBytes: 2097152,
				fetchMinBytes: 1024,
				maxInFlightRequests: 5,
				fromBeginning: true,
			},
			'my-group',
			undefined,
		);

		expect(result).toMatchObject({
			sessionTimeout: 20000,
			heartbeatInterval: 2000,
			maxBytesPerPartition: 2097152,
			minBytes: 1024,
			maxInFlightRequests: 5,
			fromBeginning: true,
		});
	});

	it('halves the workflow execution timeout, since the library doubles it', () => {
		// 600s of workflow timeout must stay 600s of processing headroom, and the
		// library sets max.poll.interval.ms to twice whatever it is handed.
		const result = toConsumerOptions({}, 'my-group', 600);

		expect(result.rebalanceTimeout).toBe(300000);
	});

	it('falls back to the Rebalance Timeout option when the workflow timeout is unbounded', () => {
		// n8n treats <= 0 as explicitly unbounded, and there is no deadline to derive
		// from, so the node's own option decides.
		const result = toConsumerOptions({ rebalanceTimeout: 900000 }, 'my-group', -1);

		expect(result.rebalanceTimeout).toBe(450000);
	});

	it('drops a zero Max Number of Requests instead of forwarding it', () => {
		// v1 turns 0 into null to mean "no limit". The library has no such sentinel,
		// and a present-but-undefined key makes librdkafka fail.
		const result = toConsumerOptions({ maxInFlightRequests: 0 }, 'my-group', undefined);

		expect(result.maxInFlightRequests).toBeUndefined();
	});
});

describe('manualRunGroupId', () => {
	it('uses the configured group for an activated workflow', () => {
		expect(manualRunGroupId('orders-consumer', false)).toBe('orders-consumer');
	});

	it('gives a manual run its own group, so it cannot take production offsets', () => {
		const first = manualRunGroupId('orders-consumer', true);
		const second = manualRunGroupId('orders-consumer', true);

		expect(first).toMatch(/^orders-consumer-n8n-manual-.+/);
		expect(first).not.toBe('orders-consumer');
		// Two editors testing at once must not land in the same group either.
		expect(second).not.toBe(first);
	});
});

describe('toEmitterOptions', () => {
	it('passes the execution timeout through raw, so unbounded stays unbounded', () => {
		expect(toEmitterOptions({}, 'onCompletion', [], 0).executionTimeoutSeconds).toBe(0);
		expect(
			toEmitterOptions({}, 'onCompletion', [], undefined).executionTimeoutSeconds,
		).toBeUndefined();
		expect(toEmitterOptions({}, 'onCompletion', [], 120).executionTimeoutSeconds).toBe(120);
	});

	it('carries the mode, allowed statuses and retry delay', () => {
		const result = toEmitterOptions({ errorRetryDelay: 1234 }, 'onStatus', ['success'], 60);

		expect(result).toStrictEqual({
			resolveOffsetMode: 'onStatus',
			allowedStatuses: ['success'],
			executionTimeoutSeconds: 60,
			errorRetryDelay: 1234,
		});
	});
});

describe('KafkaTriggerV2 Node', () => {
	beforeEach(() => {
		resetConfluentKafkaRecordings();
		consumeTopicSpy.mockClear();
	});

	it('connects, subscribes to the topic, and emits a received message', async () => {
		const { emit, close } = await startTrigger('v2-basic');

		const consumer = await lastFakeConsumer();
		expect(consumer.connect).toHaveBeenCalledTimes(1);
		expect(consumer.subscribe).toHaveBeenCalledWith({ topics: ['test-topic'] });
		expect(consumer.run).toHaveBeenCalledTimes(1);

		await consumer.deliverBatch({
			topic: 'test-topic',
			messages: [{ value: Buffer.from('message') }],
		});

		expect(emit).toHaveBeenCalledWith([[{ json: { message: 'message', topic: 'test-topic' } }]]);

		await close();
		expect(consumer.disconnect).toHaveBeenCalled();
	});

	describe('Batch Size', () => {
		it('starts one execution per message by default, as v1 does', async () => {
			const { emit } = await startTrigger('v2-batch-default');
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [
					{ value: Buffer.from('message1') },
					{ value: Buffer.from('message2') },
					{ value: Buffer.from('message3') },
				],
			});

			// A 3-message library batch must not collapse into one 3-item execution.
			expect(emit).toHaveBeenCalledTimes(3);
			expect(emit).toHaveBeenNthCalledWith(1, [
				[{ json: { message: 'message1', topic: 'test-topic' } }],
			]);
			expect(emit).toHaveBeenNthCalledWith(3, [
				[{ json: { message: 'message3', topic: 'test-topic' } }],
			]);
		});

		it('chunks into executions of Batch Size items when set above 1', async () => {
			const { emit } = await startTrigger('v2-batch-2', { options: { batchSize: 2 } });
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [
					{ value: Buffer.from('message1') },
					{ value: Buffer.from('message2') },
					{ value: Buffer.from('message3') },
				],
			});

			expect(emit).toHaveBeenCalledTimes(2);
			expect(emit).toHaveBeenNthCalledWith(1, [
				[
					{ json: { message: 'message1', topic: 'test-topic' } },
					{ json: { message: 'message2', topic: 'test-topic' } },
				],
			]);
			expect(emit).toHaveBeenNthCalledWith(2, [
				[{ json: { message: 'message3', topic: 'test-topic' } }],
			]);
		});
	});

	describe('consumer options reach the library', () => {
		it('hands the user-set consumer options to the factory', async () => {
			await startTrigger('v2-consumer-options', {
				options: {
					sessionTimeout: 20000,
					heartbeatInterval: 2000,
					fetchMaxBytes: 2097152,
					fetchMinBytes: 1024,
					maxInFlightRequests: 5,
					fromBeginning: true,
					allowAutoTopicCreation: true,
				},
			});

			const consumer = await lastFakeConsumer();
			expect(consumer.config.kafkaJS).toMatchObject({
				groupId: 'v2-consumer-options',
				sessionTimeout: 20000,
				heartbeatInterval: 2000,
				maxBytesPerPartition: 2097152,
				minBytes: 1024,
				maxInFlightRequests: 5,
				fromBeginning: true,
				// v1 declares this option and never reads it. v2 maps it onto the
				// library's `allow.auto.create.topics`, so subscribing to a topic that
				// does not exist yet creates it instead of erroring.
				allowAutoTopicCreation: true,
			});
		});

		it('leaves Allow Topic Creation off the config when the user did not set it', async () => {
			// A key present with value `undefined` makes librdkafka skip its own
			// default and then fail on the value, so it must be absent, not undefined.
			await startTrigger('v2-no-auto-create');

			const consumer = await lastFakeConsumer();
			expect(consumer.config.kafkaJS).not.toHaveProperty('allowAutoTopicCreation');
		});

		it('passes a caller-chosen partition concurrency to the loop', async () => {
			await startTrigger('v2-concurrency', {
				options: { partitionsConsumedConcurrently: 4 },
			});

			const consumer = await lastFakeConsumer();
			expect(consumer.runConfig?.partitionsConsumedConcurrently).toBe(4);
		});

		it('passes Retry Delay on Error to the loop', async () => {
			await startTrigger('v2-retry-delay', { options: { errorRetryDelay: 12345 } });

			expect(consumeTopicSpy).toHaveBeenCalledWith(
				expect.anything(),
				expect.objectContaining({ errorRetryDelay: 12345 }),
			);
		});
	});

	describe('fatal consumer errors', () => {
		/** The logger the node handed the library, which is where fatal errors surface. */
		function libraryLogger(consumer: FakeConsumer) {
			const logger = consumer.config.kafkaJS?.logger;
			if (!logger) throw new Error('the node gave the library no logger');
			return logger;
		}

		it('surfaces a non-recoverable consumer error through emitError, as v1 does', async () => {
			const { emitError } = await startTrigger('v2-fatal');
			const consumer = await lastFakeConsumer();

			libraryLogger(consumer).error('Broker: Group authorization failed');

			expect(emitError).toHaveBeenCalledTimes(1);
			expect(emitError.mock.calls[0][0].message).toMatch(/authorization failed/i);
		});

		it('stays quiet for an error the library can recover from', async () => {
			const { emitError } = await startTrigger('v2-recoverable');
			const consumer = await lastFakeConsumer();

			libraryLogger(consumer).error('Broker transport failure');

			expect(emitError).not.toHaveBeenCalled();
		});

		it('stays quiet for an error caused by our own teardown', async () => {
			const { emitError, close } = await startTrigger('v2-fatal-on-close');
			const consumer = await lastFakeConsumer();

			await close();
			libraryLogger(consumer).error('Broker: Group authorization failed');

			expect(emitError).not.toHaveBeenCalled();
		});
	});

	describe('message shape options', () => {
		it('parses JSON and returns only the message when both are set', async () => {
			const jsonData = { foo: 'bar' };
			const { emit } = await startTrigger('v2-json', {
				options: { jsonParseMessage: true, onlyMessage: true },
			});
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from(JSON.stringify(jsonData)) }],
			});

			expect(emit).toHaveBeenCalledWith([[{ json: jsonData }]]);
		});

		it('includes headers when returnHeaders is true', async () => {
			const { emit } = await startTrigger('v2-headers', { options: { returnHeaders: true } });
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [
					{
						value: Buffer.from('test-message'),
						headers: { 'content-type': Buffer.from('application/json') },
					},
				],
			});

			expect(emit).toHaveBeenCalledWith([
				[
					{
						json: {
							message: 'test-message',
							topic: 'test-topic',
							headers: { 'content-type': 'application/json' },
						},
					},
				],
			]);
		});

		it('keeps binary data when keepBinaryData is enabled', async () => {
			const { emit } = await startTrigger('v2-binary', { options: { keepBinaryData: true } });
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('binary-data') }],
			});

			const emittedItem = emit.mock.calls[0][0][0][0];
			expect(emittedItem).toHaveProperty('binary');
			expect(emittedItem.json.message).toBe('binary-data');
		});
	});

	describe('Schema Registry', () => {
		it('decodes through the registry when enabled', async () => {
			const mockDecode = vi.fn().mockResolvedValue({ data: 'decoded-data' });
			(SchemaRegistry as unknown as Mock).mockImplementation(function () {
				return { decode: mockDecode } as unknown as Mocked<SchemaRegistry>;
			});

			const { emit } = await startTrigger('v2-registry', {
				useSchemaRegistry: true,
				schemaRegistryUrl: 'http://localhost:8081',
			});
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('avro-encoded') }],
			});

			expect(SchemaRegistry).toHaveBeenCalledWith({ host: 'http://localhost:8081' });
			expect(mockDecode).toHaveBeenCalledWith(Buffer.from('avro-encoded'));
			expect(emit).toHaveBeenCalledWith([
				[{ json: { message: { data: 'decoded-data' }, topic: 'test-topic' } }],
			]);
		});

		it('activates anyway and warns when the registry is unreachable, same as v1', async () => {
			(SchemaRegistry as unknown as Mock).mockImplementationOnce(function () {
				throw Object.assign(new Error('connect ECONNREFUSED'), { status: 503 });
			});

			const { emit, logger } = await startTrigger('v2-registry-down', {
				useSchemaRegistry: true,
				schemaRegistryUrl: 'http://localhost:8081',
			});

			expect(logger.warn).toHaveBeenCalledWith('Could not connect to Schema Registry', {
				message: 'connect ECONNREFUSED',
				status: 503,
			});

			const consumer = await lastFakeConsumer();
			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('raw-message') }],
			});

			// No registry to decode with, so the raw message is emitted, as v1 does.
			expect(emit).toHaveBeenCalledWith([
				[{ json: { message: 'raw-message', topic: 'test-topic' } }],
			]);
		});

		it('fails activation when the registry credential is misconfigured, same as v1', async () => {
			await expect(
				testTriggerNode(new KafkaTriggerV2(baseDescription), {
					mode: 'trigger',
					node: {
						credentials: {
							kafka: { id: '1', name: 'Kafka account' },
							schemaRegistryApi: { id: '2', name: 'Schema Registry account' },
						},
						parameters: {
							topic: 'test-topic',
							groupId: 'v2-registry-misconfigured',
							useSchemaRegistry: true,
							schemaRegistryUrl: '',
							resolveOffset: 'immediately',
						},
					},
					credentials: {
						kafka: credential,
						schemaRegistryApi: {
							url: 'https://schema-registry.local:8081',
							authentication: 'basicAuth',
							username: 'registry-user',
							password: '',
						},
					},
				}),
			).rejects.toThrow('Username and password are required for Schema Registry Basic Auth');
		});
	});

	describe('offset resolution', () => {
		it('waits for the execution before resolving the offset on onCompletion', async () => {
			const { emit } = await startTrigger('v2-on-completion', { resolveOffset: 'onCompletion' });
			const consumer = await lastFakeConsumer();

			const delivered = consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('message') }],
			});
			await new Promise((resolve) => setImmediate(resolve));

			// The offset must not advance while the execution is still running.
			expect(emit).toHaveBeenCalled();
			expect(consumer.payloadSpies.resolveOffset).not.toHaveBeenCalled();

			const deferred = emit.mock.calls[0][2];
			expect(deferred).toBeDefined();
			deferred?.resolve(mock<IRun>({ status: 'success' }));
			await delivered;

			expect(consumer.payloadSpies.resolveOffset).toHaveBeenCalledTimes(1);
		});

		it('does not wait for the execution on immediately', async () => {
			const { emit } = await startTrigger('v2-immediately');
			const consumer = await lastFakeConsumer();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('message') }],
			});

			// No deferred promise means nothing to wait on.
			expect(emit.mock.calls[0][2]).toBeUndefined();
			expect(consumer.payloadSpies.resolveOffset).toHaveBeenCalledTimes(1);
		});

		it('leaves the offset unresolved when the execution status is not allowed', async () => {
			const { emit } = await startTrigger('v2-on-status', {
				resolveOffset: 'onStatus',
				allowedStatuses: ['success'],
				// A rejected status paces the re-delivery before reporting failure;
				// the default 5s would outlast the test.
				options: { errorRetryDelay: 1 },
			});
			const consumer = await lastFakeConsumer();

			const delivered = consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('message') }],
			});
			await new Promise((resolve) => setImmediate(resolve));

			emit.mock.calls[0][2]?.resolve(mock<IRun>({ status: 'error' }));
			await delivered;

			expect(consumer.payloadSpies.resolveOffset).not.toHaveBeenCalled();
		});
	});

	describe('manual test run isolation from production', () => {
		async function startManualRun(parameters: Record<string, unknown> = {}) {
			const started = await testTriggerNode(new KafkaTriggerV2(baseDescription), {
				mode: 'manual',
				node: {
					parameters: {
						topic: 'test-topic',
						groupId: 'orders-consumer',
						useSchemaRegistry: false,
						...parameters,
					},
				},
				credential,
			});
			await started.manualTriggerFunction?.();
			return started;
		}

		it('joins a throwaway group, never the one the activated workflow uses', async () => {
			await startManualRun();

			const consumer = await lastFakeConsumer();
			const { groupId } = consumer.config.kafkaJS ?? {};
			// Sharing the group would let this run commit offsets for messages the
			// activated workflow never received.
			expect(groupId).not.toBe('orders-consumer');
			expect(groupId).toMatch(/^orders-consumer-n8n-manual-.+/);
		});

		it('waits for the next message rather than replaying the topic', async () => {
			// Read Messages From Beginning defaults to on, and the throwaway group has
			// no committed offset, so honouring it would replay everything.
			await startManualRun({ options: { fromBeginning: true } });

			const consumer = await lastFakeConsumer();
			expect(consumer.config.kafkaJS?.fromBeginning).toBe(false);
		});

		it('still honours Read Messages From Beginning for an activated workflow', async () => {
			await startTrigger('v2-from-beginning', { options: { fromBeginning: true } });

			const consumer = await lastFakeConsumer();
			expect(consumer.config.kafkaJS?.fromBeginning).toBe(true);
		});
	});

	describe('manual test run', () => {
		it('starts the loop only when the test event is requested, and never waits', async () => {
			// v1 forces `immediately` in manual mode. The editor discards the run once
			// it has its sample, so a wait would hold the batch open until close.
			const { emit, manualTriggerFunction } = await testTriggerNode(
				new KafkaTriggerV2(baseDescription),
				{
					mode: 'manual',
					node: {
						parameters: {
							topic: 'test-topic',
							groupId: 'v2-manual',
							useSchemaRegistry: false,
							resolveOffset: 'onCompletion',
						},
					},
					credential,
				},
			);

			expect(getFakeConsumers()).toHaveLength(0);

			await manualTriggerFunction?.();

			const consumer = await lastFakeConsumer();
			expect(consumer.connect).toHaveBeenCalledTimes(1);
			expect(emit).not.toHaveBeenCalled();

			await consumer.deliverBatch({
				topic: 'test-topic',
				messages: [{ value: Buffer.from('test') }],
			});

			expect(emit).toHaveBeenCalledWith([[{ json: { message: 'test', topic: 'test-topic' } }]]);
			// Forced to immediately despite the node asking for onCompletion.
			expect(emit.mock.calls[0][2]).toBeUndefined();
		});
	});

	describe('close', () => {
		it('disconnects the consumer', async () => {
			const { close } = await startTrigger('v2-close');
			const consumer = await lastFakeConsumer();

			await close();

			expect(consumer.disconnect).toHaveBeenCalled();
		});

		it('reports a failed teardown as a TriggerCloseError, as v1 does', async () => {
			const { close } = await startTrigger('v2-close-fails');
			const consumer = await lastFakeConsumer();
			const teardownError = new Error('The coordinator is not aware of this member');
			consumer.disconnect.mockRejectedValueOnce(teardownError);

			const error = await close().then(
				() => null,
				(e: unknown) => e,
			);

			expect(error).toBeInstanceOf(TriggerCloseError);
			expect((error as TriggerCloseError).cause).toBe(teardownError);
			expect((error as TriggerCloseError).level).toBe('warning');
		});
	});
});
