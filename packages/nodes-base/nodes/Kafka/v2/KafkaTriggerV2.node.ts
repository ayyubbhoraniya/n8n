import { ensureError } from '@n8n/utils/errors/ensure-error';
import { randomUUID } from 'node:crypto';
import type {
	INodeTypeBaseDescription,
	INodeTypeDescription,
	INodeType,
	ITriggerFunctions,
	ITriggerResponse,
} from 'n8n-workflow';
import { NodeConnectionTypes, NodeOperationError, TriggerCloseError } from 'n8n-workflow';

import { setSchemaRegistry, type KafkaCredentials } from '../utils';
import {
	consumeTopic,
	createDataEmitter,
	createMessageParser,
	type DataEmitterOptions,
	type KafkaConsumerHandle,
	type KafkaMessageParserOptions,
	type ResolveOffsetMode,
} from './consumer';
import { createKafkaConsumer, type KafkaConsumerOptions } from './transport';

/** v1.3's `options` collection. Every field is optional: a `collection` node
 * parameter only carries the keys the user actually set, never its declared
 * UI defaults, so the converters below resolve each one explicitly. */
interface KafkaTriggerV2Options extends KafkaMessageParserOptions {
	batchSize?: number;
	partitionsConsumedConcurrently?: number;
	errorRetryDelay?: number;
	sessionTimeout?: number;
	heartbeatInterval?: number;
	rebalanceTimeout?: number;
	fetchMaxBytes?: number;
	fetchMinBytes?: number;
	maxInFlightRequests?: number;
	fromBeginning?: boolean;
	allowAutoTopicCreation?: boolean;
	eachBatchAutoResolve?: boolean;
	autoCommitInterval?: number;
}

/** v1's defaults for the consumer settings, from `createConsumerConfig`. v2 is
 * always >= 1.3, so the heartbeat default is 1.3's 10s rather than 3s. */
const DEFAULT_SESSION_TIMEOUT_MS = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 10_000;
const DEFAULT_REBALANCE_TIMEOUT_MS = 600_000;

/**
 * The consumer group a run joins. A manual run gets a throwaway one.
 *
 * Sharing the configured group with an activated workflow means Kafka splits the
 * partitions between the two, and a test run resolves offsets immediately, so
 * pressing "Listen for test event" would mark messages read that the activated
 * workflow never saw. A group of its own leaves production untouched. v1 has
 * this defect and needs its own fix.
 *
 * The throwaway group is left behind on the broker; Kafka expires an empty group
 * on its own once its retention window passes.
 * @param configured - The node's Group ID parameter
 * @param isManualRun - Whether this is an editor test run
 */
export function manualRunGroupId(configured: string, isManualRun: boolean): string {
	return isManualRun ? `${configured}-n8n-manual-${randomUUID()}` : configured;
}

/**
 * Maps the node's options onto the consumer factory.
 *
 * `rebalanceTimeout` is the one value not passed straight through. In this
 * library it also becomes `max.poll.interval.ms`, the deadline to finish one
 * batch before the consumer is dropped from its group, and the library doubles
 * whatever it is given (`_consumer.js:711`). So the workflow's own execution
 * timeout is the deadline we actually want, and it is halved here to survive
 * that doubling. An unbounded workflow timeout (<= 0) has no deadline to
 * derive from, so the Rebalance Timeout option stands in.
 * @param options - The node's `options` collection
 * @param groupId - The node's consumer group id
 * @param executionTimeoutSeconds - `getWorkflowSettings().executionTimeout`, raw
 */
export function toConsumerOptions(
	options: KafkaTriggerV2Options,
	groupId: string,
	executionTimeoutSeconds: number | undefined,
): KafkaConsumerOptions {
	const deadlineMs =
		executionTimeoutSeconds !== undefined && executionTimeoutSeconds > 0
			? executionTimeoutSeconds * 1000
			: (options.rebalanceTimeout ?? DEFAULT_REBALANCE_TIMEOUT_MS);

	return {
		groupId,
		sessionTimeout: options.sessionTimeout ?? DEFAULT_SESSION_TIMEOUT_MS,
		heartbeatInterval: options.heartbeatInterval ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
		rebalanceTimeout: Math.ceil(deadlineMs / 2),
		maxBytesPerPartition: options.fetchMaxBytes,
		minBytes: options.fetchMinBytes,
		// v1 turns 0 into `null` to mean "no limit". The library has no such
		// sentinel, so leave the key off and let its own default stand.
		maxInFlightRequests: options.maxInFlightRequests || undefined,
		fromBeginning: options.fromBeginning,
		allowAutoTopicCreation: options.allowAutoTopicCreation,
	};
}

/**
 * Maps the node's options and offset settings onto the emitter.
 *
 * `executionTimeoutSeconds` is passed through raw: n8n treats <= 0 as
 * explicitly unbounded, and the emitter handles that. Coercing it to a default
 * here would reintroduce a deadline the user switched off.
 * @param options - The node's `options` collection
 * @param resolveOffsetMode - Already resolved, including the manual-mode override
 * @param allowedStatuses - Only meaningful when the mode is `onStatus`
 * @param executionTimeoutSeconds - `getWorkflowSettings().executionTimeout`, raw
 */
export function toEmitterOptions(
	options: KafkaTriggerV2Options,
	resolveOffsetMode: ResolveOffsetMode,
	allowedStatuses: string[],
	executionTimeoutSeconds: number | undefined,
): DataEmitterOptions {
	return {
		resolveOffsetMode,
		allowedStatuses,
		executionTimeoutSeconds,
		errorRetryDelay: options.errorRetryDelay,
	};
}

const versionDescription: INodeTypeDescription = {
	displayName: 'Kafka Trigger',
	name: 'kafkaTrigger',
	icon: { light: 'file:kafka.svg', dark: 'file:kafka.dark.svg' },
	group: ['trigger'],
	version: 2,
	description: 'Consume messages from a Kafka topic',
	defaults: {
		name: 'Kafka Trigger',
	},
	inputs: [],
	outputs: [NodeConnectionTypes.Main],
	credentials: [
		{
			name: 'kafka',
			required: true,
		},
		{
			name: 'schemaRegistryApi',
			required: false,
			displayName: 'Schema Registry',
			displayOptions: {
				show: {
					useSchemaRegistry: [true],
				},
			},
		},
	],
	properties: [
		{
			displayName: 'Topic',
			name: 'topic',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'topic-name',
			description: 'Name of the queue of topic to consume from',
		},
		{
			displayName: 'Group ID',
			name: 'groupId',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'n8n-kafka',
			description: 'ID of the consumer group',
		},
		{
			displayName: 'Resolve Offset',
			name: 'resolveOffset',
			type: 'options',
			default: 'onCompletion',
			description:
				'Select on which condition the offsets should be resolved. In the manual mode, when execution started by clicking on Execute Workflow or Execute Step button, offsets are always resolved immediately after message received.',
			options: [
				{
					name: 'On Execution Completion',
					value: 'onCompletion',
					description: 'Resolve offset after execution completion regardless of the status',
				},
				{
					name: 'On Execution Success',
					value: 'onSuccess',
					description: 'Resolve offset only if execution status equals success',
				},
				{
					name: 'On Allowed Execution Statuses',
					value: 'onStatus',
					description: 'Resolve offset only if execution status in the list of selected statuses',
				},
				{
					name: 'Immediately',
					value: 'immediately',
					description:
						'Resolve offset immediately after message received. This option is not recommended as it can cause messages loss.',
				},
			],
		},
		{
			displayName: 'Allowed Statuses',
			name: 'allowedStatuses',
			type: 'multiOptions',
			default: ['success'],
			options: [
				{
					name: 'Canceled',
					value: 'canceled',
				},
				{
					name: 'Crashed',
					value: 'crashed',
				},
				{
					name: 'Error',
					value: 'error',
				},
				{
					name: 'New',
					value: 'new',
				},
				{
					name: 'Running',
					value: 'running',
				},
				{
					name: 'Success',
					value: 'success',
				},
				{
					name: 'Unknown',
					value: 'unknown',
				},
				{
					name: 'Waiting',
					value: 'waiting',
				},
			],
			displayOptions: {
				show: {
					resolveOffset: ['onStatus'],
				},
			},
		},
		{
			displayName: 'Use Schema Registry',
			name: 'useSchemaRegistry',
			type: 'boolean',
			default: false,
			description: 'Whether to use Confluent Schema Registry',
		},
		{
			displayName: 'Schema Registry URL',
			name: 'schemaRegistryUrl',
			type: 'string',
			displayOptions: {
				show: {
					useSchemaRegistry: [true],
				},
			},
			placeholder: 'https://schema-registry-domain:8081',
			default: '',
			description:
				'URL of the schema registry. Only used when no Schema Registry credential is selected.',
		},
		{
			displayName: 'Options',
			name: 'options',
			type: 'collection',
			default: {},
			placeholder: 'Add option',
			options: [
				{
					displayName: 'Allow Topic Creation',
					name: 'allowAutoTopicCreation',
					type: 'boolean',
					default: false,
					// Reworded from v1, which describes sending to a topic. v1 never reads
					// the option at all; v2 maps it onto the consumer's
					// `allow.auto.create.topics`.
					description:
						'Whether to allow subscribing to a previously non-existing topic, creating it',
				},
				{
					displayName: 'Auto Commit Interval',
					name: 'autoCommitInterval',
					type: 'number',
					default: 0,
					description:
						'The consumer will commit offsets after a given period, for example, five seconds',
					hint: 'Value in milliseconds',
				},
				{
					displayName: 'Batch Size',
					name: 'batchSize',
					type: 'number',
					default: 1,
					description:
						'Number of messages to process in each batch, when set to 1, message-by-message processing is enabled',
				},
				{
					displayName: 'Each Batch Auto Resolve',
					name: 'eachBatchAutoResolve',
					type: 'boolean',
					default: false,
					description: 'Whether to auto resolve offsets for each batch',
				},
				{
					displayName: 'Fetch Max Bytes',
					name: 'fetchMaxBytes',
					type: 'number',
					default: 1048576,
					description:
						'Maximum amount of data the server should return for a fetch request. In bytes. Default is 1MB. Higher values allow fetching more messages at once.',
				},
				{
					displayName: 'Fetch Min Bytes',
					name: 'fetchMinBytes',
					type: 'number',
					default: 1,
					description:
						'Minimum amount of data the server should return for a fetch request. In bytes. Server will wait up to fetchMaxWaitTime for this amount to accumulate.',
				},
				{
					displayName: 'Heartbeat Interval',
					name: 'heartbeatInterval',
					type: 'number',
					default: 10000,
					description:
						'Controls how often the consumer sends heartbeats to the broker to indicate it is still alive. Must be lower than Session Timeout. Recommended value is approximately one third of the Session Timeout (for example: 10s heartbeat with 30s session timeout).',
					hint: 'Value in milliseconds',
				},
				{
					displayName: 'Max Number of Requests',
					name: 'maxInFlightRequests',
					type: 'number',
					default: 1,
					description:
						'The maximum number of unacknowledged requests the client will send on a single connection',
				},
				{
					displayName: 'Read Messages From Beginning',
					name: 'fromBeginning',
					type: 'boolean',
					default: true,
					description: 'Whether to read message from beginning',
				},
				{
					displayName: 'JSON Parse Message',
					name: 'jsonParseMessage',
					type: 'boolean',
					default: false,
					description: 'Whether to try to parse the message to an object',
				},
				{
					displayName: 'Keep Message as Binary Data',
					name: 'keepBinaryData',
					type: 'boolean',
					default: false,
					description:
						'Whether to keep message value as binary data for downstream processing (e.g., Avro deserialization)',
				},
				{
					displayName: 'Partitions Consumed Concurrently',
					name: 'partitionsConsumedConcurrently',
					type: 'number',
					default: 0,
					description:
						'Number of Kafka partitions to process in parallel. Controls how many partitions are processed concurrently by the consumer.',
					hint: 'Set to 0 to process all partitions sequentially',
				},
				{
					displayName: 'Only Message',
					name: 'onlyMessage',
					type: 'boolean',
					displayOptions: {
						show: {
							jsonParseMessage: [true],
						},
					},
					default: false,
					description: 'Whether to return only the message property',
				},
				{
					displayName: 'Return Headers',
					name: 'returnHeaders',
					type: 'boolean',
					default: false,
					description: 'Whether to return the headers received from Kafka',
				},
				{
					displayName: 'Rebalance Timeout',
					name: 'rebalanceTimeout',
					type: 'number',
					default: 600000,
					description: 'The maximum time allowed for a consumer to join the group',
				},
				{
					displayName: 'Retry Delay on Error',
					name: 'errorRetryDelay',
					type: 'number',
					default: 5000,
					description:
						'Delay in milliseconds before retrying after a failed offset resolution. This prevents rapid retry loops that could overwhelm the Kafka broker.',
					hint: 'Value in milliseconds',
					typeOptions: {
						minValue: 1000,
					},
					displayOptions: {
						hide: {
							'/resolveOffset': ['immediately'],
						},
					},
				},
				{
					displayName: 'Session Timeout',
					name: 'sessionTimeout',
					type: 'number',
					default: 30000,
					description:
						'Timeout in milliseconds used to detect failures. Has to be higher than Heartbeat Interval. During the workflow execution heartbeat will be sent periodically to keep the session alive with configured Heartbeat Interval.',
					hint: 'Value in milliseconds',
				},
			],
		},
	],
};

export class KafkaTriggerV2 implements INodeType {
	description: INodeTypeDescription;

	constructor(baseDescription: INodeTypeBaseDescription) {
		this.description = {
			...baseDescription,
			...versionDescription,
		};
	}

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const topic = this.getNodeParameter('topic') as string;
		const options = this.getNodeParameter('options', {}) as KafkaTriggerV2Options;

		// A manual test run always resolves immediately, as in v1: the editor
		// discards the run once it has its sample, so waiting on an execution that
		// nothing will finish would hold the batch open until close.
		const isManualMode = this.getMode() === 'manual';
		const groupId = manualRunGroupId(this.getNodeParameter('groupId') as string, isManualMode);
		const resolveOffsetMode = isManualMode
			? 'immediately'
			: // Falls back to the field's own default rather than v1's 'immediately',
				// so an absent value keeps the at-least-once behaviour.
				(this.getNodeParameter('resolveOffset', 'onCompletion') as ResolveOffsetMode);
		const allowedStatuses = this.getNodeParameter('allowedStatuses', []) as string[];
		const { executionTimeout } = this.getWorkflowSettings();

		const credentials = await this.getCredentials<KafkaCredentials>('kafka');

		// Resolved before the consumer connects: a bad Schema Registry credential
		// must fail activation rather than leave a connected consumer behind. Shared
		// with v1: config/credential problems (NodeOperationError) fail activation
		// loudly, but a registry that is merely unreachable only logs a warning, so
		// a transient outage does not block the trigger from starting.
		const registry = await setSchemaRegistry(this);

		const parseMessage = createMessageParser(
			options,
			this.logger,
			registry,
			this.helpers.prepareBinaryData,
		);

		// Aborted before the consumer disconnects, so an execution the emitter is
		// still waiting on cannot hold teardown open.
		const closeController = new AbortController();
		const emit = createDataEmitter(
			this,
			toEmitterOptions(options, resolveOffsetMode, allowedStatuses, executionTimeout),
			closeController.signal,
		);

		let handle: KafkaConsumerHandle | undefined;

		const startConsumer = async () => {
			try {
				const consumer = await createKafkaConsumer(
					credentials,
					toConsumerOptions(
						// Read Messages From Beginning defaults to on, and a manual run's
						// group is brand new, so honouring it would replay the whole topic
						// into the editor. On an activated workflow the setting is moot
						// anyway, since the group already has a committed offset to resume
						// from. A test run waits for the next message instead.
						isManualMode ? { ...options, fromBeginning: false } : options,
						groupId,
						executionTimeout,
					),
					{
						logger: this.logger,
						// v1 routes non-restartable consumer crashes to emitError so n8n
						// re-activates the trigger. Errors caused by our own teardown are
						// not failures, so they stay quiet.
						onFatalError: (error) => {
							if (!closeController.signal.aborted) this.emitError(error);
						},
					},
				);
				handle = await consumeTopic(consumer, {
					topic,
					parseMessage,
					emit,
					logger: this.logger,
					batchSize: options.batchSize,
					partitionsConsumedConcurrently: options.partitionsConsumedConcurrently || undefined,
					errorRetryDelay: options.errorRetryDelay,
				});
			} catch (error) {
				throw new NodeOperationError(this.getNode(), error);
			}
		};

		const closeFunction = async () => {
			closeController.abort();
			try {
				await handle?.close();
			} catch (error) {
				// A disconnect that overruns its bound is reported the way v1 reports
				// teardown failures, rather than as an unattributed rejection. It happens
				// in practice: a consumer fenced by the processing deadline does not
				// disconnect within the bound.
				throw new TriggerCloseError(this.getNode(), {
					cause: ensureError(error),
					level: 'warning',
				});
			}
		};

		if (this.getMode() !== 'manual') {
			await startConsumer();
			return { closeFunction };
		}

		return {
			closeFunction,
			manualTriggerFunction: startConsumer,
		};
	}
}
