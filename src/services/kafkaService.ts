import { Kafka, Producer, Consumer, KafkaConfig, logLevel, EachMessagePayload, ConsumerSubscribeTopics } from "kafkajs"
import * as vscode from "vscode"
import { logger } from "../utils/logging" // Note: Path needs verification
import { ClineProvider } from "../core/webview/ClineProvider"

// Kafka Configuration
const KAFKA_BROKER = "localhost:9092"
const KAFKA_PRODUCE_TOPIC = "message-queue"
const KAFKA_CONSUME_TOPIC = "message-incoming"
const KAFKA_CLIENT_ID = "roo-cline-extension"

// Kafka State
let kafka: Kafka | null = null
let producer: Producer | null = null
let consumer: Consumer | null = null
let isProducerConnecting = false
let isConsumerConnecting = false
let isProducerConnected = false
let isConsumerConnected = false
let activeClineProvider: ClineProvider | null = null // Store the provider instance

// Producer Config
function getKafkaProducerConfig(): KafkaConfig {
	return {
		clientId: KAFKA_CLIENT_ID,
		brokers: [KAFKA_BROKER],
		logLevel: logLevel.WARN,
		retry: {
			initialRetryTime: 300,
			retries: 5,
		},
	}
}

// Connect Producer
async function connectProducer(): Promise<void> {
	if (isProducerConnected || isProducerConnecting) return
	isProducerConnecting = true

	logger.debug("[KafkaService] Connecting Kafka producer...")

	try {
		if (!kafka) {
			kafka = new Kafka(getKafkaProducerConfig())
		}
		producer = kafka.producer()
		logger.info("[KafkaService] Attempting to connect producer...")
		await producer.connect()
		isProducerConnected = true
		isProducerConnecting = false
		logger.info("[KafkaService] Producer successfully connected.")

		logger.debug(`[KafkaService] Producer connected to ${KAFKA_BROKER}`)

		producer.on(producer.events.DISCONNECT, () => {
			logger.debug("[KafkaService] Kafka producer disconnected.")
			isProducerConnected = false
			producer = null
		})
	} catch (error) {
		isProducerConnecting = false
		isProducerConnected = false
		logger.error("[KafkaService] Failed to connect Kafka producer:", { error })
		console.error("[KafkaService] Failed to connect to Kafka producer:", error)
		vscode.window.showErrorMessage(`Failed to connect to Kafka at ${KAFKA_BROKER}.`)
	}
}

// Send Message
export async function sendMessageToKafka(messageContent: string): Promise<void> {
	if (!isProducerConnected) {
		await connectProducer()
	}

	if (!producer || !isProducerConnected) {
		logger.error("[KafkaService] Attempted to send message, but producer is not connected.")
		console.error("[KafkaService] Producer not connected.")
		return
	}

	try {
		logger.info(`[KafkaService] Sending message to topic "${KAFKA_PRODUCE_TOPIC}"...`)
		await producer.send({
			topic: KAFKA_PRODUCE_TOPIC,
			messages: [{ value: messageContent }],
		})
		logger.info(`[KafkaService] Message successfully sent to topic "${KAFKA_PRODUCE_TOPIC}".`)
	} catch (error) {
		logger.error("[KafkaService] Failed to send message:", { error })
		console.error("[KafkaService] Failed to send message:", error)
		vscode.window.showErrorMessage("Kafka send failed.")
	}
}

// Consumer Config
function getKafkaConsumerConfig(): KafkaConfig {
	return {
		clientId: `${KAFKA_CLIENT_ID}-consumer`,
		brokers: [KAFKA_BROKER],
		logLevel: logLevel.WARN,
		retry: {
			initialRetryTime: 300,
			retries: 5,
		},
	}
}

// Connect Consumer
async function connectConsumer(): Promise<void> {
	if (isConsumerConnected || isConsumerConnecting) return
	isConsumerConnecting = true

	logger.debug("[KafkaService] Connecting Kafka consumer...")

	try {
		if (!kafka) {
			kafka = new Kafka(getKafkaConsumerConfig())
		}
		consumer = kafka.consumer({
			groupId: `${KAFKA_CLIENT_ID}-group`,
			// Add session timeout and heartbeat interval for better rebalancing handling
			sessionTimeout: 60000, // 60 seconds (Increased timeout)
			heartbeatInterval: 3000, // 3 seconds
			// Add rebalance timeout to give more time for rebalancing
			rebalanceTimeout: 60000, // 60 seconds
			// Retry configuration for consumer
			retry: {
				initialRetryTime: 300,
				retries: 10,
				maxRetryTime: 30000,
				factor: 0.2, // Exponential backoff factor
			},
		})

		logger.info("[KafkaService] Attempting to connect consumer...")
		await consumer.connect()
		isConsumerConnected = true
		isConsumerConnecting = false
		logger.info("[KafkaService] Consumer successfully connected.")

		logger.debug(`[KafkaService] Consumer connected to ${KAFKA_BROKER}`)

		// Handle disconnect event
		consumer.on(consumer.events.DISCONNECT, () => {
			logger.debug("[KafkaService] Kafka consumer disconnected.")
			isConsumerConnected = false
			consumer = null

			// Attempt to reconnect after a short delay
			setTimeout(() => {
				if (!isConsumerConnected && !isConsumerConnecting) {
					logger.debug("[KafkaService] Attempting to reconnect consumer...")
					connectConsumer().then(() => {
						// Ensure startListening is called with the provider after reconnect
						if (isConsumerConnected && activeClineProvider) {
							startListening(activeClineProvider).catch((err) => {
								console.error("[KafkaService] Failed to restart listening after reconnect:", err)
							})
						} else if (!activeClineProvider) {
							logger.error(
								"[KafkaService] Cannot restart listening after reconnect: ClineProvider instance is missing.",
							)
						}
					})
				}
			}, 5000) // Wait 5 seconds before reconnecting
		})

		// Handle rebalancing events
		consumer.on(consumer.events.GROUP_JOIN, () => {
			logger.debug("[KafkaService] Consumer joined group.")
		})

		consumer.on(consumer.events.REBALANCING, () => {
			logger.debug("[KafkaService] Consumer group is rebalancing...")
		})

		consumer.on(consumer.events.CRASH, (event) => {
			console.error("[KafkaService] Consumer crashed:", event.payload.error)
			logger.debug("[KafkaService] Consumer crashed, will attempt to reconnect")
			isConsumerConnected = false

			// Attempt to recover from crash
			setTimeout(() => {
				if (!isConsumerConnected && !isConsumerConnecting) {
					logger.debug("[KafkaService] Attempting to recover from crash...")
					connectConsumer().then(() => {
						// Ensure startListening is called with the provider after crash recovery
						if (isConsumerConnected && activeClineProvider) {
							startListening(activeClineProvider).catch((err) => {
								console.error("[KafkaService] Failed to restart listening after crash:", err)
							})
						} else if (!activeClineProvider) {
							logger.error(
								"[KafkaService] Cannot restart listening after crash: ClineProvider instance is missing.",
							)
						}
					})
				}
			}, 5000) // Wait 5 seconds before reconnecting
		})
	} catch (error) {
		isConsumerConnecting = false
		isConsumerConnected = false
		logger.error("[KafkaService] Failed to connect Kafka consumer:", { error })
		console.error("[KafkaService] Failed to connect Kafka consumer:", error)
		vscode.window.showErrorMessage(`Failed to connect Kafka consumer at ${KAFKA_BROKER}.`)
	}
}

// Handle Incoming Kafka Messages
async function handleKafkaMessage(payload: EachMessagePayload): Promise<void> {
	const { topic, partition, message } = payload
	const messageText = message.value?.toString()

	logger.info(`[KafkaService] Received raw message from topic "${topic}" [${partition}]`)

	if (!messageText) {
		logger.warn("[KafkaService] Empty message received, skipping.")
		return
	}

	logger.debug(`[KafkaService] Processing message content: ${messageText.substring(0, 100)}...`)

	try {
		// Use the stored provider instance
		const provider = activeClineProvider
		logger.info(
			`[KafkaService] Using stored ClineProvider instance: ${provider ? "Instance available" : "Instance MISSING"}`,
		)

		if (provider) {
			logger.info("[KafkaService] Active ClineProvider found. Attempting to forward message to webview...")
			logger.debug(`[KafkaService] Posting 'setChatBoxMessage' with text: ${messageText.substring(0, 50)}...`)
			await provider.postMessageToWebview({
				type: "invoke",
				invoke: "setChatBoxMessage",
				text: messageText,
			})
			logger.debug("[KafkaService] Successfully posted 'setChatBoxMessage'.")
			logger.debug(`[KafkaService] Posting 'sendMessage' with text: ${messageText.substring(0, 50)}...`)
			await provider.postMessageToWebview({
				type: "invoke",
				invoke: "sendMessage",
				text: messageText,
			})
			logger.info("[KafkaService] Successfully forwarded 'sendMessage' to webview.")
		} else {
			logger.warn("[KafkaService] No active ClineProvider found. Cannot forward message.")
		}
	} catch (error) {
		logger.error("[KafkaService] Error handling Kafka message:", { error })
		console.error("[KafkaService] Error handling Kafka message:", error)
	}
}

// Start Listening to Incoming Messages
export async function startListening(providerInstance: ClineProvider): Promise<void> {
	if (!providerInstance) {
		logger.error("[KafkaService] startListening called without a valid ClineProvider instance.")
		return
	}
	activeClineProvider = providerInstance // Store the passed instance
	// Maximum number of retries
	const maxRetries = 3
	let retryCount = 0
	let success = false

	while (retryCount < maxRetries && !success) {
		if (!isConsumerConnected) {
			try {
				await connectConsumer()
			} catch (error) {
				console.error(
					`[KafkaService] Failed to connect consumer (attempt ${retryCount + 1}/${maxRetries}):`,
					error,
				)
				retryCount++

				if (retryCount < maxRetries) {
					logger.debug(`[KafkaService] Retrying connection in 5 seconds...`)
					await new Promise((resolve) => setTimeout(resolve, 5000)) // Wait 5 seconds before retrying
				}
				continue
			}
		}

		if (!consumer || !isConsumerConnected) {
			logger.error("[KafkaService] Attempted to start listening, but consumer is not connected.")
			console.error("[KafkaService] Kafka consumer not connected.")
			retryCount++

			if (retryCount < maxRetries) {
				logger.debug(`[KafkaService] Retrying connection in 5 seconds...`)
				await new Promise((resolve) => setTimeout(resolve, 5000)) // Wait 5 seconds before retrying
			}
			continue
		}

		try {
			// Unsubscribe from any existing topics first to avoid duplicate subscriptions
			// This is important after rebalancing events
			try {
				await consumer.stop()
				logger.debug("[KafkaService] Stopped existing consumer run if any")
			} catch (stopError) {
				// Ignore errors from stopping - it might not be running
				logger.debug("[KafkaService] No existing consumer run to stop")
			}

			const topics: ConsumerSubscribeTopics = {
				topics: [KAFKA_CONSUME_TOPIC],
				fromBeginning: false,
			}

			await consumer.subscribe(topics)
			logger.debug(`[KafkaService] Subscribed to topic: ${KAFKA_CONSUME_TOPIC}`)

			await consumer.run({
				eachMessage: handleKafkaMessage,
				// Auto-commit configuration
				autoCommit: true,
				autoCommitInterval: 5000, // 5 seconds
				// Error handling for eachBatch
				eachBatchAutoResolve: true,
				// Process one partition at a time for simplicity
				partitionsConsumedConcurrently: 1,
			})

			logger.info("[KafkaService] Consumer started listening successfully.")

			// Add heartbeat check interval
			const heartbeatInterval = setInterval(() => {
				if (!consumer || !isConsumerConnected) {
					clearInterval(heartbeatInterval)
					return
				}

				logger.debug("[KafkaService] Checking consumer connection status...")
				// If we detect issues, we'll rely on the event handlers to reconnect
			}, 60000) // Check every minute

			success = true
		} catch (error) {
			logger.error(`[KafkaService] Error starting consumer listener (attempt ${retryCount + 1}/${maxRetries}):`, {
				error,
			})
			console.error(`[KafkaService] Error starting consumer (attempt ${retryCount + 1}/${maxRetries}):`, error)
			retryCount++

			if (retryCount >= maxRetries) {
				vscode.window.showErrorMessage("Kafka consumer run failed after multiple attempts.")
			} else {
				logger.debug(`[KafkaService] Retrying in 5 seconds...`)
				await new Promise((resolve) => setTimeout(resolve, 5000)) // Wait 5 seconds before retrying
			}
		}
	}

	// If we couldn't start the consumer after all retries
	if (!success) {
		logger.error("[KafkaService] Failed to start Kafka consumer listener after multiple attempts.")
		console.error("[KafkaService] Failed to start Kafka consumer after multiple attempts")
		// Reset consumer state to allow future attempts
		if (consumer) {
			try {
				await consumer.disconnect()
			} catch (error) {
				// Ignore disconnect errors
			}
			consumer = null
			isConsumerConnected = false
			isConsumerConnecting = false
		}
	}
}

// Stop Listening
export async function stopListening(): Promise<void> {
	if (consumer && isConsumerConnected) {
		try {
			logger.debug("[KafkaService] Stopping consumer...")
			await consumer.stop()
			await consumer.disconnect()
			logger.info("[KafkaService] Consumer stopped successfully.")
		} catch (error) {
			logger.error("[KafkaService] Error stopping consumer:", { error })
			console.error("[KafkaService] Error stopping consumer:", error)
		} finally {
			consumer = null
			isConsumerConnected = false
			isConsumerConnecting = false
		}
	}
}

// Disconnect Both Producer and Consumer
export async function disconnectKafka(): Promise<void> {
	if (producer) {
		try {
			logger.debug("[KafkaService] Disconnecting producer...")
			await producer.disconnect()
			logger.info("[KafkaService] Producer disconnected successfully.")
		} catch (error) {
			logger.error("[KafkaService] Error disconnecting producer:", { error })
			console.error("[KafkaService] Error disconnecting producer:", error)
		} finally {
			producer = null
			isProducerConnected = false
			isProducerConnecting = false
		}
	}

	await stopListening()

	if (!producer && !consumer) {
		kafka = null
		logger.debug("[KafkaService] Kafka instance cleared.")
	}
}
