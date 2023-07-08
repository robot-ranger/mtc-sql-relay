import argparse
import logging
import mysql.connector
import paho.mqtt.client as mqtt
import json

MQTT_BROKER_HOST = "127.0.0.1"
MQTT_BROKER_PORT = 1883
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "password123."
DB_NAME = "myDB"

LOG_FILENAME = "mqtt_connector.log"

class MQTTConnector:
	def __init__(self, db_host, db_port, db_user, db_password, db_name):
		self.db_host = db_host
		self.db_port = db_port
		self.db_user = db_user
		self.db_password = db_password
		self.db_name = db_name
		self.mqtt_client = mqtt.Client()
		self.mqtt_client.on_message = self.on_message
		self.device_id = ""
		self.subscribed_topics = []

		# Set up logging
		self.logger = logging.getLogger("MQTTConnector")
		self.logger.setLevel(logging.DEBUG)
		formatter = logging.Formatter("%(asctime)s - [%(lineno)d] - %(levelname)s - %(message)s")

		for handler in self.logger.handlers:
			self.logger.removeHandler(handler)

		# Log to file
		file_handler = logging.FileHandler(LOG_FILENAME)
		file_handler.setFormatter(formatter)
		self.logger.addHandler(file_handler)

		# Log to console
		console_handler = logging.StreamHandler()
		console_handler.setFormatter(formatter)
		self.logger.addHandler(console_handler)

	def connect(self, broker, port=MQTT_BROKER_PORT):
		try:
			self.mqtt_client.connect(broker, port)
			self.mqtt_client.loop_start()
			self.logger.info("Connected to MQTT broker successfully.")
		except Exception as e:
			self.logger.error(f"Failed to connect to MQTT broker: {str(e)}")

	def disconnect(self):
		self.mqtt_client.disconnect()
		self.mqtt_client.loop_stop()

	def on_message(self, client, userdata, message):
		# Store the received message in the remote SQL database
		self.logger.debug(f'Received message: {message.topic.split("/")[-1]} - {message.payload.decode()}')
		self.store_message(message.topic, message.payload.decode())

	def store_message(self, topic, message):
		if self.db_name is None:
			raise ValueError("Database name is not set.")

		# Parse the MQTT message
		try:
			data = json.loads(message)
			device_id = topic.split("/")[-4]
			short_topic = topic.split("/")[-1]
			data_item_id = data.get("dataItemId", "")
			sequence = data.get("sequence", 0)
			timestamp = data.get("timestamp", "")
			value = data.get("value", "")
		except json.JSONDecodeError as e:
			self.logger.error(f"Failed to parse MQTT message: {str(e)}")
			return

		conn = mysql.connector.connect(
			host=self.db_host,
			port=self.db_port,
			user=self.db_user,
			password=self.db_password,
			database=self.db_name
		)
		cursor = conn.cursor()
		cursor.execute("INSERT INTO messages (device_id, topic, data_item_id, sequence, timestamp, value) VALUES (%s, %s, %s, %s, %s, %s)",
					   (device_id, short_topic, data_item_id, sequence, timestamp, value))
		conn.commit()
		cursor.close()
		conn.close()

		# Log the published message
		self.logger.info(f'Device ID: {device_id} - Topic: {short_topic} - Data Item ID: {data_item_id} - Sequence: {sequence} - Timestamp: {timestamp} - Value: {value}')

	def set_device_id(self, device_id):
		self.device_id = device_id

	def set_database_name(self, db_name):
		self.db_name = db_name

	def add_topic(self, topic):
		full_topic = f"MTConnect/Observation/{self.device_id}/Controller/Events/{topic}"
		if full_topic not in self.subscribed_topics:  # Check if topic is already subscribed
			self.subscribed_topics.append(full_topic)
			self.logger.warning(f"Subscribing to topic: {full_topic}")
			self.mqtt_client.subscribe(full_topic)

	def start(self):
		self.logger.info("Starting MQTT-DB connector...")
		self.mqtt_client.loop_forever()


if __name__ == "__main__":
	# Command line argument parsing
	parser = argparse.ArgumentParser(description="MQTT Connector")
	parser.add_argument("--broker", default=MQTT_BROKER_HOST, help="Broker host address")
	parser.add_argument("--device-id", required=True, help="Device ID")
	parser.add_argument("--db-host", default=DB_HOST, help="Remote SQL database host address")
	parser.add_argument("--db-port", default=DB_PORT, type=int, help="Remote SQL database port")
	parser.add_argument("--db-user", default=DB_USER, help="Remote SQL database username")
	parser.add_argument("--db-password", default=DB_PASSWORD, help="Remote SQL database password")
	parser.add_argument("--db-name", default=DB_NAME, help="Remote SQL database name")
	args = parser.parse_args()

	# Create a new instance of the MQTTConnector class
	connector = MQTTConnector(args.db_host, args.db_port, args.db_user, args.db_password, args.db_name)

	# Connect to the MQTT broker
	connector.connect(args.broker)

	# Set the device ID
	connector.set_device_id(args.device_id)

	# Set the database name
	connector.set_database_name(args.db_name)

	# Create the remote database if it doesn't already exist
	conn = mysql.connector.connect(
		host=args.db_host,
		port=args.db_port,
		user=args.db_user,
		password=args.db_password
	)
	cursor = conn.cursor()
	cursor.execute(f"CREATE DATABASE IF NOT EXISTS {args.db_name}")
	## create table for storing messages if it doesn't already exist
	cursor.execute(f"CREATE TABLE IF NOT EXISTS {args.db_name}.messages (id INT AUTO_INCREMENT PRIMARY KEY, device_id VARCHAR(255), topic VARCHAR(255), data_item_id VARCHAR(255), sequence INT, timestamp VARCHAR(255), value VARCHAR(255))")
	cursor.close()
	conn.close()

	# Add subscribed MQTT topics
	connector.add_topic("ControllerMode")
	connector.add_topic("PartStatus")
	connector.add_topic("Execution")
	# Log the current list of subscribed topics
	connector.logger.debug(f"Subscribed topics: {connector.subscribed_topics}")

	try:
		# Start the MQTT connector
		connector.start()
	except KeyboardInterrupt:
		connector.logger.info("Stopping MQTT connector...")
		# Disconnect the MQTT connector
		connector.disconnect()

		# Remove the debugging database if it exists
		if connector.db_name == "myDB":
			conn = mysql.connector.connect(
				host=args.db_host,
				port=args.db_port,
				user=args.db_user,
				password=args.db_password
			)
			cursor = conn.cursor()
			cursor.execute(f"DROP DATABASE IF EXISTS {args.db_name}")
			connector.logger.info(f"Removed debugging database: {args.db_name}")
			cursor.close()
			conn.close()
			connector.logger.info("Goodbye.")
