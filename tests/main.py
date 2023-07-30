import argparse
import logging
from time import sleep
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
TABLE_NAME = "messages"

LOG_FILENAME = "mqtt_connector.log"

class MQTTConnector:
	"""
	MQTTConnector class handles the MQTT connection and message handling.

	Args:
		db_host (str): Remote SQL database host address.
		db_port (int): Remote SQL database port.
		db_user (str): Remote SQL database username.
		db_password (str): Remote SQL database password.
		db_name (str): Remote SQL database name.
		table_name (str): Remote SQL table name.
	"""

	def __init__(self, db_host, db_port, db_user, db_password, db_name, table_name):
		self.db_host = db_host
		self.db_port = db_port
		self.db_user = db_user
		self.db_password = db_password
		self.db_name = db_name
		self.table_name = table_name
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
		"""
		Connects to the MQTT broker.

		Args:
			broker (str): Broker host address.
			port (int, optional): Broker port. Defaults to MQTT_BROKER_PORT.
		"""
		try:
			self.mqtt_client.connect(broker, port)
			self.mqtt_client.loop_start()
			self.logger.info("Connected to MQTT broker successfully.")
		except Exception as e:
			self.logger.error(f"Failed to connect to MQTT broker: {str(e)}")

	def disconnect(self):
		"""Disconnects from the MQTT broker."""
		self.mqtt_client.disconnect()
		self.mqtt_client.loop_stop()

	def on_message(self, client, userdata, message):
		"""
		Callback function for handling MQTT messages.

		Args:
			client (mqtt.Client): MQTT client instance.
			userdata: Userdata associated with the client.
			message (mqtt.MQTTMessage): Received MQTT message.
		"""
		self.logger.debug(f'Received message: {message.topic.split("/")[-1]} - {message.payload.decode()}')
		self.store_message(message.topic, message.payload.decode())

	def store_message(self, topic, message):
		"""
		Stores the received message in the remote SQL database.

		Args:
			topic (str): MQTT topic.
			message (str): MQTT message payload.
		"""
		if self.db_name is None:
			raise ValueError("Database name is not set.")
		if self.table_name is None:
			raise ValueError("Table name is not set.")

		# Parse the MQTT message
		try:
			data = json.loads(message)
			device_id = topic.split("/")[-4].strip()
			short_topic = topic.split("/")[-1].strip()
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
		cursor.execute(f"INSERT INTO {self.table_name} (timestamp, sequence, device_id, topic, data_item_id, value) VALUES (%s, %s, %s, %s, %s, %s)",
					   (timestamp, sequence, device_id, short_topic, data_item_id, value))
		conn.commit()
		cursor.close()
		conn.close()

		# Log the published message
		self.logger.info(f'Device ID: {device_id} - Topic: {short_topic} - Data Item ID: {data_item_id} - Sequence: {sequence} - Timestamp: {timestamp} - Value: {value}')

	def set_table_name(self, table_name):
		"""
		Sets the remote SQL table name.

		Args:
			table_name (str): Remote SQL table name.
		"""
		self.table_name = table_name
				
	def set_device_id(self, device_id):
		"""
		Sets the device ID.

		Args:
			device_id (str): Device ID.
		"""
		self.device_id = device_id

	def set_database_name(self, db_name):
		"""
		Sets the remote SQL database name.

		Args:
			db_name (str): Remote SQL database name.
		"""
		self.db_name = db_name

	def add_topic(self, topic):
		"""
		Adds a topic to subscribe to.

		Args:
			topic (str): MQTT topic to subscribe to.
		"""
		full_topic = f"MTConnect/Observation/{self.device_id}/Controller/Events/{topic}"
		if full_topic not in self.subscribed_topics:
			self.subscribed_topics.append(full_topic)
			self.logger.warning(f"Subscribing to topic: {full_topic}")
			self.mqtt_client.subscribe(full_topic)

	def start(self):
		"""Starts the MQTT-DB connector."""
		self.logger.info("Starting MQTT-DB connector...")
		self.mqtt_client.loop_start()


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
	parser.add_argument("--table-name", default=TABLE_NAME, help="Table name for storing messages")
	args = parser.parse_args()

	# Create a new instance of the MQTTConnector class
	connector = MQTTConnector(args.db_host, args.db_port, args.db_user, args.db_password, args.db_name, args.table_name)

	# Connect to the MQTT broker
	connector.connect(args.broker)

	# Set the device ID
	connector.set_device_id(args.device_id)

	# Set the database name
	connector.set_database_name(args.db_name)

	# Set the table name
	connector.set_table_name(args.table_name)

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
	cursor.execute(f"CREATE TABLE IF NOT EXISTS {args.db_name}.{args.table_name} (id INT AUTO_INCREMENT PRIMARY KEY, timestamp VARCHAR(255), sequence INT, device_id VARCHAR(255), topic VARCHAR(255), data_item_id VARCHAR(255), value VARCHAR(255))")
	cursor.close()
	conn.close()

	# Subscribe to MQTT topics
	connector.add_topic("ControllerMode")
	connector.add_topic("PartStatus")
	connector.add_topic("Execution")
	# Log the current list of subscribed topics
	connector.logger.debug(f"Subscribed topics: {connector.subscribed_topics}")

	try:
		# Start the MQTT connector
		connector.start()
		while True:
			sleep(1)
	except KeyboardInterrupt:
		connector.logger.info("Received ctrl+C; Stopping MQTT connector...")
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
