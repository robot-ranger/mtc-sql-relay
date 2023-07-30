from typing import List, Optional
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import get_openapi
import paho.mqtt.client as mqtt
import argparse
import sqlite3
import mysql.connector
import json
import logging
import uvicorn

description = """
![img](/static/process_robotics.png)

## Publish & Subscribe to MQTT topics

Subscribe to MQTT topics and store the messages to a database.

"""

tags_metadata = [
	{
		"name": "mqtt client",
		"description": "Subscribe and Publish to MQTT topics."
	},
	{
		"name": "database",
		"description": "Configure the database settings."
	}
]

app = FastAPI(
	title="MQTT-to-DB Connector 🚀",
	description=description,
	version="0.1.0",
	contact={
		"name": "Process Robotics",
		"url": "https://processrobotics.com",
		"email": "mqtt2db@processrobtoics.com"
	},
)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default broker address and port
DEFAULT_BROKER_ADDRESS = "localhost"
DEFAULT_BROKER_PORT = 1883

# Default SQLite database file
DEFAULT_DB_FILE = "subscribed_topics.db"

# Default MySQL database settings
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 3306
DEFAULT_DB_USER = "root"
DEFAULT_DB_PASSWORD = "password123."
DEFAULT_DB_NAME = "myDB"
DEFAULT_TABLE_NAME = "messages"


class MQTTConnector:
	def __init__(self, broker_address: str, broker_port: int, db_file: str, db_host: str, db_port: int, db_user: str,
				 db_password: str, db_name: str):
		self.broker_address = broker_address
		self.broker_port = broker_port
		self.db_file = db_file
		self.db_host = db_host
		self.db_port = db_port
		self.db_user = db_user
		self.db_password = db_password
		self.db_name = db_name
		self.mqtt_client = mqtt.Client()
		self.conn = self.create_database_connection()

	def create_database_connection(self):
		conn = sqlite3.connect(self.db_file)
		cursor = conn.cursor()

		# Create the 'topics' table if it doesn't exist
		cursor.execute(
			"""
			CREATE TABLE IF NOT EXISTS topics (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				topic TEXT UNIQUE
			)
			"""
		)
		conn.commit()

		# Create the MySQL database table if it doesn't exist
		self.create_mysql_db_table()

		return conn

	def create_mysql_db_table(self):
		conn = mysql.connector.connect(
			host=self.db_host,
			port=self.db_port,
			user=self.db_user,
			password=self.db_password
		)
		cursor = conn.cursor()

		# Create the database if it doesn't exist
		cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
		cursor.execute(f"USE {self.db_name}")

		# Create the 'messages' table if it doesn't exist
		cursor.execute(
			f"""
			CREATE TABLE IF NOT EXISTS {DEFAULT_TABLE_NAME} (
				id INT AUTO_INCREMENT PRIMARY KEY,
				topic VARCHAR(255),
				timestamp VARCHAR(255),
				sequence VARCHAR(255),
				dataItemId VARCHAR(255),
				value VARCHAR(255)
			)
			"""
		)
		conn.commit()
		cursor.close()
		conn.close()

	def on_message(self, client, userdata, msg):
		# Parse the published message
		payload = msg.payload.decode()
		json_data = json.loads(payload)

		# Extract the elements (timestamp, sequence, dataItemId, value)
		timestamp = json_data.get('timestamp')
		sequence = json_data.get('sequence')
		data_item_id = json_data.get('dataItemId')
		value = json_data.get('value')

		# Log parsed message elements to remote MySQL database
		topic = msg.topic
		conn_mysql = mysql.connector.connect(
			host=self.db_host,
			port=self.db_port,
			user=self.db_user,
			password=self.db_password,
			database=self.db_name
		)
		cursor_mysql = conn_mysql.cursor()
		cursor_mysql.execute(
			f"""
			INSERT INTO {DEFAULT_TABLE_NAME} (topic, timestamp, sequence, dataItemId, value)
			VALUES (%s, %s, %s, %s, %s)
			""",
			(topic, timestamp, sequence, data_item_id, value)
		)
		conn_mysql.commit()
		cursor_mysql.close()
		conn_mysql.close()

		# Log the event
		logger.info(f"Received message on topic '{topic}': {payload}")

	def subscribe_topics(self, topics: List[str]):
		for topic in topics:
			result, _ = self.mqtt_client.subscribe(topic)
			if result == mqtt.MQTT_ERR_SUCCESS:
				logger.info(f"Subscribed to topic '{topic}'")
				# Add the topic to the 'topics' table
				cursor = self.conn.cursor()
				cursor.execute("INSERT OR IGNORE INTO topics (topic) VALUES (?)", (topic,))
				self.conn.commit()
				cursor.close()
			else:
				logger.warning(f"Failed to subscribe to topic '{topic}'")
		return {"message": "Subscribe Successful!"}

	def unsubscribe_topics(self, topics: List[str]):
		for topic in topics:
			result, _ = self.mqtt_client.unsubscribe(topic)
			if result == mqtt.MQTT_ERR_SUCCESS:
				logger.info(f"Unsubscribed from topic '{topic}'")
				# Remove the topic from the 'topics' table
				cursor = self.conn.cursor()
				cursor.execute("DELETE FROM topics WHERE topic = ?", (topic,))
				self.conn.commit()
				cursor.close()
			else:
				logger.warning(f"Failed to unsubscribe from topic '{topic}'")
		return {"message": "Unsubscribe Successful!"}

	def query_mqtt(self, topics: List[str], qos: int = 0):
		for topic in topics:
			self.mqtt_client.publish(topic, payload="", qos=qos, retain=False)
			logger.info(f"Sent query for topic '{topic}'")
		return {"message": "Query sent successfully!"}

	def get_subscribed_topics(self):
		cursor = self.conn.cursor()
		cursor.execute("SELECT topic FROM topics")
		result = cursor.fetchall()
		cursor.close()
		return [row[0] for row in result]


parser = argparse.ArgumentParser()
parser.add_argument("--broker-address", type=str, default=DEFAULT_BROKER_ADDRESS, help="MQTT broker address")
parser.add_argument("--broker-port", type=int, default=DEFAULT_BROKER_PORT, help="MQTT broker port")
parser.add_argument("--db-file", type=str, default=DEFAULT_DB_FILE, help="SQLite database file")
parser.add_argument("--db-host", type=str, default=DEFAULT_DB_HOST, help="MySQL database host")
parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help="MySQL database port")
parser.add_argument("--db-user", type=str, default=DEFAULT_DB_USER, help="MySQL database user")
parser.add_argument("--db-password", type=str, default=DEFAULT_DB_PASSWORD, help="MySQL database password")
parser.add_argument("--db-name", type=str, default=DEFAULT_DB_NAME, help="MySQL database name")
args = parser.parse_args()

broker_address = args.broker_address
broker_port = args.broker_port
db_file = args.db_file
db_host = args.db_host
db_port = args.db_port
db_user = args.db_user
db_password = args.db_password
db_name = args.db_name


mqtt_connector = MQTTConnector(broker_address, broker_port, db_file, db_host, db_port, db_user, db_password, db_name)


@app.on_event("startup")
async def startup_event():
	logger.info("Starting the application...")
	logger.info(f"Broker address: {broker_address}")
	logger.info(f"Broker port: {broker_port}")
	logger.info(f"Database file: {db_file}")
	logger.info(f"Database host: {db_host}")
	logger.info(f"Database user: {db_user}")
	logger.info(f"Database name: {db_name}")

	mqtt_connector.mqtt_client.connect(broker_address, broker_port, 60)
	mqtt_connector.mqtt_client.on_message = mqtt_connector.on_message
	mqtt_connector.mqtt_client.loop_start()
	logger.info("MQTT client connected")

	# Log the subscribed topics
	subscribed_topics = mqtt_connector.get_subscribed_topics()
	if subscribed_topics:
		logger.info("Subscribed topics:")
		for topic in subscribed_topics:
			logger.info(f"- {topic}")
	else:
		logger.info("No subscribed topics")


@app.on_event("shutdown")
async def shutdown_event():
	logger.info("Shutting down the application...")
	mqtt_connector.mqtt_client.loop_stop()
	mqtt_connector.conn.close()
	logger.info("MQTT client disconnected")

	# Drop the remote MySQL database
	conn_mysql = mysql.connector.connect(
		host=db_host,
		port=db_port,
		user=db_user,
		password=db_password,
		database=db_name
	)
	cursor_mysql = conn_mysql.cursor()
	cursor_mysql.execute(f"DROP DATABASE IF EXISTS {DEFAULT_DB_NAME}")
	conn_mysql.commit()
	cursor_mysql.close()
	conn_mysql.close()
	logger.info(f"Dropped remote MySQL database: {DEFAULT_DB_NAME}")


@app.post("/subscribe", tags=["mqtt client"])
async def subscribe_topics(topics: List[str] = Query(...)):
	"""Subscribe to MQTT topics. Must include full topic path, e.g. 'MTConnect/Observation/{device_id}/Controller/Events/Program'.

	Args:
		topics (List[str]): List of topics to subscribe to

	Returns:
		dict: Message confirming that the topics were subscribed to successfully!"""
	result = mqtt_connector.subscribe_topics(topics)
	topics = mqtt_connector.get_subscribed_topics()
	ret = {"result": result,"subscribed_topics": topics}
	return ret


@app.post("/unsubscribe", tags=["mqtt client"])
async def unsubscribe_topics(topics: List[str] = Query(...)):
	"""Unsubscribe from MQTT topics. Must include full topic path, e.g. 'MTConnect/Observation/{device_id}/Controller/Events/Program'.
	A topic can only be unsubscribed if it is in the 'topics' table.

	Args:
		topics (List[str]): List of topics to unsubscribe from

	Returns:
		dict: Message confirming that the topics were unsubscribed from successfully!"""
	result = mqtt_connector.unsubscribe_topics(topics)
	return result


@app.post("/query", tags=["mqtt client"])
async def query_topics(topics: List[str] = Query(...), qos: Optional[int] = Query(0)):
	"""Send a query for MQTT topics. Must include full topic path, e.g. 'MTConnect/Observation/{device_id}/Controller/Events/Program'.

	Args:
		topics (List[str]): List of topics to query

	Returns:
		dict: Message confirming that the query was sent successfully"""
	result = mqtt_connector.query_mqtt(topics, qos)
	return result


@app.post("/publish", tags=["mqtt client"])
async def publish_message(topic: str = Query(...), message: str = Query(...)):
	"""Publish a message on an MQTT topic. Must include full topic path, e.g. 'MTConnect/Observation/{device_id}/Controller/Events/Program'.

	Args:
		topic (str): Topic to publish to
		message (str): Message to publish

	Returns:
		dict: Message confirming that the message was published successfully"""
	mqtt_connector.mqtt_client.publish(topic, message)
	logger.info(f"Published message on topic '{topic}': {message}")
	return {"message": "Published successfully!"}


@app.get("/subscribed_topics", tags=["mqtt client"])
async def get_subscribed_topics():
	"""Get a list of subscribed topics.

	Returns:
		dict: dict of subscribed topics"""
	topics = mqtt_connector.get_subscribed_topics()
	return {"subscribed_topics": topics}


if __name__ == "__main__":
	try:
		uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True)
	except KeyboardInterrupt:
		logger.info("Keyboard interrupt received. Stopping the application...")
