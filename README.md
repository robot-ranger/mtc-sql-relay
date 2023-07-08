# MTConnect Agent-to-SQL Relay
A relay connector for scraping topics from an MQTT broker and storing them in a mySQL db

## Quick Start
`docker run -it -e MYSQL_ROOT_PASSWORD=password123. -e MYSQL_DATABASE=myDB -p 3306:3306 mysql`

This command creates a new mySQL Docker container, sets the root password to "password123.", creates a database named "myDB," and opens port 3306 on the container to port 3306 on the host machine.

`python main.py --broker 192.168.0.105 --device-id 0987654321 --db-host 172.18.0.1`

This command specifies a mqtt broker connection at `192.168.0.105`, sets target device id to `0987654321` and sets database host to `172.18.0.1`. All else are left DEFAULT. This command should connect to the db launched in docker above.

### Alternative: Launch VSCode Debugger

Launch debugger w/ included `./vscode/launch.json`

## reference mySQL config

Currently using `mysql.connector`. One convenient way to test is w/ mysql docker container. Launch it with the following:

`docker run -it -e MYSQL_ROOT_PASSWORD=password123. -e MYSQL_DATABASE=myDB -p 3306:3306 mysql`

This command creates a new Docker container with MySQL, sets the root password to "password123.", creates a database named "myDB," and maps port 3306 on the container to port 3306 on the host machine.

# Run it:
`python main.py --broker <broker_host> --device-id <device_id> --db-host <db_host> --db-port <db_port> --db-user <db_user> --db-password <db_password> --db-name <db_name>
`

## CLI Params

| Argument        | Description                                     | Default Value |
|-----------------|-------------------------------------------------|---------------|
| --broker        | Broker host address                             | 127.0.0.1     |
| --device-id     | Device ID (required)                            | None          |
| --db-host       | Remote SQL database host address                | 127.0.0.1     |
| --db-port       | Remote SQL database port                        | 3306          |
| --db-user       | Remote SQL database username                    | root          |
| --db-password   | Remote SQL database password                    | password123.  |
| --db-name       | Remote SQL database name                        | myDB          |
