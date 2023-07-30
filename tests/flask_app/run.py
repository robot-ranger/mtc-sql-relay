from flask import Flask, render_template, request, redirect

app = Flask(__name__)

# Sample data for the subscribed devices
devices = [
    "Device1",
    "Device2",
]

# Sample data for the configuration
config = {
    "broker_host": "mqtt.example.com",
    "db_host": "localhost",
    "db_port": 5432,
    "db_user": "admin",
    "db_password": "password",
    "db_name": "mydb",
    "table_name": "subscribed_devices",
}


@app.route('/')
def index():
    # Render the template with the subscribed devices and configuration
    return render_template('index.html', devices=devices, config=config)


@app.route('/add_device', methods=['POST'])
def add_device():
    # Retrieve the form data from the request
    device_id = request.form.get('device_id')

    # Process the form data (e.g., save to a database)
    devices.append(device_id)

    # Redirect back to the main page
    return redirect('/')


@app.route('/apply_config', methods=['POST'])
def apply_config():
    # Retrieve the form data from the request
    broker_host = request.form.get('broker_host')
    db_host = request.form.get('db_host')
    db_port = request.form.get('db_port')
    db_user = request.form.get('db_user')
    db_password = request.form.get('db_password')
    db_name = request.form.get('db_name')
    table_name = request.form.get('table_name')

    # Process the form data (e.g., save to a database)
    config.update({
        "broker_host": broker_host,
        "db_host": db_host,
        "db_port": db_port,
        "db_user": db_user,
        "db_password": db_password,
        "db_name": db_name,
        "table_name": table_name,
    })

    # Redirect back to the main page
    return redirect('/')


if __name__ == '__main__':
    app.run(debug=True)
