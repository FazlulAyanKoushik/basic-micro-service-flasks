import json
import pika
import requests
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///products.db'

# Initialize the database
db = SQLAlchemy(app)


# Define the Product model
class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    price = db.Column(db.Float, nullable=False)
    created_by = db.Column(db.Integer, nullable=False)  # Foreign key to User ID

    def __repr__(self):
        return f'<Product {self.name}>'

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'price': self.price
        }


# Create the database and tables
with app.app_context():
    db.create_all()


# Route to add a product
@app.route('/product', methods=['POST'])
def add_product():
    data = request.get_json()
    new_product = Product(name=data['name'], price=data['price'], created_by=data['created_by'])
    db.session.add(new_product)
    db.session.commit()
    return jsonify({"message": "Product added successfully!"}), 201


# Route to get all users from user_service
@app.route('/users', methods=['GET'])
def get_users():
    # URL of the user_service's /users endpoint
    user_service_url = 'http://localhost:5000/users'

    try:
        response = requests.get(user_service_url)
        response.raise_for_status()  # Will raise an exception for 4xx/5xx responses
        users = response.json().get('users', [])
        return jsonify({'users': users}), 200
    except requests.exceptions.RequestException as e:
        return jsonify({'message': f'Error fetching users: {str(e)}'}), 500


# Route to get all products from product_service
@app.route('/products', methods=['GET'])
def get_products():
    products = Product.query.all()
    product_list = [product.to_dict() for product in products]
    return jsonify({'products': product_list}), 200


# RabbitMQ consumer for responding to product requests
def product_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the 'product_request' queue and the 'product_response' queue with durable=True
    channel.queue_declare(queue='product_request', durable=True)
    channel.queue_declare(queue='product_response', durable=True)  # Ensure durability for 'product_response'

    def callback(ch, method, properties, body):
        user_id = int(body)

        with app.app_context():  # Ensure you're within the Flask app context
            products = Product.query.filter_by(created_by=user_id).all()
            product_list = [product.to_dict() for product in products]

            # Send the response back with product details
            response_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            response_channel = response_connection.channel()

            # Ensure the response queue is declared with durable=True to match the consumer
            response_channel.queue_declare(queue='product_response', durable=True)

            # Send the product list back to the user service via RabbitMQ
            response_channel.basic_publish(
                exchange='',
                routing_key='product_response',
                body=json.dumps(product_list)
            )
            response_connection.close()

    channel.basic_consume(queue='product_request', on_message_callback=callback, auto_ack=True)
    print('Waiting for product requests...')
    channel.start_consuming()


if __name__ == '__main__':
    import threading

    # Start the RabbitMQ consumer in a separate thread
    threading.Thread(target=product_consumer).start()
    app.run(debug=True, port=5001)
