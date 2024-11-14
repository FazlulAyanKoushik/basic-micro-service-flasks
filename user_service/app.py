import json
import secrets
import pika
from decouple import config
from flask import Flask, request, jsonify, Response
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    jwt_required,
    get_jwt_identity
)
from flask_sqlalchemy import SQLAlchemy
from collections import OrderedDict

# print(secrets.token_hex(32))

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['JWT_SECRET_KEY'] = config('SECRET_KEY')

# Initialize the database
db = SQLAlchemy(app)

# Initialize JWT
jwt = JWTManager(app)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'

    def to_dict(self):
        return OrderedDict([
            ('id', self.id),
            ('email', self.email),
        ])


# Create the database and tables
with app.app_context():
    db.create_all()


@app.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    new_user = User(email=data['email'], password=data['password'])
    db.session.add(new_user)
    db.session.commit()
    return jsonify({'message': 'User created successfully'}), 201


@app.route('/users', methods=['GET'])
def get_users():
    users = User.query.all()
    user_dicts = [user.to_dict() for user in users]

    # Use json.dumps to ensure the key order in OrderedDict is preserved
    response_data = json.dumps({'users': user_dicts}, indent=4)
    return Response(response_data, mimetype='application/json')


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    user = User.query.filter_by(email=data['email']).first()
    if user and user.password == data['password']:
        access_token = create_access_token(identity=user.id)
        return jsonify({'access_token': access_token}), 200
    else:
        return jsonify({'message': 'Invalid email or password'}), 401


@app.route('/profile', methods=['GET'])
@jwt_required()
def profile():
    user_id = get_jwt_identity()
    user = User.query.get(user_id)
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the 'product_request' queue (already done)
    channel.queue_declare(queue='product_request', durable=True)

    # Send user id as a message to the queue
    channel.basic_publish(exchange='', routing_key='product_request', body=str(user_id))
    print("=" * 30)
    print("I am in basic_publish")
    print("=" * 30)

    # Close the connection to send the request
    connection.close()

    # Set up RabbitMQ connection to receive the response
    response_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    print("=" * 30)
    print("I am in localhost")
    print("=" * 30)
    response_channel = response_connection.channel()
    print("=" * 30)
    print("I am in response_channel")
    print("=" * 30)
    
    # Declare 'product_response' queue with 'durable=True' to match the producer
    response_channel.queue_declare(queue='product_response', durable=True)  # Ensures durability

    # Get the response from the 'product_response' queue
    method_frame, header_frame, body = response_channel.basic_get(queue='product_response', auto_ack=True)

    if body:
        product_list = json.loads(body)
    else:
        product_list = []

    response_connection.close()

    return jsonify({
        'email': user.email,
        "products": product_list
    })


if __name__ == '__main__':
    app.run(debug=True)
