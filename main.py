import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
from datetime import datetime, timedelta
import uuid
import logging
import hashlib

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Table names
users_table_name = 'users'
stock_table_name = 'stock'
transactions_table_name = 'stock_transactions'
production_table_name = 'production'
push_production_table_name = 'push_to_production'


def wait_for_table_creation(table_name):
    """
    Helper function that waits for a DynamoDB table to become active.
    """
    try:
        table = dynamodb.Table(table_name)
        logger.info(f"Waiting for table '{table_name}' to be active...")
        table.wait_until_exists()
        logger.info(f"Table '{table_name}' is now active.")
    except Exception as e:
        logger.error(f"Error while waiting for table '{table_name}': {str(e)}")


def initialize_tables():
    """
    Checks if required DynamoDB tables exist; if not, creates them.
    """
    try:
        existing_tables = dynamodb.meta.client.list_tables()['TableNames']
        logger.info(f"Existing tables: {existing_tables}")

        # 1) users
        if users_table_name not in existing_tables:
            logger.info(f"Creating '{users_table_name}' table...")
            dynamodb.create_table(
                TableName=users_table_name,
                KeySchema=[
                    {'AttributeName': 'username', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'username', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(users_table_name)

        # 2) stock
        if stock_table_name not in existing_tables:
            logger.info(f"Creating '{stock_table_name}' table...")
            dynamodb.create_table(
                TableName=stock_table_name,
                KeySchema=[
                    {'AttributeName': 'item_id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'item_id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(stock_table_name)

        # 3) stock_transactions
        if transactions_table_name not in existing_tables:
            logger.info(f"Creating '{transactions_table_name}' table...")
            dynamodb.create_table(
                TableName=transactions_table_name,
                KeySchema=[
                    {'AttributeName': 'transaction_id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'transaction_id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(transactions_table_name)

        # 4) production
        if production_table_name not in existing_tables:
            logger.info(f"Creating '{production_table_name}' table...")
            dynamodb.create_table(
                TableName=production_table_name,
                KeySchema=[
                    {'AttributeName': 'product_id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'product_id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(production_table_name)

        # 5) push_to_production
        if push_production_table_name not in existing_tables:
            logger.info(f"Creating '{push_production_table_name}' table...")
            dynamodb.create_table(
                TableName=push_production_table_name,
                KeySchema=[
                    {'AttributeName': 'push_id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'push_id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(push_production_table_name)

    except ClientError as e:
        logger.error(f"Error creating tables: {e.response['Error']['Message']}")
    except Exception as e:
        logger.error(f"Unexpected error in initialize_tables: {str(e)}")


class DecimalEncoder(json.JSONEncoder):
    """
    Helper class to convert a DynamoDB item to JSON, handling Decimal type.
    """
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def log_transaction(operation_type, details, username):
    """
    Generic logger for all operations (except admin operations).
    Stores them in 'stock_transactions' with Indian Standard Time (IST).
    """
    try:
        transaction_id = str(uuid.uuid4())

        # Convert current UTC time to IST (UTC+5:30)
        timestamp_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        timestamp_str = timestamp_ist.isoformat()
        date_str = timestamp_ist.strftime("%Y-%m-%d")

        # Convert any float in 'details' to Decimal
        for key in list(details.keys()):
            if isinstance(details[key], float):
                details[key] = Decimal(str(details[key]))

        details['username'] = username

        transactions_table = dynamodb.Table(transactions_table_name)
        transactions_table.put_item(
            Item={
                'transaction_id': transaction_id,
                'operation_type': operation_type,
                'details': details,
                'date': date_str,
                'timestamp': timestamp_str
            }
        )
        logger.info(f"Transaction logged: {operation_type}, ID: {transaction_id}")

    except ClientError as e:
        logger.error(f"Error logging transaction: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in log_transaction: {e}")


# =============================================================================
# ================ ADMIN OPERATIONS (No Transaction Logs) =====================
# =============================================================================

def admin_auth_check(body):
    """
    Helper to validate admin username/password. 
    Returns True if 'username' == 'admin' and 'password' == '37773'; otherwise False.
    """
    return (body.get('username') == 'admin' and body.get('password') == '37773')


def delete_transaction_data(body):
    """
    ADMIN-ONLY: Deletes all data from 'stock_transactions'.
    No transaction log for admin operations.
    """
    try:
        if not admin_auth_check(body):
            return {
                "statusCode": 403,
                "body": json.dumps({"error": "Unauthorized: Admin credentials required."})
            }

        transactions_table = dynamodb.Table(transactions_table_name)
        # Scan all items, then delete in a loop
        scan_resp = transactions_table.scan()
        items = scan_resp.get('Items', [])

        with transactions_table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={'transaction_id': item['transaction_id']})

        # If you want to keep deleting in pages until 'LastEvaluatedKey' no longer exists, do so:
        while 'LastEvaluatedKey' in scan_resp:
            scan_resp = transactions_table.scan(ExclusiveStartKey=scan_resp['LastEvaluatedKey'])
            items = scan_resp.get('Items', [])
            with transactions_table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'transaction_id': item['transaction_id']})

        logger.info("All transaction data deleted by admin.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "All transaction data deleted."})
        }

    except Exception as e:
        logger.error(f"Error in delete_transaction_data: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def admin_view_users(body):
    """
    ADMIN-ONLY: View all users from 'users' table.
    No transaction logs for this operation.
    """
    try:
        if not admin_auth_check(body):
            return {
                "statusCode": 403,
                "body": json.dumps({"error": "Unauthorized: Admin credentials required."})
            }

        users_table = dynamodb.Table(users_table_name)
        scan_resp = users_table.scan()
        items = scan_resp.get('Items', [])

        # Keep scanning if needed
        while 'LastEvaluatedKey' in scan_resp:
            scan_resp = users_table.scan(ExclusiveStartKey=scan_resp['LastEvaluatedKey'])
            items.extend(scan_resp.get('Items', []))

        logger.info("Admin viewed all users.")
        return {
            "statusCode": 200,
            "body": json.dumps(items, cls=DecimalEncoder)
        }

    except Exception as e:
        logger.error(f"Error in admin_view_users: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def admin_update_user(body):
    """
    ADMIN-ONLY: Update user details in 'users' table (e.g., password).
    No transaction logs for this operation.
    Example payload:
    {
      "operation": "AdminUpdateUser",
      "admin_username": "admin",
      "admin_password": "37773",
      "username_to_update": "john_doe",
      "new_password": "someNewPass"
    }
    """
    try:
        if not admin_auth_check(body):
            return {
                "statusCode": 403,
                "body": json.dumps({"error": "Unauthorized: Admin credentials required."})
            }

        if 'username_to_update' not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "'username_to_update' is required"})
            }

        username_to_update = body['username_to_update']
        new_password = body.get('new_password')

        users_table = dynamodb.Table(users_table_name)
        user_resp = users_table.get_item(Key={'username': username_to_update})
        if 'Item' not in user_resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"User '{username_to_update}' not found."})
            }

        # Let's say we only allow password update for simplicity:
        if new_password:
            hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
            users_table.update_item(
                Key={'username': username_to_update},
                UpdateExpression="SET #p = :pw",
                ExpressionAttributeNames={"#p": "password"},
                ExpressionAttributeValues={":pw": hashed_password}
            )

        logger.info(f"Admin updated user '{username_to_update}'.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"User '{username_to_update}' updated successfully."})
        }

    except Exception as e:
        logger.error(f"Error in admin_update_user: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


# =============================================================================
# ================ USER MANAGEMENT (Register, Login) ===========================
# =============================================================================

def register_user(body):
    """
    Register a new user in 'users'.
    """
    try:
        if 'username' not in body or 'password' not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "'username' and 'password' are required."})
            }

        username = body['username']
        password = body['password']

        users_table = dynamodb.Table(users_table_name)
        existing_user = users_table.get_item(Key={'username': username}).get('Item')
        if existing_user:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "User already exists."})
            }

        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        users_table.put_item(
            Item={
                'username': username,
                'password': hashed_password
            }
        )
        logger.info(f"New user registered: {username}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "User registered successfully."})
        }
    except Exception as e:
        logger.error(f"Error registering user: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def login_user(body):
    """
    Login by checking hashed password in 'users'.
    """
    try:
        if 'username' not in body or 'password' not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "'username' and 'password' are required."})
            }

        username = body['username']
        password = body['password']
        users_table = dynamodb.Table(users_table_name)

        user_item = users_table.get_item(Key={'username': username}).get('Item')
        if not user_item:
            return {
                "statusCode": 401,
                "body": json.dumps({"error": "Invalid username or password."})
            }

        hashed_input_password = hashlib.sha256(password.encode()).hexdigest()
        if hashed_input_password != user_item['password']:
            return {
                "statusCode": 401,
                "body": json.dumps({"error": "Invalid username or password."})
            }

        logger.info(f"User logged in: {username}")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Login successful."})
        }
    except Exception as e:
        logger.error(f"Error logging in user: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


# =============================================================================
# ====================== STOCK (Create, Update, etc.) =========================
# =============================================================================

def create_stock(body):
    """
    Create a new stock item in 'stock'.
    """
    try:
        required = ['name', 'quantity', 'defective', 'cost_per_unit', 'stock_limit', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        item_id = body['name']
        quantity = int(body['quantity'])
        defective = int(body['defective'])
        cost_per_unit = Decimal(str(body['cost_per_unit']))
        stock_limit = int(body['stock_limit'])
        username = body['username']

        if quantity < 0 or defective < 0 or defective > quantity:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid quantity or defective values"})
            }

        available_quantity = quantity - defective
        total_cost = available_quantity * cost_per_unit

        stock_table = dynamodb.Table(stock_table_name)
        existing_item = stock_table.get_item(Key={'item_id': item_id}).get('Item')
        if existing_item:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Stock with this name already exists"})
            }

        # Insert new stock
        stock_table.put_item(
            Item={
                'item_id': item_id,
                'name': item_id,
                'quantity': available_quantity,
                'cost_per_unit': cost_per_unit,
                'total_cost': total_cost,
                'stock_limit': stock_limit,
                'defective': defective,
                'total_quantity': quantity,
                'username': username
            }
        )

        # Log transaction
        log_transaction(
            "CreateStock",
            {
                'item_id': item_id,
                'available_quantity': available_quantity,
                'defective': defective,
                'quantity': quantity,
                'cost_per_unit': float(cost_per_unit),
                'total_cost': float(total_cost),
                'stock_limit': stock_limit
            },
            username
        )

        logger.info(f"Stock created by {username}: {item_id}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Stock created successfully.",
                "name": item_id,
                "quantity": available_quantity,
                "defective": defective,
                "total_quantity": quantity,
                "cost_per_unit": float(cost_per_unit),
                "total_cost": float(total_cost),
                "stock_limit": stock_limit,
                "username": username
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in create_stock: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def update_stock(body):
    """
    Update an existing stock item.
    """
    try:
        required = ['name', 'quantity', 'defective', 'cost_per_unit', 'stock_limit', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        item_id = body['name']
        new_quantity = int(body['quantity'])
        defective = int(body['defective'])
        new_cost_per_unit = Decimal(str(body['cost_per_unit']))
        stock_limit = int(body['stock_limit'])
        username = body['username']

        available_quantity = new_quantity - defective
        new_total_cost = available_quantity * new_cost_per_unit

        stock_table = dynamodb.Table(stock_table_name)
        existing_item = stock_table.get_item(Key={'item_id': item_id}).get('Item')
        if not existing_item:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Stock item not found"})
            }

        old_quantity = existing_item['quantity']
        old_cost_per_unit = existing_item['cost_per_unit']

        stock_table.update_item(
            Key={'item_id': item_id},
            UpdateExpression="""
                SET quantity = :quantity,
                    cost_per_unit = :cost_per_unit,
                    total_cost = :total_cost,
                    stock_limit = :stock_limit,
                    defective = :defective,
                    total_quantity = :total_quantity
            """,
            ExpressionAttributeValues={
                ':quantity': available_quantity,
                ':cost_per_unit': new_cost_per_unit,
                ':total_cost': new_total_cost,
                ':stock_limit': stock_limit,
                ':defective': defective,
                ':total_quantity': new_quantity
            }
        )

        # Log update
        log_transaction(
            "UpdateStock",
            {
                'item_id': item_id,
                'old_quantity': old_quantity,
                'new_quantity': available_quantity,
                'defective': defective,
                'total_quantity': new_quantity,
                'old_cost_per_unit': float(old_cost_per_unit),
                'new_cost_per_unit': float(new_cost_per_unit),
                'new_total_cost': float(new_total_cost),
                'stock_limit': stock_limit
            },
            username
        )

        logger.info(f"Stock updated by {username}: {item_id}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Stock updated successfully.",
                "name": item_id,
                "quantity": available_quantity,
                "defective": defective,
                "total_quantity": new_quantity,
                "cost_per_unit": float(new_cost_per_unit),
                "total_cost": float(new_total_cost),
                "stock_limit": stock_limit,
                "username": username
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in update_stock: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def delete_stock(body):
    """
    Delete a stock item.
    """
    try:
        required = ['name', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        name = body['name']
        username = body['username']

        stock_table = dynamodb.Table(stock_table_name)
        response = stock_table.get_item(Key={'item_id': name})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Stock item '{name}' not found."})
            }

        stock_table.delete_item(Key={'item_id': name})

        # Log transaction
        log_transaction(
            "DeleteStock",
            {
                "item_id": name,
                "details": f"Stock '{name}' deleted"
            },
            username
        )

        logger.info(f"Stock '{name}' deleted by {username}.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Stock '{name}' deleted successfully."})
        }

    except Exception as e:
        logger.error(f"Error in delete_stock: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def get_all_stocks(body):
    """
    Get all stock items.
    """
    try:
        username = body.get('username', 'Unknown')
        stock_table = dynamodb.Table(stock_table_name)

        items = []
        response = stock_table.scan()
        items.extend(response['Items'])
        while 'LastEvaluatedKey' in response:
            response = stock_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        logger.info(f"User '{username}' retrieved all stock items.")
        return {
            "statusCode": 200,
            "body": json.dumps(items, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_all_stocks: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


# =============================================================================
# ======================= PRODUCTION (Product CRUD) ===========================
# =============================================================================

def create_product(body):
    """
    Create a new product in 'production'.
    Example:
      {
        "operation": "CreateProduct",
        "product_name": "CoolGadget",
        "stock_needed": {
          "ItemA": 2,
          "ItemB": 1
        },
        "username": "john_doe"
      }
    Automatically calculates how many units can be produced with current stock.
    """
    try:
        required = ['product_name', 'stock_needed', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        product_name = body['product_name']
        stock_needed = body['stock_needed']  # dict of {stock_item: qty_per_unit}
        username = body['username']

        product_id = str(uuid.uuid4())
        stock_table = dynamodb.Table(stock_table_name)

        # Calculate max_produce
        max_produce = None
        for item_id, qty_needed_str in stock_needed.items():
            qty_needed_each = Decimal(str(qty_needed_str))

            # fetch stock
            resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' not in resp:
                # That stock doesn't exist => can't produce
                max_produce = 0
                break
            available_qty = Decimal(str(resp['Item']['quantity']))

            possible = available_qty // qty_needed_each
            if max_produce is None or possible < max_produce:
                max_produce = possible

        if max_produce is None:
            max_produce = 0

        production_table = dynamodb.Table(production_table_name)
        production_table.put_item(
            Item={
                'product_id': product_id,
                'product_name': product_name,
                'stock_needed': stock_needed,
                'max_produce': int(max_produce),
                'username': username
            }
        )

        # Log
        log_transaction(
            "CreateProduct",
            {
                "product_id": product_id,
                "product_name": product_name,
                "stock_needed": stock_needed,
                "max_produce": int(max_produce)
            },
            username
        )

        logger.info(f"Product created: {product_name} (ID: {product_id}), max produce={max_produce}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Product created successfully.",
                "product_id": product_id,
                "product_name": product_name,
                "stock_needed": stock_needed,
                "max_produce": int(max_produce)
            }, cls=DecimalEncoder)
        }

    except Exception as e:
        logger.error(f"Error in create_product: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def update_product(body):
    """
    Update product fields (name, stock_needed). Recalculate max_produce if stock_needed changed.
    """
    try:
        required = ['product_id', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        product_id = body['product_id']
        username = body['username']

        production_table = dynamodb.Table(production_table_name)
        product_response = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_response:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Product '{product_id}' not found."})
            }

        product_item = product_response['Item']
        new_product_name = body.get('product_name', product_item.get('product_name'))
        new_stock_needed = body.get('stock_needed', product_item.get('stock_needed'))

        # If stock_needed changed, recalc max_produce
        if 'stock_needed' in body:
            stock_table = dynamodb.Table(stock_table_name)
            max_produce = None

            for item_id, qty_needed_str in new_stock_needed.items():
                qty_needed = Decimal(str(qty_needed_str))
                resp = stock_table.get_item(Key={'item_id': item_id})
                if 'Item' not in resp:
                    max_produce = 0
                    break
                available_qty = Decimal(str(resp['Item']['quantity']))
                possible = available_qty // qty_needed
                if max_produce is None or possible < max_produce:
                    max_produce = possible

            if max_produce is None:
                max_produce = 0
        else:
            max_produce = product_item.get('max_produce', 0)

        production_table.update_item(
            Key={'product_id': product_id},
            UpdateExpression="""
                SET product_name = :pn,
                    stock_needed = :sn,
                    max_produce = :mp
            """,
            ExpressionAttributeValues={
                ':pn': new_product_name,
                ':sn': new_stock_needed,
                ':mp': int(max_produce)
            }
        )

        # Log
        log_transaction(
            "UpdateProduct",
            {
                "product_id": product_id,
                "new_product_name": new_product_name,
                "new_stock_needed": new_stock_needed,
                "max_produce": int(max_produce)
            },
            username
        )

        logger.info(f"Product updated: {product_id}, new max_produce={max_produce}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Product updated successfully.",
                "product_id": product_id,
                "product_name": new_product_name,
                "stock_needed": new_stock_needed,
                "max_produce": int(max_produce)
            }, cls=DecimalEncoder)
        }

    except Exception as e:
        logger.error(f"Error in update_product: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def delete_product(body):
    """
    Delete a product from 'production'.
    """
    try:
        required = ['product_id', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        product_id = body['product_id']
        username = body['username']

        production_table = dynamodb.Table(production_table_name)
        product_resp = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Product '{product_id}' not found."})
            }

        production_table.delete_item(Key={'product_id': product_id})

        # Log
        log_transaction(
            "DeleteProduct",
            {
                "product_id": product_id,
                "details": f"Product '{product_id}' deleted"
            },
            username
        )

        logger.info(f"Product '{product_id}' deleted by {username}.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Product '{product_id}' deleted successfully."
            })
        }
    except Exception as e:
        logger.error(f"Error in delete_product: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def get_all_products(body):
    """
    Retrieve all products from the 'production' table.
    """
    try:
        username = body.get('username', 'Unknown')
        production_table = dynamodb.Table(production_table_name)

        items = []
        response = production_table.scan()
        items.extend(response['Items'])
        while 'LastEvaluatedKey' in response:
            response = production_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        logger.info(f"User '{username}' retrieved all products.")
        return {
            "statusCode": 200,
            "body": json.dumps(items, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_all_products: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


# =============================================================================
# =========== PUSH TO PRODUCTION & UNDO (push_to_production) ==================
# =============================================================================

def recalc_max_produce(product_id):
    """
    Recalculate the max_produce in 'production' table based on current stock.
    This is called after stock changes (like after push_to_production).
    """
    production_table = dynamodb.Table(production_table_name)
    stock_table = dynamodb.Table(stock_table_name)

    product_resp = production_table.get_item(Key={'product_id': product_id})
    if 'Item' not in product_resp:
        return  # can't recalc if product doesn't exist

    prod_item = product_resp['Item']
    stock_needed = prod_item['stock_needed']
    max_produce = None

    for item_id, qty_str in stock_needed.items():
        qty_needed = Decimal(str(qty_str))
        stock_resp = stock_table.get_item(Key={'item_id': item_id})
        if 'Item' not in stock_resp:
            # no stock => can't produce any
            max_produce = 0
            break
        available_qty = Decimal(str(stock_resp['Item']['quantity']))
        possible = available_qty // qty_needed
        if max_produce is None or possible < max_produce:
            max_produce = possible

    if max_produce is None:
        max_produce = 0

    # update the product record
    production_table.update_item(
        Key={'product_id': product_id},
        UpdateExpression="SET max_produce = :mp",
        ExpressionAttributeValues={':mp': int(max_produce)}
    )


def push_to_production(body):
    """
    Produce a certain quantity of a product, deducting stock usage, logging in 'push_to_production'.
    Then recalc max_produce in 'production'.
    """
    try:
        required = ['product_id', 'quantity', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        product_id = body['product_id']
        quantity_to_produce = int(body['quantity'])
        username = body['username']

        # 1. Get product details
        production_table = dynamodb.Table(production_table_name)
        product_resp = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Product '{product_id}' not found."})
            }

        product_item = product_resp['Item']
        product_name = product_item['product_name']
        stock_needed = product_item['stock_needed']  # dict {stock_item_id: qty_needed_per_unit}

        # 2. Check stock availability
        stock_table = dynamodb.Table(stock_table_name)
        required_deductions = {}

        for item_id, qty_str in stock_needed.items():
            qty_needed_each = Decimal(str(qty_str))
            total_needed = qty_needed_each * quantity_to_produce

            resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' not in resp:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Required stock '{item_id}' not found."})
                }

            available_qty = Decimal(str(resp['Item']['quantity']))
            if available_qty < total_needed:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Insufficient stock '{item_id}' to produce {quantity_to_produce}."})
                }

            required_deductions[item_id] = total_needed

        # 3. Deduct from stock
        for item_id, deduction_qty in required_deductions.items():
            stock_item = stock_table.get_item(Key={'item_id': item_id})['Item']
            new_quantity = Decimal(str(stock_item['quantity'])) - deduction_qty
            stock_table.update_item(
                Key={'item_id': item_id},
                UpdateExpression="SET quantity = :q",
                ExpressionAttributeValues={':q': new_quantity}
            )

        # 4. Record push in push_to_production
        push_table = dynamodb.Table(push_production_table_name)
        push_id = str(uuid.uuid4())

        # We'll store the deductions as Decimal in DynamoDB to avoid float issues
        push_table.put_item(
            Item={
                'push_id': push_id,
                'product_id': product_id,
                'product_name': product_name,
                'quantity_produced': quantity_to_produce,
                'stock_deductions': required_deductions,  # Decimals
                'status': 'ACTIVE',
                'username': username
            }
        )

        # 5. Log transaction
        log_transaction(
            "PushToProduction",
            {
                "push_id": push_id,
                "product_id": product_id,
                "quantity_produced": quantity_to_produce,
                "deductions": required_deductions
            },
            username
        )

        # 6. Recalculate the max_produce now that stock has changed
        recalc_max_produce(product_id)

        logger.info(f"Pushed {quantity_to_produce} of '{product_name}' to production (push_id={push_id}).")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Product pushed to production successfully.",
                "push_id": push_id,
                "product_id": product_id,
                "quantity_produced": quantity_to_produce
            })
        }
    except Exception as e:
        logger.error(f"Error in push_to_production: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def undo_production(body):
    """
    Undo a previous push, restoring the deducted stock, marking status=UNDONE.
    Then recalc max_produce in 'production'.
    """
    try:
        required = ['push_id', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        push_id = body['push_id']
        username = body['username']

        push_table = dynamodb.Table(push_production_table_name)
        push_resp = push_table.get_item(Key={'push_id': push_id})
        if 'Item' not in push_resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Push '{push_id}' not found."})
            }

        push_item = push_resp['Item']
        if push_item['status'] != 'ACTIVE':
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Push '{push_id}' is not active or already undone."})
            }

        product_id = push_item['product_id']

        # restore stock
        stock_deductions = push_item['stock_deductions']  # {item_id: Decimal deducted}
        stock_table = dynamodb.Table(stock_table_name)

        for item_id, deduction_decimal in stock_deductions.items():
            stock_resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' in stock_resp:
                current_qty = Decimal(str(stock_resp['Item']['quantity']))
                new_qty = current_qty + Decimal(str(deduction_decimal))
                stock_table.update_item(
                    Key={'item_id': item_id},
                    UpdateExpression="SET quantity = :q",
                    ExpressionAttributeValues={':q': new_qty}
                )
            else:
                # If stock was deleted entirely, skip or re-create if needed.
                pass

        # mark push as undone
        push_table.update_item(
            Key={'push_id': push_id},
            UpdateExpression="SET #s = :st",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':st': 'UNDONE'}
        )

        # log transaction
        log_transaction(
            "UndoProduction",
            {
                "push_id": push_id,
                "details": f"Stock restored for push '{push_id}'"
            },
            username
        )

        # recalc
        recalc_max_produce(product_id)

        logger.info(f"Undo push '{push_id}' by {username}, stock restored.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Push '{push_id}' undone successfully."})
        }
    except Exception as e:
        logger.error(f"Error in undo_production: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


def delete_push_to_production(body):
    """
    Delete a push record from 'push_to_production' by push_id.
    This is a production-related operation => we log it.
    """
    try:
        required = ['push_id', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }

        push_id = body['push_id']
        username = body['username']

        push_table = dynamodb.Table(push_production_table_name)
        push_resp = push_table.get_item(Key={'push_id': push_id})
        if 'Item' not in push_resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Push record '{push_id}' not found."})
            }

        push_table.delete_item(Key={'push_id': push_id})

        # Log
        log_transaction(
            "DeletePushToProduction",
            {
                "push_id": push_id,
                "details": f"Push record '{push_id}' deleted"
            },
            username
        )

        logger.info(f"Push record '{push_id}' deleted by {username}.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Push record '{push_id}' deleted successfully."})
        }

    except Exception as e:
        logger.error(f"Error in delete_push_to_production: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


# =============================================================================
# =========================== DAILY REPORT ====================================
# =============================================================================

def get_daily_report(body):
    """
    Get a daily report of all operations performed on a given date (IST).
    """
    try:
        if 'date' not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "'date' is required for GetDailyReport operation"})
            }

        date = body['date']
        username = body.get('username', 'Unknown')

        transactions_table = dynamodb.Table(transactions_table_name)
        response = transactions_table.scan(
            FilterExpression=Attr('date').eq(date)
        )
        transactions = response['Items']

        # Categorize by operation_type
        categorized_report = {}
        for tx in transactions:
            operation = tx.get('operation_type', 'UnknownOperation')
            details = tx.get('details', {})
            if operation not in categorized_report:
                categorized_report[operation] = []
            categorized_report[operation].append(details)

        logger.info(f"Daily report requested by {username} for date {date}.")
        return {
            "statusCode": 200,
            "body": json.dumps(categorized_report, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_daily_report: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }


# =============================================================================
# =========================== LAMBDA HANDLER ==================================
# =============================================================================

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    1) Ensure tables exist (initialize_tables).
    2) Parse JSON from event['body'].
    3) Route to the correct function by 'operation'.
    """
    try:
        # Create tables if they don't exist
        initialize_tables()

        # Parse the body from the event
        body = json.loads(event.get('body', '{}'))
        operation = body.get('operation')

        if not operation:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'operation' field"})
            }

        # -----------------------------
        # Admin operations (no logs)
        # -----------------------------
        if operation == "DeleteTransactionData":
            return delete_transaction_data(body)
        elif operation == "AdminViewUsers":
            return admin_view_users(body)
        elif operation == "AdminUpdateUser":
            return admin_update_user(body)

        # -----------------------------
        # User management
        # -----------------------------
        elif operation == "RegisterUser":
            return register_user(body)
        elif operation == "LoginUser":
            return login_user(body)

        # -----------------------------
        # Stock
        # -----------------------------
        elif operation == "CreateStock":
            return create_stock(body)
        elif operation == "UpdateStock":
            return update_stock(body)
        elif operation == "DeleteStock":
            return delete_stock(body)
        elif operation == "GetAllStocks":
            return get_all_stocks(body)

        # -----------------------------
        # Production
        # -----------------------------
        elif operation == "CreateProduct":
            return create_product(body)
        elif operation == "UpdateProduct":
            return update_product(body)
        elif operation == "DeleteProduct":
            return delete_product(body)
        elif operation == "GetAllProducts":
            return get_all_products(body)

        # -----------------------------
        # Push/Undo production
        # -----------------------------
        elif operation == "PushToProduction":
            return push_to_production(body)
        elif operation == "UndoProduction":
            return undo_production(body)
        elif operation == "DeletePushToProduction":
            return delete_push_to_production(body)

        # -----------------------------
        # Reporting
        # -----------------------------
        elif operation == "GetDailyReport":
            return get_daily_report(body)

        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid operation"})
            }

    except json.JSONDecodeError:
        logger.error("Invalid JSON format in request.")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }





