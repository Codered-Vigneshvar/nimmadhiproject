import json
import boto3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
from datetime import datetime, timedelta, date
import uuid
import logging
import hashlib
import calendar

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Specify region explicitly (Ohio is us-east-2)
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

# Table names
users_table_name = 'users'
stock_table_name = 'stock'
transactions_table_name = 'stock_transactions'
production_table_name = 'production'
push_production_table_name = 'push_to_production'
undo_table_name = 'undo_actions'

# =============================================================================
# TABLE INITIALIZATION
# =============================================================================

def wait_for_table_creation(table_name):
    try:
        table = dynamodb.Table(table_name)
        logger.info(f"Waiting for table '{table_name}' to be active...")
        table.wait_until_exists()
        desc = table.table_status
        logger.info(f"Table '{table_name}' is now active. Status: {desc}")
    except Exception as e:
        logger.error(f"Error while waiting for table '{table_name}': {str(e)}")

def initialize_tables():
    try:
        existing_tables = dynamodb.meta.client.list_tables()['TableNames']
        logger.info(f"Existing tables: {existing_tables}")

        if users_table_name not in existing_tables:
            logger.info(f"Creating '{users_table_name}' table...")
            dynamodb.create_table(
                TableName=users_table_name,
                KeySchema=[{'AttributeName': 'username', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'username', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(users_table_name)

        if stock_table_name not in existing_tables:
            logger.info(f"Creating '{stock_table_name}' table...")
            dynamodb.create_table(
                TableName=stock_table_name,
                KeySchema=[{'AttributeName': 'item_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'item_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(stock_table_name)

        if transactions_table_name not in existing_tables:
            logger.info(f"Creating '{transactions_table_name}' table...")
            dynamodb.create_table(
                TableName=transactions_table_name,
                KeySchema=[{'AttributeName': 'transaction_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'transaction_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(transactions_table_name)

        if production_table_name not in existing_tables:
            logger.info(f"Creating '{production_table_name}' table...")
            dynamodb.create_table(
                TableName=production_table_name,
                KeySchema=[{'AttributeName': 'product_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'product_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(production_table_name)

        if push_production_table_name not in existing_tables:
            logger.info(f"Creating '{push_production_table_name}' table...")
            dynamodb.create_table(
                TableName=push_production_table_name,
                KeySchema=[{'AttributeName': 'push_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'push_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(push_production_table_name)

        if undo_table_name not in existing_tables:
            logger.info(f"Creating '{undo_table_name}' table...")
            dynamodb.create_table(
                TableName=undo_table_name,
                KeySchema=[{'AttributeName': 'undo_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'undo_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            wait_for_table_creation(undo_table_name)

    except Exception as e:
        logger.error(f"Unexpected error in initialize_tables: {str(e)}")

# =============================================================================
# JSON ENCODER
# =============================================================================

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

# =============================================================================
# HELPER FUNCTIONS FOR CONSUMPTION DETAILS
# =============================================================================

def format_ist_timestamp(iso_timestamp):
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00').split('+')[0])
        return dt.strftime('%Y-%m-%d %I:%M:%S %p')
    except Exception as e:
        logger.error(f"Error formatting timestamp {iso_timestamp}: {str(e)}")
        return iso_timestamp

def extract_consumption_details(transactions):
    """
    From a list of transactions, extract only consumption details from:
      - AddDefectiveGoods
      - PushToProduction
    """
    consumption_ops = ["AddDefectiveGoods", "PushToProduction"]
    details = []
    for tx in transactions:
        op = tx.get("operation_type")
        if op in consumption_ops:
            dt = tx.get("details", {})
            if op == "PushToProduction":
                deductions = dt.get("deductions", {})
                for item_id, qty in deductions.items():
                    details.append({
                        "item_id": item_id,
                        "quantity_consumed": qty,
                        "operation": op,
                        "timestamp": format_ist_timestamp(tx.get("timestamp"))
                    })
            else:
                qty = dt.get("defective_added", 0)
                details.append({
                    "item_id": dt.get("item_id", "Unknown"),
                    "quantity_consumed": qty,
                    "operation": op,
                    "timestamp": format_ist_timestamp(tx.get("timestamp"))
                })
    return details

def summarize_consumption_details(details):
    """
    Group the consumption details by item_id and sum the quantity consumed.
    Returns a list of dictionaries.
    """
    summary = {}
    for d in details:
        item = d.get("item_id", "Unknown")
        qty = Decimal(str(d.get("quantity_consumed", 0)))
        summary[item] = summary.get(item, Decimal('0')) + qty
    summarized_list = [{"item_id": k, "total_quantity_consumed": float(v)} for k, v in summary.items()]
    return summarized_list

def compute_consumption_amount(transactions):
    """
    Compute the total consumption amount based on PushToProduction transactions.
    """
    amount = Decimal('0')
    for tx in transactions:
        if tx.get("operation_type") == "PushToProduction":
            dt = tx.get("details", {})
            amt = Decimal(str(dt.get("total_production_cost", 0)))
            amount += amt
    return amount

# =============================================================================
# TRANSACTION LOGGING
# =============================================================================

def log_transaction(operation_type, details, username):
    try:
        transaction_id = str(uuid.uuid4())
        timestamp_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        timestamp_str = timestamp_ist.isoformat()
        date_str = timestamp_ist.strftime("%Y-%m-%d")

        # Convert float values to Decimal if needed
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
    except Exception as e:
        logger.error(f"Error in log_transaction: {str(e)}")

# =============================================================================
# GET CURRENT STOCK SUMMARY
# =============================================================================

def get_current_stock_summary():
    try:
        stock_table = dynamodb.Table(stock_table_name)
        items = []
        response = stock_table.scan()
        items.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = stock_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        total_qty = Decimal('0')
        total_amount = Decimal('0')
        for it in items:
            q = Decimal(str(it.get('quantity', 0)))
            tc = Decimal(str(it.get('total_cost', 0)))
            total_qty += q
            total_amount += tc
        return total_qty, total_amount
    except Exception as e:
        logger.error(f"Error in get_current_stock_summary: {str(e)}")
        return Decimal('0'), Decimal('0')

# =============================================================================
# GROUP & CLASSIFY TRANSACTIONS
# =============================================================================

def group_transactions_by_operation(transactions):
    grouped = {}
    for tx in transactions:
        op = tx.get('operation_type', 'UnknownOperation')
        if op not in grouped:
            grouped[op] = []
        grouped[op].append(tx)
    return grouped

def classify_addition_and_consumption(transactions):
    additions_qty = Decimal('0')
    additions_amount = Decimal('0')
    consumption_qty = Decimal('0')
    consumption_amount = Decimal('0')
    for tx in transactions:
        op = tx.get('operation_type', '')
        details = tx.get('details', {})
        if op in ["CreateStock", "AddStockQuantity", "SubtractDefectiveGoods"]:
            if op == "CreateStock":
                added_qty = Decimal(str(details.get("quantity", 0)))
                added_cost = Decimal(str(details.get("total_cost", 0)))
                additions_qty += added_qty
                additions_amount += added_cost
            elif op == "AddStockQuantity":
                added_qty = Decimal(str(details.get("quantity_added", 0)))
                added_cost = Decimal(str(details.get("added_cost", 0))) if "added_cost" in details else (added_qty * Decimal(str(details.get("cost_per_unit", 0))))
                additions_qty += added_qty
                additions_amount += added_cost
            elif op == "SubtractDefectiveGoods":
                def_sub = Decimal(str(details.get("defective_subtracted", 0)))
                additions_qty += def_sub
        elif op in ["SubtractStockQuantity", "AddDefectiveGoods", "PushToProduction"]:
            # Only count consumption from AddDefectiveGoods and PushToProduction
            if op in ["AddDefectiveGoods", "PushToProduction"]:
                if op == "AddDefectiveGoods":
                    qty = Decimal(str(details.get("defective_added", 0)))
                else:
                    qty = Decimal(str(details.get("quantity_produced", 0)))
                consumption_qty += qty
                if op == "PushToProduction":
                    prod_cost = Decimal(str(details.get("total_production_cost", 0)))
                    consumption_amount += prod_cost
    return (additions_qty, additions_amount, consumption_qty, consumption_amount)

# =============================================================================
# LOGGING & UNDO FUNCTIONS
# =============================================================================

def get_user_active_undo_count(username):
    try:
        table = dynamodb.Table(undo_table_name)
        response = table.scan(
            FilterExpression=Attr('username').eq(username) & Attr('status').eq('ACTIVE')
        )
        return len(response.get('Items', []))
    except Exception as e:
        logger.error(f"Error in get_user_active_undo_count: {str(e)}")
        return 0

def remove_oldest_undo(username):
    try:
        table = dynamodb.Table(undo_table_name)
        response = table.scan(
            FilterExpression=Attr('username').eq(username) & Attr('status').eq('ACTIVE')
        )
        items = response.get('Items', [])
        if items:
            oldest = sorted(items, key=lambda x: x.get('timestamp'))[0]
            table.delete_item(Key={'undo_id': oldest['undo_id']})
            logger.info(f"Removed oldest undo record for user {username}: {oldest['undo_id']}")
    except Exception as e:
        logger.error(f"Error in remove_oldest_undo: {str(e)}")

def log_undo_action(operation, undo_details, username):
    try:
        if get_user_active_undo_count(username) >= 3:
            remove_oldest_undo(username)
        table = dynamodb.Table(undo_table_name)
        undo_id = str(uuid.uuid4())
        timestamp = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        table.put_item(
            Item={
                'undo_id': undo_id,
                'operation': operation,
                'undo_details': undo_details,
                'username': username,
                'status': 'ACTIVE',
                'timestamp': timestamp
            }
        )
        logger.info(f"Undo record logged: {operation}, ID: {undo_id}")
        return undo_id
    except Exception as e:
        logger.error(f"Error in log_undo_action: {str(e)}")
        return None

def mark_undo_as_done(undo_id):
    try:
        table = dynamodb.Table(undo_table_name)
        table.update_item(
            Key={'undo_id': undo_id},
            UpdateExpression="SET #s = :st",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':st': 'UNDONE'}
        )
    except Exception as e:
        logger.error(f"Error in mark_undo_as_done: {str(e)}")

def get_undo_record(undo_id):
    try:
        table = dynamodb.Table(undo_table_name)
        response = table.get_item(Key={'undo_id': undo_id})
        return response.get('Item')
    except Exception as e:
        logger.error(f"Error in get_undo_record: {str(e)}")
        return None

# =============================================================================
# ADMIN & USER MANAGEMENT
# =============================================================================

def admin_auth_check(body):
    return (body.get('username') == 'admin' and body.get('password') == '37773')

def delete_transaction_data(body):
    try:
        if not admin_auth_check(body):
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized: Admin credentials required."})}
        transactions_table = dynamodb.Table(transactions_table_name)
        scan_resp = transactions_table.scan()
        items = scan_resp.get('Items', [])
        with transactions_table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={'transaction_id': item['transaction_id']})
        while 'LastEvaluatedKey' in scan_resp:
            scan_resp = transactions_table.scan(ExclusiveStartKey=scan_resp['LastEvaluatedKey'])
            items = scan_resp.get('Items', [])
            with transactions_table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'transaction_id': item['transaction_id']})
        logger.info("All transaction data deleted by admin.")
        return {"statusCode": 200, "body": json.dumps({"message": "All transaction data deleted."})}
    except Exception as e:
        logger.error(f"Error in delete_transaction_data: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def admin_view_users(body):
    try:
        if not admin_auth_check(body):
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized: Admin credentials required."})}
        users_table = dynamodb.Table(users_table_name)
        scan_resp = users_table.scan()
        items = scan_resp.get('Items', [])
        while 'LastEvaluatedKey' in scan_resp:
            scan_resp = users_table.scan(ExclusiveStartKey=scan_resp['LastEvaluatedKey'])
            items.extend(scan_resp.get('Items', []))
        logger.info("Admin viewed all users.")
        return {"statusCode": 200, "body": json.dumps(items, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in admin_view_users: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def admin_update_user(body):
    try:
        if not admin_auth_check(body):
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized: Admin credentials required."})}
        if 'username_to_update' not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'username_to_update' is required"})}
        username_to_update = body['username_to_update']
        new_password = body.get('new_password')
        users_table = dynamodb.Table(users_table_name)
        user_resp = users_table.get_item(Key={'username': username_to_update})
        if 'Item' not in user_resp:
            return {"statusCode": 404, "body": json.dumps({"error": f"User '{username_to_update}' not found."})}
        if new_password:
            hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
            users_table.update_item(
                Key={'username': username_to_update},
                UpdateExpression="SET #p = :pw",
                ExpressionAttributeNames={"#p": "password"},
                ExpressionAttributeValues={":pw": hashed_password}
            )
        logger.info(f"Admin updated user '{username_to_update}'.")
        return {"statusCode": 200, "body": json.dumps({"message": f"User '{username_to_update}' updated successfully."})}
    except Exception as e:
        logger.error(f"Error in admin_update_user: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def register_user(body):
    try:
        if 'username' not in body or 'password' not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'username' and 'password' are required."})}
        username = body['username']
        password = body['password']
        users_table = dynamodb.Table(users_table_name)
        existing_user = users_table.get_item(Key={'username': username}).get('Item')
        if existing_user:
            return {"statusCode": 400, "body": json.dumps({"error": "Username already registered."})}
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        users_table.put_item(Item={'username': username, 'password': hashed_password})
        logger.info(f"New user registered: {username}")
        return {"statusCode": 200, "body": json.dumps({"message": "User registered successfully."})}
    except Exception as e:
        logger.error(f"Error registering user: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def login_user(body):
    try:
        if 'username' not in body or 'password' not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'username' and 'password' are required."})}
        
        username = body['username']
        password = body['password']
        
        if username == "admin":
            if str(password) == "37773":
                logger.info("Admin logged in: admin")
                return {"statusCode": 200, "body": json.dumps({"message": "Admin logged in successfully."})}
            else:
                return {"statusCode": 401, "body": json.dumps({"error": "Wrong admin credentials."})}
        
        users_table = dynamodb.Table(users_table_name)
        user_item = users_table.get_item(Key={'username': username}).get('Item')
        if not user_item:
            return {"statusCode": 401, "body": json.dumps({"error": "Invalid username or password."})}
        
        hashed_input_password = hashlib.sha256(password.encode()).hexdigest()
        if hashed_input_password != user_item['password']:
            return {"statusCode": 401, "body": json.dumps({"error": "Invalid username or password."})}
        
        logger.info(f"User logged in: {username}")
        return {"statusCode": 200, "body": json.dumps({"message": "Login successful."})}
    
    except Exception as e:
        logger.error(f"Error logging in user: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

# =============================================================================
# PRODUCTION UTILS
# =============================================================================

def recalc_max_produce(product_id):
    try:
        production_table = dynamodb.Table(production_table_name)
        stock_table = dynamodb.Table(stock_table_name)
        product_resp = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_resp:
            return
        prod_item = product_resp['Item']
        stock_needed = prod_item['stock_needed']
        max_produce = None
        for item_id, qty_str in stock_needed.items():
            qty_needed = Decimal(str(qty_str))
            stock_resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' not in stock_resp:
                max_produce = 0
                break
            available_qty = Decimal(str(stock_resp['Item']['quantity']))
            possible = available_qty // qty_needed
            if max_produce is None or possible < max_produce:
                max_produce = possible
        if max_produce is None:
            max_produce = 0
        production_table.update_item(
            Key={'product_id': product_id},
            UpdateExpression="SET max_produce = :mp",
            ExpressionAttributeValues={':mp': int(max_produce)}
        )
    except Exception as e:
        logger.error(f"Error in recalc_max_produce: {str(e)}")

def recalc_all_production():
    try:
        production_table = dynamodb.Table(production_table_name)
        response = production_table.scan()
        products = response.get('Items', [])
        for product in products:
            recalc_max_produce(product['product_id'])
        while 'LastEvaluatedKey' in response:
            response = production_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            products = response.get('Items', [])
            for product in products:
                recalc_max_produce(product['product_id'])
    except Exception as e:
        logger.error(f"Error in recalc_all_production: {str(e)}")

# =============================================================================
# STOCK OPERATIONS
# =============================================================================

def create_stock(body):
    try:
        required = ['name', 'quantity', 'defective', 'cost_per_unit', 'stock_limit', 'username', 'unit']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        item_id = body['name']
        quantity = Decimal(str(body['quantity']))
        defective = Decimal(str(body['defective']))
        cost_per_unit = Decimal(str(body['cost_per_unit']))
        stock_limit = Decimal(str(body['stock_limit']))
        username = body['username']
        unit = body['unit']
        if quantity < 0 or defective < 0 or defective > quantity:
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid quantity or defective values"})}
        available_quantity = quantity - defective
        total_cost = available_quantity * cost_per_unit
        stock_table = dynamodb.Table(stock_table_name)
        existing_item = stock_table.get_item(Key={'item_id': item_id}).get('Item')
        if existing_item:
            return {"statusCode": 400, "body": json.dumps({"error": "Stock with this name already exists"})}
        now_str = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        stock_item = {
            'item_id': item_id,
            'name': item_id,
            'quantity': available_quantity,
            'cost_per_unit': cost_per_unit,
            'total_cost': total_cost,
            'stock_limit': stock_limit,
            'defective': defective,
            'total_quantity': quantity,
            'username': username,
            'unit': unit,
            'created_at': now_str,
            'updated_at': now_str
        }
        stock_table.put_item(Item=stock_item)
        log_transaction("CreateStock", {
            'item_id': item_id,
            'available_quantity': available_quantity,
            'defective': defective,
            'quantity': quantity,
            'cost_per_unit': float(cost_per_unit),
            'total_cost': float(total_cost),
            'stock_limit': stock_limit
        }, username)
        log_undo_action("CreateStock", {'item_id': item_id}, username)
        recalc_all_production()
        logger.info(f"Stock created by {username}: {item_id}")
        return {"statusCode": 200, "body": json.dumps({
            "message": "Stock created successfully.",
            "name": item_id,
            "quantity": available_quantity,
            "defective": defective,
            "total_quantity": quantity,
            "cost_per_unit": float(cost_per_unit),
            "total_cost": float(total_cost),
            "stock_limit": stock_limit,
            "username": username,
            "unit": unit
        }, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in create_stock: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def update_stock(body):
    try:
        required = ['name', 'quantity', 'defective', 'cost_per_unit', 'stock_limit', 'username', 'unit']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        item_id = body['name']
        new_quantity = Decimal(str(body['quantity']))
        defective = Decimal(str(body['defective']))
        new_cost_per_unit = Decimal(str(body['cost_per_unit']))
        stock_limit = Decimal(str(body['stock_limit']))
        username = body['username']
        unit = body['unit']
        available_quantity = new_quantity - defective
        new_total_cost = available_quantity * new_cost_per_unit
        stock_table = dynamodb.Table(stock_table_name)
        existing_item = stock_table.get_item(Key={'item_id': item_id}).get('Item')
        if not existing_item:
            return {"statusCode": 404, "body": json.dumps({"error": "Stock item not found"})}
        old_state = {
            'quantity': existing_item['quantity'],
            'cost_per_unit': existing_item['cost_per_unit'],
            'total_cost': existing_item['total_cost'],
            'stock_limit': existing_item['stock_limit'],
            'defective': existing_item['defective'],
            'total_quantity': existing_item['total_quantity'],
            'unit': existing_item['unit']
        }
        now_str = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        stock_table.update_item(
            Key={'item_id': item_id},
            UpdateExpression="""
                SET quantity = :quantity,
                    cost_per_unit = :cost_per_unit,
                    total_cost = :total_cost,
                    stock_limit = :stock_limit,
                    defective = :defective,
                    total_quantity = :total_quantity,
                    #u = :unit,
                    updated_at = :updated_at
            """,
            ExpressionAttributeNames={"#u": "unit"},
            ExpressionAttributeValues={
                ':quantity': available_quantity,
                ':cost_per_unit': new_cost_per_unit,
                ':total_cost': new_total_cost,
                ':stock_limit': stock_limit,
                ':defective': defective,
                ':total_quantity': new_quantity,
                ':unit': unit,
                ':updated_at': now_str
            }
        )
        log_transaction("UpdateStock", {
            'item_id': item_id,
            'old_quantity': old_state['quantity'],
            'new_quantity': available_quantity,
            'defective': defective,
            'total_quantity': new_quantity,
            'old_cost_per_unit': float(old_state['cost_per_unit']),
            'new_cost_per_unit': float(new_cost_per_unit),
            'new_total_cost': float(new_total_cost),
            'stock_limit': stock_limit
        }, username)
        log_undo_action("UpdateStock", {'item_id': item_id, 'old_state': old_state}, username)
        recalc_all_production()
        logger.info(f"Stock updated by {username}: {item_id}")
        return {"statusCode": 200, "body": json.dumps({
            "message": "Stock updated successfully.",
            "name": item_id,
            "quantity": available_quantity,
            "defective": defective,
            "total_quantity": new_quantity,
            "cost_per_unit": float(new_cost_per_unit),
            "total_cost": float(new_total_cost),
            "stock_limit": stock_limit,
            "username": username,
            "unit": unit
        }, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in update_stock: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def delete_stock(body):
    try:
        required = ['name', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        name = body['name']
        username = body['username']
        stock_table = dynamodb.Table(stock_table_name)
        response = stock_table.get_item(Key={'item_id': name})
        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"error": f"Stock item '{name}' not found."})}
        deleted_item = response['Item']
        stock_table.delete_item(Key={'item_id': name})
        log_transaction("DeleteStock", {"item_id": name, "details": f"Stock '{name}' deleted"}, username)
        log_undo_action("DeleteStock", {'deleted_item': deleted_item}, username)
        recalc_all_production()
        logger.info(f"Stock '{name}' deleted by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Stock '{name}' deleted successfully."})}
    except Exception as e:
        logger.error(f"Error in delete_stock: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def add_stock_quantity(body):
    try:
        required = ['name', 'quantity_to_add', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        name = body['name']
        quantity_to_add = Decimal(str(body['quantity_to_add']))
        username = body['username']
        stock_table = dynamodb.Table(stock_table_name)
        response = stock_table.get_item(Key={'item_id': name})
        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"error": f"Stock item '{name}' not found."})}
        item = response['Item']
        new_total = Decimal(str(item['total_quantity'])) + quantity_to_add
        defective = Decimal(str(item['defective']))
        new_available = new_total - defective
        now_str = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        cost_per_unit = Decimal(str(item.get('cost_per_unit', 0)))
        added_cost = cost_per_unit * quantity_to_add
        new_total_cost = Decimal(str(item.get('total_cost', 0))) + added_cost
        stock_table.update_item(
            Key={'item_id': name},
            UpdateExpression="""
                SET total_quantity = :t,
                    quantity = :q,
                    total_cost = :tc,
                    updated_at = :updated_at
            """,
            ExpressionAttributeValues={
                ':t': new_total,
                ':q': new_available,
                ':tc': new_total_cost,
                ':updated_at': now_str
            }
        )
        log_transaction("AddStockQuantity", {
            "item_id": name,
            "quantity_added": quantity_to_add,
            "new_total": new_total,
            "added_cost": float(added_cost)
        }, username)
        log_undo_action("AddStockQuantity", {"item_id": name, "quantity_added": quantity_to_add}, username)
        recalc_all_production()
        logger.info(f"Added {quantity_to_add} to stock '{name}' by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Added {quantity_to_add} units to stock '{name}'."})}
    except Exception as e:
        logger.error(f"Error in add_stock_quantity: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def subtract_stock_quantity(body):
    try:
        required = ['name', 'quantity_to_subtract', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        name = body['name']
        quantity_to_subtract = Decimal(str(body['quantity_to_subtract']))
        username = body['username']
        stock_table = dynamodb.Table(stock_table_name)
        response = stock_table.get_item(Key={'item_id': name})
        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"error": f"Stock item '{name}' not found."})}
        item = response['Item']
        current_total = Decimal(str(item['total_quantity']))
        if quantity_to_subtract > (current_total - Decimal(str(item['defective']))):
            return {"statusCode": 400, "body": json.dumps({"error": "Insufficient available stock to subtract."})}
        new_total = current_total - quantity_to_subtract
        defective = Decimal(str(item['defective']))
        new_available = new_total - defective
        now_str = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        cost_per_unit = Decimal(str(item.get('cost_per_unit', 0)))
        sub_cost = cost_per_unit * quantity_to_subtract
        new_total_cost = Decimal(str(item.get('total_cost', 0))) - sub_cost
        if new_total_cost < 0:
            new_total_cost = Decimal('0')
        stock_table.update_item(
            Key={'item_id': name},
            UpdateExpression="""
                SET total_quantity = :t,
                    quantity = :q,
                    total_cost = :tc,
                    updated_at = :updated_at
            """,
            ExpressionAttributeValues={
                ':t': new_total,
                ':q': new_available,
                ':tc': new_total_cost,
                ':updated_at': now_str
            }
        )
        log_transaction("SubtractStockQuantity", {
            "item_id": name,
            "quantity_subtracted": quantity_to_subtract,
            "new_total": new_total
        }, username)
        log_undo_action("SubtractStockQuantity", {"item_id": name, "quantity_subtracted": quantity_to_subtract}, username)
        recalc_all_production()
        logger.info(f"Subtracted {quantity_to_subtract} from stock '{name}' by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Subtracted {quantity_to_subtract} units from stock '{name}'."})}
    except Exception as e:
        logger.error(f"Error in subtract_stock_quantity: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def add_defective_goods(body):
    try:
        required = ['name', 'defective_to_add', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        name = body['name']
        defective_to_add = Decimal(str(body['defective_to_add']))
        username = body['username']
        stock_table = dynamodb.Table(stock_table_name)
        response = stock_table.get_item(Key={'item_id': name})
        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"error": f"Stock item '{name}' not found."})}
        item = response['Item']
        current_defective = Decimal(str(item['defective']))
        current_total = Decimal(str(item['total_quantity']))
        new_defective = current_defective + defective_to_add
        if new_defective > current_total:
            return {"statusCode": 400, "body": json.dumps({"error": "Defective count cannot exceed total quantity."})}
        new_available = current_total - new_defective
        now_str = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        stock_table.update_item(
            Key={'item_id': name},
            UpdateExpression="""
                SET defective = :d,
                    quantity = :q,
                    updated_at = :updated_at
            """,
            ExpressionAttributeValues={
                ':d': new_defective,
                ':q': new_available,
                ':updated_at': now_str
            }
        )
        log_transaction("AddDefectiveGoods", {
            "item_id": name,
            "defective_added": defective_to_add,
            "new_defective": new_defective
        }, username)
        log_undo_action("AddDefectiveGoods", {"item_id": name, "defective_added": defective_to_add}, username)
        recalc_all_production()
        logger.info(f"Added {defective_to_add} defective goods to stock '{name}' by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Added {defective_to_add} defective goods to stock '{name}'."})}
    except Exception as e:
        logger.error(f"Error in add_defective_goods: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def subtract_defective_goods(body):
    try:
        required = ['name', 'defective_to_subtract', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        name = body['name']
        defective_to_subtract = Decimal(str(body['defective_to_subtract']))
        username = body['username']
        stock_table = dynamodb.Table(stock_table_name)
        response = stock_table.get_item(Key={'item_id': name})
        if 'Item' not in response:
            return {"statusCode": 404, "body": json.dumps({"error": f"Stock item '{name}' not found."})}
        item = response['Item']
        current_defective = Decimal(str(item['defective']))
        if defective_to_subtract > current_defective:
            return {"statusCode": 400, "body": json.dumps({"error": "Cannot subtract more defective goods than currently recorded."})}
        new_defective = current_defective - defective_to_subtract
        current_total = Decimal(str(item['total_quantity']))
        new_available = current_total - new_defective
        now_str = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        stock_table.update_item(
            Key={'item_id': name},
            UpdateExpression="""
                SET defective = :d,
                    quantity = :q,
                    updated_at = :updated_at
            """,
            ExpressionAttributeValues={
                ':d': new_defective,
                ':q': new_available,
                ':updated_at': now_str
            }
        )
        log_transaction("SubtractDefectiveGoods", {
            "item_id": name,
            "defective_subtracted": defective_to_subtract,
            "new_defective": new_defective
        }, username)
        log_undo_action("SubtractDefectiveGoods", {"item_id": name, "defective_subtracted": defective_to_subtract}, username)
        recalc_all_production()
        logger.info(f"Subtracted {defective_to_subtract} defective goods from stock '{name}' by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Subtracted {defective_to_subtract} defective goods from stock '{name}'."})}
    except Exception as e:
        logger.error(f"Error in subtract_defective_goods: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def get_all_stocks(body):
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
        return {"statusCode": 200, "body": json.dumps(items, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in get_all_stocks: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

# =============================================================================
# PRODUCTION OPERATIONS
# =============================================================================

def create_product(body):
    try:
        required = ['product_name', 'stock_needed', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        product_name = body['product_name']
        stock_needed = body['stock_needed']
        # Convert stock_needed values to Decimal automatically
        stock_needed_converted = { k: Decimal(str(v)) for k, v in stock_needed.items() }
        username = body['username']
        product_id = str(uuid.uuid4())
        stock_table = dynamodb.Table(stock_table_name)
        max_produce = None
        cost_breakdown = {}
        production_cost_total = Decimal('0')
        for item_id, qty_needed in stock_needed_converted.items():
            resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' not in resp:
                max_produce = Decimal('0')
                production_cost_total = Decimal('0')
                cost_breakdown = {}
                break
            available_qty = Decimal(str(resp['Item']['quantity']))
            possible = available_qty // qty_needed
            if max_produce is None or possible < max_produce:
                max_produce = possible
            cost_per_unit = Decimal(str(resp['Item']['cost_per_unit']))
            cost_for_item = cost_per_unit * qty_needed
            cost_breakdown[item_id] = cost_for_item
            production_cost_total += cost_for_item
        if max_produce is None:
            max_produce = Decimal('0')
        production_table = dynamodb.Table(production_table_name)
        created_at = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        production_table.put_item(
            Item={
                'product_id': product_id,
                'product_name': product_name,
                'stock_needed': stock_needed_converted,
                'max_produce': int(max_produce),
                'original_max_produce': int(max_produce),
                'username': username,
                'production_cost_breakdown': {k: str(v) for k, v in cost_breakdown.items()},
                'production_cost_total': production_cost_total,
                'inventory': int(max_produce),
                'created_at': created_at
            }
        )
        log_transaction("CreateProduct", {
            "product_id": product_id,
            "product_name": product_name,
            "stock_needed": stock_needed_converted,
            "max_produce": int(max_produce),
            "production_cost_breakdown": {k: str(v) for k, v in cost_breakdown.items()},
            "production_cost_total": production_cost_total
        }, username)
        log_undo_action("CreateProduct", {"product_id": product_id}, username)
        logger.info(f"Product created: {product_name} (ID: {product_id}), max_produce={max_produce}")
        return {"statusCode": 200, "body": json.dumps({
            "message": "Product created successfully.",
            "product_id": product_id,
            "product_name": product_name,
            "stock_needed": stock_needed_converted,
            "max_produce": int(max_produce),
            "production_cost_breakdown": {k: float(Decimal(v)) for k, v in {k: str(v) for k, v in cost_breakdown.items()}.items()},
            "production_cost_total": float(production_cost_total)
        }, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in create_product: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def update_product(body):
    try:
        required = ['product_id', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        product_id = body['product_id']
        username = body['username']
        production_table = dynamodb.Table(production_table_name)
        product_response = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_response:
            return {"statusCode": 404, "body": json.dumps({"error": f"Product '{product_id}' not found."})}
        product_item = product_response['Item']
        old_state = {
            'product_name': product_item.get('product_name'),
            'stock_needed': product_item.get('stock_needed'),
            'max_produce': product_item.get('max_produce'),
            'production_cost_breakdown': product_item.get('production_cost_breakdown'),
            'production_cost_total': product_item.get('production_cost_total')
        }
        new_product_name = body.get('product_name', product_item.get('product_name'))
        new_stock_needed = body.get('stock_needed', product_item.get('stock_needed'))
        if 'stock_needed' in body:
            # Convert incoming stock_needed values to Decimal
            new_stock_needed_converted = { k: Decimal(str(v)) for k, v in new_stock_needed.items() }
            stock_table = dynamodb.Table(stock_table_name)
            max_produce = None
            cost_breakdown = {}
            production_cost_total = Decimal('0')
            for item_id, qty_needed in new_stock_needed_converted.items():
                resp = stock_table.get_item(Key={'item_id': item_id})
                if 'Item' not in resp:
                    max_produce = Decimal('0')
                    production_cost_total = Decimal('0')
                    cost_breakdown = {}
                    break
                available_qty = Decimal(str(resp['Item']['quantity']))
                possible = available_qty // qty_needed
                if max_produce is None or possible < max_produce:
                    max_produce = possible
                cost_per_unit = Decimal(str(resp['Item']['cost_per_unit']))
                cost_for_item = cost_per_unit * qty_needed
                cost_breakdown[item_id] = cost_for_item
                production_cost_total += cost_for_item
            if max_produce is None:
                max_produce = Decimal('0')
        else:
            max_produce = product_item.get('max_produce', 0)
            cost_breakdown = product_item.get('production_cost_breakdown', {})
            production_cost_total = product_item.get('production_cost_total', Decimal('0'))
            new_stock_needed_converted = new_stock_needed
        production_table.update_item(
            Key={'product_id': product_id},
            UpdateExpression="""
                SET product_name = :pn,
                    stock_needed = :sn,
                    max_produce = :mp,
                    production_cost_breakdown = :pcb,
                    production_cost_total = :pct
            """,
            ExpressionAttributeValues={
                ':pn': new_product_name,
                ':sn': new_stock_needed_converted,
                ':mp': int(max_produce),
                ':pcb': {k: str(v) for k, v in cost_breakdown.items()},
                ':pct': production_cost_total
            }
        )
        log_transaction("UpdateProduct", {
            "product_id": product_id,
            "new_product_name": new_product_name,
            "new_stock_needed": new_stock_needed_converted,
            "max_produce": int(max_produce),
            "production_cost_breakdown": {k: str(v) for k, v in cost_breakdown.items()},
            "production_cost_total": production_cost_total
        }, username)
        log_undo_action("UpdateProduct", {"product_id": product_id, "old_state": old_state}, username)
        logger.info(f"Product updated: {product_id}")
        return {"statusCode": 200, "body": json.dumps({
            "message": "Product updated successfully.",
            "product_id": product_id,
            "product_name": new_product_name,
            "stock_needed": new_stock_needed_converted,
            "max_produce": int(max_produce),
            "production_cost_breakdown": {k: float(Decimal(v)) for k, v in {k: str(v) for k, v in cost_breakdown.items()}.items()},
            "production_cost_total": float(production_cost_total)
        }, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in update_product: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def delete_product(body):
    try:
        required = ['product_id', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        product_id = body['product_id']
        username = body['username']
        production_table = dynamodb.Table(production_table_name)
        product_resp = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_resp:
            return {"statusCode": 404, "body": json.dumps({"error": f"Product '{product_id}' not found."})}
        deleted_product = product_resp['Item']
        production_table.delete_item(Key={'product_id': product_id})
        log_transaction("DeleteProduct", {"product_id": product_id, "details": f"Product '{product_id}' deleted"}, username)
        log_undo_action("DeleteProduct", {"deleted_product": deleted_product}, username)
        logger.info(f"Product '{product_id}' deleted by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Product '{product_id}' deleted successfully."})}
    except Exception as e:
        logger.error(f"Error in delete_product: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def get_all_products(body):
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
        return {"statusCode": 200, "body": json.dumps(items, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in get_all_products: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def push_to_production(body):
    try:
        required = ['product_id', 'quantity', 'username']
        for field in required:
            if field not in body:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"'{field}' is required"})
                }
        product_id = body['product_id']
        quantity_to_produce = Decimal(str(body['quantity']))
        username = body['username']

        production_table = dynamodb.Table(production_table_name)
        product_resp = production_table.get_item(Key={'product_id': product_id})
        if 'Item' not in product_resp:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Product '{product_id}' not found."})
            }

        product_item = product_resp['Item']
        product_name = product_item['product_name']
        stock_needed = product_item['stock_needed']
        stock_table = dynamodb.Table(stock_table_name)
        required_deductions = {}
        cost_per_unit_total = Decimal('0')

        for item_id, qty_str in stock_needed.items():
            qty_needed_each = Decimal(str(qty_str))
            total_needed = qty_needed_each * quantity_to_produce
            resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' not in resp:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Required stock '{item_id}' not found."})
                }
            stock_item = resp['Item']
            available_qty = Decimal(str(stock_item['quantity']))
            if available_qty < total_needed:
                return {
                    "statusCode": 400,
                    "body": json.dumps({
                        "error": f"Insufficient stock '{item_id}' to produce {quantity_to_produce}."
                    })
                }
            required_deductions[item_id] = total_needed
            cost_per_unit = Decimal(str(stock_item['cost_per_unit']))
            cost_per_unit_total += cost_per_unit * qty_needed_each

        total_production_cost = cost_per_unit_total * quantity_to_produce

        for item_id, deduction_qty in required_deductions.items():
            stock_item = stock_table.get_item(Key={'item_id': item_id})['Item']
            new_quantity = Decimal(str(stock_item['quantity'])) - deduction_qty
            stock_table.update_item(
                Key={'item_id': item_id},
                UpdateExpression="SET quantity = :q",
                ExpressionAttributeValues={':q': new_quantity}
            )

        push_table = dynamodb.Table(push_production_table_name)
        push_timestamp = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat()
        push_id = str(uuid.uuid4())
        push_table.put_item(
            Item={
                'push_id': push_id,
                'product_id': product_id,
                'product_name': product_name,
                'quantity_produced': quantity_to_produce,
                'stock_deductions': required_deductions,
                'status': 'ACTIVE',
                'username': username,
                'production_cost_per_unit': cost_per_unit_total,
                'total_production_cost': total_production_cost,
                'timestamp': push_timestamp
            }
        )

        log_transaction("PushToProduction", {
            "push_id": push_id,
            "product_id": product_id,
            "quantity_produced": quantity_to_produce,
            "deductions": required_deductions,
            "production_cost_per_unit": cost_per_unit_total,
            "total_production_cost": total_production_cost
        }, username)

        log_undo_action("PushToProduction", {"push_id": push_id}, username)
        recalc_max_produce(product_id)

        production_table.update_item(
            Key={'product_id': product_id},
            UpdateExpression="SET production_cost = :pc",
            ExpressionAttributeValues={':pc': cost_per_unit_total}
        )

        logger.info(f"Pushed {quantity_to_produce} of '{product_name}' to production (push_id={push_id}).")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Product pushed to production successfully.",
                "push_id": push_id,
                "product_id": product_id,
                "quantity_produced": quantity_to_produce,
                "production_cost_per_unit": float(cost_per_unit_total),
                "total_production_cost": float(total_production_cost)
            })
        }
    except Exception as e:
        logger.error(f"Error in push_to_production: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        }

def undo_production(body):
    try:
        required = ['push_id', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        push_id = body['push_id']
        username = body['username']
        push_table = dynamodb.Table(push_production_table_name)
        push_resp = push_table.get_item(Key={'push_id': push_id})
        if 'Item' not in push_resp:
            return {"statusCode": 404, "body": json.dumps({"error": f"Push '{push_id}' not found."})}
        push_item = push_resp['Item']
        if push_item['status'] != 'ACTIVE':
            return {"statusCode": 400, "body": json.dumps({"error": f"Push '{push_id}' is not active or already undone."})}
        product_id = push_item['product_id']
        stock_deductions = push_item['stock_deductions']
        stock_table = dynamodb.Table(stock_table_name)
        for item_id, deduction in stock_deductions.items():
            stock_resp = stock_table.get_item(Key={'item_id': item_id})
            if 'Item' in stock_resp:
                current_qty = Decimal(str(stock_resp['Item']['quantity']))
                new_qty = current_qty + Decimal(str(deduction))
                stock_table.update_item(
                    Key={'item_id': item_id},
                    UpdateExpression="SET quantity = :q",
                    ExpressionAttributeValues={':q': new_qty}
                )
        push_table.update_item(
            Key={'push_id': push_id},
            UpdateExpression="SET #s = :st",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':st': 'UNDONE'}
        )
        log_transaction("UndoProduction", {"push_id": push_id, "details": f"Stock restored for push '{push_id}'"}, username)
        recalc_max_produce(product_id)
        logger.info(f"Undo push '{push_id}' by {username}, stock restored.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Push '{push_id}' undone successfully."})}
    except Exception as e:
        logger.error(f"Error in undo_production: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}
        
def get_all_push_to_production(body):
    try:
        username = body.get('username', 'Unknown')
        push_table = dynamodb.Table(push_production_table_name)
        items = []
        response = push_table.scan()
        items.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = push_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        logger.info(f"User '{username}' retrieved all push-to-production records.")
        return {"statusCode": 200, "body": json.dumps(items, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in get_all_push_to_production: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def delete_push_to_production(body):
    try:
        required = ['push_id', 'username']
        for field in required:
            if field not in body:
                return {"statusCode": 400, "body": json.dumps({"error": f"'{field}' is required"})}
        push_id = body['push_id']
        username = body['username']
        push_table = dynamodb.Table(push_production_table_name)
        push_resp = push_table.get_item(Key={'push_id': push_id})
        if 'Item' not in push_resp:
            return {"statusCode": 404, "body": json.dumps({"error": f"Push record '{push_id}' not found."})}
        push_table.delete_item(Key={'push_id': push_id})
        log_transaction("DeletePushToProduction", {"push_id": push_id, "details": f"Push record '{push_id}' deleted"}, username)
        logger.info(f"Push record '{push_id}' deleted by {username}.")
        return {"statusCode": 200, "body": json.dumps({"message": f"Push record '{push_id}' deleted successfully."})}
    except Exception as e:
        logger.error(f"Error in delete_push_to_production: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def undo_action(body):
    try:
        username = body.get('username')
        if not username:
            return {"statusCode": 400, "body": json.dumps({"error": "'username' is required"})}
        if "undo_id" in body and body["undo_id"]:
            undo_id = body["undo_id"]
        else:
            table = dynamodb.Table(undo_table_name)
            response = table.scan(
                FilterExpression=Attr('username').eq(username) & Attr('status').eq('ACTIVE')
            )
            active_records = response.get('Items', [])
            if not active_records:
                return {"statusCode": 404, "body": json.dumps({"error": "No active undo records found for the user."})}
            active_records.sort(key=lambda r: r.get('timestamp', ''), reverse=True)
            undo_id = active_records[0]['undo_id']
        record = get_undo_record(undo_id)
        if not record:
            return {"statusCode": 404, "body": json.dumps({"error": "Undo record not found."})}
        if record['status'] != 'ACTIVE':
            return {"statusCode": 400, "body": json.dumps({"error": "This undo record is already undone."})}
        operation = record['operation']
        details = record['undo_details']
        if operation == "CreateStock":
            response = delete_stock({'name': details['item_id'], 'username': username})
        elif operation == "UpdateStock":
            old_state = details['old_state']
            update_body = {
                'name': details['item_id'],
                'quantity': old_state['total_quantity'],
                'defective': old_state['defective'],
                'cost_per_unit': float(old_state['cost_per_unit']),
                'stock_limit': old_state['stock_limit'],
                'username': username,
                'unit': old_state['unit']
            }
            response = update_stock(update_body)
        elif operation == "DeleteStock":
            stock_item = details['deleted_item']
            stock_table = dynamodb.Table(stock_table_name)
            stock_table.put_item(Item=stock_item)
            log_transaction("UndoDeleteStock", {"item_id": stock_item['item_id']}, username)
            response = {"statusCode": 200, "body": json.dumps({"message": f"Stock '{stock_item['item_id']}' restored successfully."})}
        elif operation == "AddStockQuantity":
            response = subtract_stock_quantity({'name': details['item_id'], 'quantity_to_subtract': details['quantity_added'], 'username': username})
        elif operation == "SubtractStockQuantity":
            response = add_stock_quantity({'name': details['item_id'], 'quantity_to_add': details['quantity_subtracted'], 'username': username})
        elif operation == "AddDefectiveGoods":
            response = subtract_defective_goods({'name': details['item_id'], 'defective_to_subtract': details['defective_added'], 'username': username})
        elif operation == "SubtractDefectiveGoods":
            response = add_defective_goods({'name': details['item_id'], 'defective_to_add': details['defective_subtracted'], 'username': username})
        elif operation == "CreateProduct":
            response = delete_product({'product_id': details['product_id'], 'username': username})
        elif operation == "UpdateProduct":
            old_state = details['old_state']
            update_body = {
                'product_id': details['product_id'],
                'product_name': old_state['product_name'],
                'stock_needed': old_state['stock_needed'],
                'username': username
            }
            response = update_product(update_body)
        elif operation == "DeleteProduct":
            product = details['deleted_product']
            production_table = dynamodb.Table(production_table_name)
            production_table.put_item(Item=product)
            log_transaction("UndoDeleteProduct", {"product_id": product['product_id']}, username)
            response = {"statusCode": 200, "body": json.dumps({"message": f"Product '{product['product_id']}' restored successfully."})}
        elif operation == "PushToProduction":
            response = undo_production({'push_id': details['push_id'], 'username': username})
        else:
            return {"statusCode": 400, "body": json.dumps({"error": "This operation is not undoable."})}
        mark_undo_as_done(undo_id)
        return response
    except Exception as e:
        logger.error(f"Error in undo_action: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

# =============================================================================
# REPORTS (DAILY/WEEKLY/MONTHLY) WITH CONSUMPTION SUMMARY (ONLY for AddDefectiveGoods & PushToProduction)
# =============================================================================

def get_daily_report(body):
    try:
        report_date = body.get('report_date', (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")).strip()
        logger.info(f"Daily report requested for report_date: {report_date}")

        transactions_table = dynamodb.Table(transactions_table_name)
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('date').eq(report_date)
        )
        transactions = tx_resp.get('Items', [])
        while 'LastEvaluatedKey' in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('date').eq(report_date),
                ExclusiveStartKey=tx_resp['LastEvaluatedKey']
            )
            transactions.extend(tx_resp.get('Items', []))
        for tx in transactions:
            tx['timestamp'] = format_ist_timestamp(tx.get('timestamp'))
        logger.info(f"Found {len(transactions)} transactions for report_date {report_date}")

        # Only consider consumption transactions from AddDefectiveGoods and PushToProduction
        consumption_details = extract_consumption_details(transactions)
        consumption_summary = summarize_consumption_details(consumption_details)
        consumption_amount = compute_consumption_amount(transactions)
        consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in consumption_details)

        opening_record = get_existing_stock_record("SaveOpeningStock", report_date)
        closing_record = get_existing_stock_record("SaveClosingStock", report_date)
        if opening_record and closing_record:
            opening_stock = opening_record.get('details', {})
            closing_stock = closing_record.get('details', {})
            stock_summary = {
                "opening_stock_qty": opening_stock.get("opening_stock_qty", 0),
                "opening_stock_amount": float(opening_stock.get("opening_stock_amount", 0)),
                "closing_stock_qty": closing_stock.get("closing_stock_qty", 0),
                "closing_stock_amount": float(closing_stock.get("closing_stock_amount", 0)),
                "consumption_qty": float(consumption_qty),
                "consumption_amount": float(consumption_amount)
            }
        else:
            stock_summary = {}

        transactions_by_operation = group_transactions_by_operation(transactions)
        for op_type in transactions_by_operation:
            for tx in transactions_by_operation[op_type]:
                tx['timestamp'] = format_ist_timestamp(tx.get('timestamp'))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "report_date": report_date,
                "stock_summary": stock_summary,
                "transactions_by_operation": transactions_by_operation,
                "consumption_details": consumption_details,
                "consumption_summary": consumption_summary
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_daily_report: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}


def get_weekly_report(body):
    try:
        start_date = body.get('start_date')
        end_date = body.get('end_date')
        if not start_date or not end_date:
            now = datetime.utcnow() + timedelta(hours=5, minutes=30)
            end_date = now.strftime("%Y-%m-%d")
            start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        start_date = start_date.strip()
        end_date = end_date.strip()
        logger.info(f"Weekly report requested for period: {start_date} to {end_date}")

        transactions_table = dynamodb.Table(transactions_table_name)
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('date').between(start_date, end_date)
        )
        transactions = tx_resp.get('Items', [])
        while 'LastEvaluatedKey' in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('date').between(start_date, end_date),
                ExclusiveStartKey=tx_resp['LastEvaluatedKey']
            )
            transactions.extend(tx_resp.get('Items', []))
        for tx in transactions:
            tx['timestamp'] = format_ist_timestamp(tx.get('timestamp'))
        logger.info(f"Found {len(transactions)} transactions for the weekly period.")

        daily_data = {}
        for tx in transactions:
            tx_date = tx.get('date')
            if tx_date:
                tx_date = tx_date.strip()
                if start_date <= tx_date <= end_date:
                    daily_data.setdefault(tx_date, []).append(tx)

        sorted_dates = sorted(daily_data.keys())
        daily_report = {}
        for tx_date in sorted_dates:
            txs = daily_data[tx_date]
            for tx in txs:
                tx['timestamp'] = format_ist_timestamp(tx.get('timestamp'))
            day_consumption_details = extract_consumption_details(txs)
            day_consumption_summary = summarize_consumption_details(day_consumption_details)
            day_consumption_amount = compute_consumption_amount(txs)
            day_consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in day_consumption_details)
            opening_record = get_existing_stock_record("SaveOpeningStock", tx_date)
            closing_record = get_existing_stock_record("SaveClosingStock", tx_date)
            if opening_record and closing_record:
                opening_stock = opening_record.get('details', {})
                closing_stock = closing_record.get('details', {})
                stock_summary = {
                    "opening_stock_qty": opening_stock.get("opening_stock_qty", 0),
                    "opening_stock_amount": float(opening_stock.get("opening_stock_amount", 0)),
                    "closing_stock_qty": closing_stock.get("closing_stock_qty", 0),
                    "closing_stock_amount": float(closing_stock.get("closing_stock_amount", 0)),
                    "consumption_qty": float(day_consumption_qty),
                    "consumption_amount": float(day_consumption_amount)
                }
            else:
                stock_summary = {}
            daily_report[tx_date] = {
                "stock_summary": stock_summary,
                "transactions": txs,
                "consumption_details": day_consumption_details,
                "consumption_summary": day_consumption_summary
            }

        overall_consumption_details = extract_consumption_details(transactions)
        overall_consumption_summary = summarize_consumption_details(overall_consumption_details)
        overall_consumption_amount = compute_consumption_amount(transactions)
        overall_consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in overall_consumption_details)

        overall_summary = {}
        dates_with_records = [d for d in sorted_dates if get_existing_stock_record("SaveOpeningStock", d) and get_existing_stock_record("SaveClosingStock", d)]
        if dates_with_records:
            overall_opening_record = get_existing_stock_record("SaveOpeningStock", dates_with_records[0])
            overall_closing_record = get_existing_stock_record("SaveClosingStock", dates_with_records[-1])
            if overall_opening_record and overall_closing_record:
                opening_stock = overall_opening_record.get('details', {})
                closing_stock = overall_closing_record.get('details', {})
                overall_summary = {
                    "opening_stock_qty": opening_stock.get("opening_stock_qty", 0),
                    "opening_stock_amount": float(opening_stock.get("opening_stock_amount", 0)),
                    "closing_stock_qty": closing_stock.get("closing_stock_qty", 0),
                    "closing_stock_amount": float(closing_stock.get("closing_stock_amount", 0)),
                    "consumption_qty": float(overall_consumption_qty),
                    "consumption_amount": float(overall_consumption_amount)
                }
        return {
            "statusCode": 200,
            "body": json.dumps({
                "report_period": {"start_date": start_date, "end_date": end_date},
                "overall_stock_summary": overall_summary,
                "daily_report": daily_report,
                "overall_consumption_details": overall_consumption_details,
                "overall_consumption_summary": overall_consumption_summary
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_weekly_report: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}


def get_monthly_report(body):
    try:
        if 'month' not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'month' parameter is required in format YYYY-MM"})}
        month_str = body['month'].strip()
        try:
            year, month = map(int, month_str.split('-'))
        except Exception as e:
            logger.error("Invalid month format")
            return {"statusCode": 400, "body": json.dumps({"error": "'month' must be in format YYYY-MM"})}
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        if year > now_ist.year or (year == now_ist.year and month > now_ist.month):
            logger.info("Requested month is in the future")
            return {"statusCode": 200, "body": json.dumps({
                "report_period": {"year": year, "month": month, "start_date": None, "end_date": None},
                "report": {}
            })}
        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        start_date_str = first_day.strftime("%Y-%m-%d")
        end_date_str = last_day.strftime("%Y-%m-%d")
        logger.info(f"Monthly report requested for {month_str} (range: {start_date_str} to {end_date_str})")

        transactions_table = dynamodb.Table(transactions_table_name)
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('date').between(start_date_str, end_date_str)
        )
        transactions = tx_resp.get('Items', [])
        while 'LastEvaluatedKey' in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('date').between(start_date_str, end_date_str),
                ExclusiveStartKey=tx_resp['LastEvaluatedKey']
            )
            transactions.extend(tx_resp.get('Items', []))
        for tx in transactions:
            tx['timestamp'] = format_ist_timestamp(tx.get('timestamp'))
        logger.info(f"Found {len(transactions)} transactions for the month.")

        daily_data = {}
        for tx in transactions:
            tx_date = tx.get('date')
            if tx_date:
                tx_date = tx_date.strip()
                daily_data.setdefault(tx_date, []).append(tx)
        sorted_dates = sorted(daily_data.keys())
        daily_report = {}
        for tx_date in sorted_dates:
            txs = daily_data[tx_date]
            for tx in txs:
                tx['timestamp'] = format_ist_timestamp(tx.get('timestamp'))
            day_consumption_details = extract_consumption_details(txs)
            day_consumption_summary = summarize_consumption_details(day_consumption_details)
            day_consumption_amount = compute_consumption_amount(txs)
            day_consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in day_consumption_details)
            opening_record = get_existing_stock_record("SaveOpeningStock", tx_date)
            closing_record = get_existing_stock_record("SaveClosingStock", tx_date)
            if opening_record and closing_record:
                opening_stock = opening_record.get('details', {})
                closing_stock = closing_record.get('details', {})
                stock_summary = {
                    "opening_stock_qty": opening_stock.get("opening_stock_qty", 0),
                    "opening_stock_amount": float(opening_stock.get("opening_stock_amount", 0)),
                    "closing_stock_qty": closing_stock.get("closing_stock_qty", 0),
                    "closing_stock_amount": float(closing_stock.get("closing_stock_amount", 0)),
                    "consumption_qty": float(day_consumption_qty),
                    "consumption_amount": float(day_consumption_amount)
                }
            else:
                stock_summary = {}
            daily_report[tx_date] = {
                "stock_summary": stock_summary,
                "transactions": txs,
                "consumption_details": day_consumption_details,
                "consumption_summary": day_consumption_summary
            }

        overall_consumption_details = extract_consumption_details(transactions)
        overall_consumption_summary = summarize_consumption_details(overall_consumption_details)
        overall_consumption_amount = compute_consumption_amount(transactions)
        overall_consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in overall_consumption_details)

        overall_summary = {}
        dates_with_records = [d for d in sorted_dates if get_existing_stock_record("SaveOpeningStock", d) and get_existing_stock_record("SaveClosingStock", d)]
        if dates_with_records:
            overall_opening_record = get_existing_stock_record("SaveOpeningStock", dates_with_records[0])
            overall_closing_record = get_existing_stock_record("SaveClosingStock", dates_with_records[-1])
            if overall_opening_record and overall_closing_record:
                opening_stock = overall_opening_record.get('details', {})
                closing_stock = overall_closing_record.get('details', {})
                overall_summary = {
                    "opening_stock_qty": opening_stock.get("opening_stock_qty", 0),
                    "opening_stock_amount": float(opening_stock.get("opening_stock_amount", 0)),
                    "closing_stock_qty": closing_stock.get("closing_stock_qty", 0),
                    "closing_stock_amount": float(closing_stock.get("closing_stock_amount", 0)),
                    "consumption_qty": float(overall_consumption_qty),
                    "consumption_amount": float(overall_consumption_amount)
                }
        return {
            "statusCode": 200,
            "body": json.dumps({
                "report_period": {"year": year, "month": month, "start_date": start_date_str, "end_date": end_date_str},
                "overall_stock_summary": overall_summary,
                "daily_report": daily_report,
                "overall_consumption_details": overall_consumption_details,
                "overall_consumption_summary": overall_consumption_summary
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_monthly_report: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

# =============================================================================
# NEW FUNCTIONS: SAVE OPENING AND CLOSING STOCK
# =============================================================================

def get_existing_stock_record(operation, report_date):
    try:
        report_date = report_date.strip()
        transactions_table = dynamodb.Table(transactions_table_name)
        resp = transactions_table.scan(
            FilterExpression=Attr('operation_type').eq(operation) & Attr('date').eq(report_date)
        )
        items = resp.get('Items', [])
        if items:
            logger.info(f"Found {len(items)} record(s) for {operation} on {report_date}")
            return min(items, key=lambda x: x.get('timestamp', ''))
        return None
    except Exception as e:
        logger.error(f"Error in get_existing_stock_record: {str(e)}")
        return None

def save_opening_stock(body):
    try:
        if 'username' not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'username' is required"})}
        username = body['username']
        current_qty, current_amount = get_current_stock_summary()
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        timestamp_str = now_ist.isoformat()
        report_date = now_ist.strftime("%Y-%m-%d").strip()
        details = {
            "opening_stock_qty": current_qty,
            "opening_stock_amount": current_amount
        }
        transactions_table = dynamodb.Table(transactions_table_name)
        existing = get_existing_stock_record("SaveOpeningStock", report_date)
        if existing:
            transactions_table.update_item(
                Key={'transaction_id': existing['transaction_id']},
                UpdateExpression="SET details = :d, #ts = :t, #dt = :r",
                ExpressionAttributeNames={'#ts': 'timestamp', '#dt': 'date'},
                ExpressionAttributeValues={
                    ':d': details,
                    ':t': timestamp_str,
                    ':r': report_date
                }
            )
            log_undo_action("SaveOpeningStock", details, username)
            logger.info(f"Updated opening stock for {username} on {report_date}: qty={current_qty}, amount={current_amount}")
            response_message = "Opening stock updated successfully."
        else:
            log_transaction("SaveOpeningStock", details, username)
            log_undo_action("SaveOpeningStock", details, username)
            logger.info(f"Saved opening stock for {username} on {report_date}: qty={current_qty}, amount={current_amount}")
            response_message = "Opening stock saved successfully."
        return {"statusCode": 200, "body": json.dumps({
            "message": response_message,
            "opening_stock_qty": current_qty,
            "opening_stock_amount": current_amount,
            "timestamp": timestamp_str
        }, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in save_opening_stock: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def save_closing_stock(body):
    try:
        if 'username' not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'username' is required"})}
        username = body['username']
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        closing_timestamp = now_ist.isoformat()
        closing_qty, closing_amount = get_current_stock_summary()
        report_date = now_ist.strftime("%Y-%m-%d").strip()
        transactions_table = dynamodb.Table(transactions_table_name)
        opening_record = get_existing_stock_record("SaveOpeningStock", report_date)
        if not opening_record:
            return {"statusCode": 400, "body": json.dumps({"error": "Opening stock not saved for today. Cannot calculate consumption."})}
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('timestamp').between(opening_record['timestamp'], closing_timestamp)
        )
        transactions = tx_resp.get('Items', [])
        while 'LastEvaluatedKey' in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('timestamp').between(opening_record['timestamp'], closing_timestamp),
                ExclusiveStartKey=tx_resp['LastEvaluatedKey']
            )
            transactions.extend(tx_resp.get('Items', []))
        opening_stock = get_existing_stock_record("SaveOpeningStock", report_date)
        if opening_stock:
            opening_qty = opening_stock.get('details', {}).get("opening_stock_qty", 0)
            opening_amount = opening_stock.get('details', {}).get("opening_stock_amount", Decimal('0'))
        else:
            opening_qty = 0
            opening_amount = Decimal('0')
        consumption_details = extract_consumption_details(transactions)
        consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in consumption_details)
        consumption_amount = compute_consumption_amount(transactions)
        details = {
            "closing_stock_qty": closing_qty,
            "closing_stock_amount": closing_amount,
            "consumption_qty": consumption_qty,
            "consumption_amount": consumption_amount
        }
        existing = get_existing_stock_record("SaveClosingStock", report_date)
        if existing:
            transactions_table.update_item(
                Key={'transaction_id': existing['transaction_id']},
                UpdateExpression="SET details = :d, #ts = :t, #dt = :r",
                ExpressionAttributeNames={'#ts': 'timestamp', '#dt': 'date'},
                ExpressionAttributeValues={
                    ':d': details,
                    ':t': closing_timestamp,
                    ':r': report_date
                }
            )
            log_undo_action("SaveClosingStock", details, username)
            logger.info(f"Updated closing stock for {username} on {report_date}: qty={closing_qty}, amount={closing_amount}, consumption_qty={consumption_qty}, consumption_amount={consumption_amount}")
            response_message = "Closing stock updated successfully."
        else:
            log_transaction("SaveClosingStock", details, username)
            log_undo_action("SaveClosingStock", details, username)
            logger.info(f"Saved closing stock for {username} on {report_date}: qty={closing_qty}, amount={closing_amount}, consumption_qty={consumption_qty}, consumption_amount={consumption_amount}")
            response_message = "Closing stock saved successfully."
        return {"statusCode": 200, "body": json.dumps({
            "message": response_message,
            "closing_stock_qty": closing_qty,
            "closing_stock_amount": float(closing_amount),
            "consumption_qty": float(consumption_qty),
            "consumption_amount": float(consumption_amount),
            "timestamp": closing_timestamp
        }, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in save_closing_stock: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

def get_all_stock_transactions(body):
    try:
        transactions_table = dynamodb.Table(transactions_table_name)
        items = []
        response = transactions_table.scan()
        items.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = transactions_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        items.sort(key=lambda x: x.get('timestamp', ''))
        logger.info(f"Retrieved {len(items)} stock transaction records.")
        return {"statusCode": 200, "body": json.dumps(items, cls=DecimalEncoder)}
    except Exception as e:
        logger.error(f"Error in get_all_stock_transactions: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}
#==============================================================================
def get_daily_consumption_summary(body):
    """
    Expects a payload with:
      "operation": "GetDailyConsumptionSummary"
      "report_date": "YYYY-MM-DD"  (optional; defaults to current IST date)
    Returns the daily consumption summary.
    """
    try:
        if body.get("operation") != "GetDailyConsumptionSummary":
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid operation for daily consumption summary."})}
        report_date = body.get("report_date")
        if not report_date:
            report_date = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")
        transactions_table = dynamodb.Table(transactions_table_name)
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('date').eq(report_date)
        )
        transactions = tx_resp.get("Items", [])
        while "LastEvaluatedKey" in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('date').eq(report_date),
                ExclusiveStartKey=tx_resp["LastEvaluatedKey"]
            )
            transactions.extend(tx_resp.get("Items", []))
        # Extract consumption transactions from AddDefectiveGoods and PushToProduction.
        consumption_details = extract_consumption_details(transactions)
        consumption_summary = summarize_consumption_details(consumption_details)
        consumption_amount = compute_consumption_amount(transactions)
        consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in consumption_details)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "report_date": report_date,
                "consumption_summary": consumption_summary,
                "total_consumption_quantity": float(consumption_qty),
                "total_consumption_amount": float(consumption_amount)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_daily_consumption_summary: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}


def get_weekly_consumption_summary(body):
    """
    Expects a payload with:
      "operation": "GetWeeklyConsumptionSummary"
      "start_date": "YYYY-MM-DD"
      "end_date": "YYYY-MM-DD"
    Returns the weekly consumption summary.
    """
    try:
        if body.get("operation") != "GetWeeklyConsumptionSummary":
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid operation for weekly consumption summary."})}
        start_date = body.get("start_date")
        end_date = body.get("end_date")
        if not start_date or not end_date:
            now = datetime.utcnow() + timedelta(hours=5, minutes=30)
            end_date = now.strftime("%Y-%m-%d")
            start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        start_date = start_date.strip()
        end_date = end_date.strip()
        transactions_table = dynamodb.Table(transactions_table_name)
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('date').between(start_date, end_date)
        )
        transactions = tx_resp.get("Items", [])
        while "LastEvaluatedKey" in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('date').between(start_date, end_date),
                ExclusiveStartKey=tx_resp["LastEvaluatedKey"]
            )
            transactions.extend(tx_resp.get("Items", []))
        # Extract consumption details over the period.
        consumption_details = extract_consumption_details(transactions)
        consumption_summary = summarize_consumption_details(consumption_details)
        consumption_amount = compute_consumption_amount(transactions)
        consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in consumption_details)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "start_date": start_date,
                "end_date": end_date,
                "consumption_summary": consumption_summary,
                "total_consumption_quantity": float(consumption_qty),
                "total_consumption_amount": float(consumption_amount)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_weekly_consumption_summary: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}


def get_monthly_consumption_summary(body):
    """
    Expects a payload with:
      "operation": "GetMonthlyConsumptionSummary"
      "month": "YYYY-MM"
    Returns the monthly consumption summary.
    """
    try:
        if body.get("operation") != "GetMonthlyConsumptionSummary":
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid operation for monthly consumption summary."})}
        if "month" not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "'month' parameter is required in format YYYY-MM"})}
        month_str = body["month"].strip()
        try:
            year, month = map(int, month_str.split("-"))
        except Exception as e:
            return {"statusCode": 400, "body": json.dumps({"error": "'month' must be in format YYYY-MM"})}
        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        start_date_str = first_day.strftime("%Y-%m-%d")
        end_date_str = last_day.strftime("%Y-%m-%d")
        transactions_table = dynamodb.Table(transactions_table_name)
        tx_resp = transactions_table.scan(
            FilterExpression=Attr('date').between(start_date_str, end_date_str)
        )
        transactions = tx_resp.get("Items", [])
        while "LastEvaluatedKey" in tx_resp:
            tx_resp = transactions_table.scan(
                FilterExpression=Attr('date').between(start_date_str, end_date_str),
                ExclusiveStartKey=tx_resp["LastEvaluatedKey"]
            )
            transactions.extend(tx_resp.get("Items", []))
        # Extract consumption details for the month.
        consumption_details = extract_consumption_details(transactions)
        consumption_summary = summarize_consumption_details(consumption_details)
        consumption_amount = compute_consumption_amount(transactions)
        consumption_qty = sum(Decimal(str(d.get("quantity_consumed", 0))) for d in consumption_details)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "month": month_str,
                "start_date": start_date_str,
                "end_date": end_date_str,
                "consumption_summary": consumption_summary,
                "total_consumption_quantity": float(consumption_qty),
                "total_consumption_amount": float(consumption_amount)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in get_monthly_consumption_summary: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal error: {str(e)}"})}

# =============================================================================
# LAMBDA HANDLER
# =============================================================================

def add_cors_headers(response):
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS, GET, POST, PUT, DELETE",
        "Access-Control-Allow-Headers": "Content-Type, access-control-allow-methods"
    }
    if not response.get("headers"):
        response["headers"] = {}
    response["headers"].update(cors_headers)
    return response

def lambda_handler(event, context):
    try:
        http_method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method", "")
        if http_method.upper() == "OPTIONS":
            return add_cors_headers({"statusCode": 200, "body": ""})
        initialize_tables()
        body = json.loads(event.get('body', '{}'))
        operation = body.get('operation')
        if not operation:
            return add_cors_headers({
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'operation' field"})
            })
        if operation == "GetDailyReport":
            response = get_daily_report(body)
        elif operation == "GetWeeklyReport":
            response = get_weekly_report(body)
        elif operation == "GetAllStockTransactions":
            response = get_all_stock_transactions(body)
        elif operation == "GetMonthlyReport":
            response = get_monthly_report(body)
        elif operation == "CreateStock":
            response = create_stock(body)
        elif operation == "UpdateStock":
            response = update_stock(body)
        elif operation == "DeleteStock":
            response = delete_stock(body)
        elif operation == "AddStockQuantity":
            response = add_stock_quantity(body)
        elif operation == "SubtractStockQuantity":
            response = subtract_stock_quantity(body)
        elif operation == "AddDefectiveGoods":
            response = add_defective_goods(body)
        elif operation == "SubtractDefectiveGoods":
            response = subtract_defective_goods(body)
        elif operation == "GetAllStocks":
            response = get_all_stocks(body)
        elif operation == "CreateProduct":
            response = create_product(body)
        elif operation == "UpdateProduct":
            response = update_product(body)
        elif operation == "DeleteProduct":
            response = delete_product(body)
        elif operation == "GetAllProducts":
            response = get_all_products(body)
        elif operation == "PushToProduction":
            response = push_to_production(body)
        elif operation == "UndoProduction":
            response = undo_production(body)
        elif operation == "DeletePushToProduction":
            response = delete_push_to_production(body)
        elif operation == "DeleteTransactionData":
            response = delete_transaction_data(body)
        elif operation == "AdminViewUsers":
            response = admin_view_users(body)
        elif operation == "AdminUpdateUser":
            response = admin_update_user(body)
        elif operation == "RegisterUser":
            response = register_user(body)
        elif operation == "LoginUser":
            response = login_user(body)
        elif operation == "UndoAction":
            response = undo_action(body)
        elif operation == "SaveOpeningStock":
            response = save_opening_stock(body)
        elif operation == "SaveClosingStock":
            response = save_closing_stock(body)
        elif operation == "GetAllPushToProduction":   
            response = get_all_push_to_production(body)
        elif operation == "GetDailyConsumptionSummary":
            response = get_daily_consumption_summary(body)
        elif operation == "GetWeeklyConsumptionSummary":
            response = get_weekly_consumption_summary(body)
        elif operation == "GetMonthlyConsumptionSummary":
            response = get_monthly_consumption_summary(body)    
        else:
            response = {"statusCode": 400, "body": json.dumps({"error": "Invalid operation"})}
        return add_cors_headers(response)
    except json.JSONDecodeError:
        return add_cors_headers({
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        })
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return add_cors_headers({
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal error: {str(e)}"})
        })
