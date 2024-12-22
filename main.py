from flask import Flask, request, jsonify
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

# Initialize Flask app
app = Flask(__name__)

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Helper function to create a table if it doesn't exist
def create_table_if_not_exists(table_name, key_schema, attribute_definitions, provisioned_throughput):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput
        )
        table.wait_until_exists()
        print(f"Table '{table_name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table '{table_name}' already exists.")
        else:
            print(f"Failed to create table '{table_name}': {str(e)}")
    return dynamodb.Table(table_name)

# Define the tables
stock_table = create_table_if_not_exists(
    table_name='stock',
    key_schema=[{'AttributeName': 'item_id', 'KeyType': 'HASH'}],
    attribute_definitions=[{'AttributeName': 'item_id', 'AttributeType': 'S'}],
    provisioned_throughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
)

products_table = create_table_if_not_exists(
    table_name='products',
    key_schema=[{'AttributeName': 'ProductName', 'KeyType': 'HASH'}],
    attribute_definitions=[{'AttributeName': 'ProductName', 'AttributeType': 'S'}],
    provisioned_throughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
)
materials_table_name = "Table_materials"
materials_table = create_table_if_not_exists(
    table_name=materials_table_name,
    key_schema=[{'AttributeName': 'MaterialName', 'KeyType': 'HASH'}],
    attribute_definitions=[{'AttributeName': 'MaterialName', 'AttributeType': 'S'}],
    provisioned_throughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
)
@app.route('/api/stock/create', methods=['POST'])
def create_stock():
    data = request.json

    # Extract fields
    name = data.get('Name')
    qty = data.get('Qty')
    unit = data.get('Unit')
    total_cost = data.get('TotalCost')

    # Validation
    if not all([name, qty, unit, total_cost]):
        return jsonify({"error": "All fields (Name, Qty, Unit, TotalCost) are required"}), 400

    try:
        qty = Decimal(str(qty))
        total_cost = Decimal(str(total_cost))
    except Exception as e:
        return jsonify({"error": f"Invalid Qty or TotalCost: {str(e)}"}), 400

    if qty <= 0 or total_cost < 0:
        return jsonify({"error": "Qty must be > 0 and TotalCost must be >= 0"}), 400

    cost_per_unit = total_cost / qty

    # Save to DynamoDB
    try:
        stock_table.put_item(Item={
            'item_id': name,
            'Name': name,
            'Qty': qty,
            'Unit': unit,
            'TotalCost': total_cost,
            'CostPerUnit': cost_per_unit
        })
        return jsonify({"message": "Stock item created successfully"}), 201
    except Exception as e:
        return jsonify({"error": f"Failed to save item: {str(e)}"}), 500


@app.route('/api/stock/get/<item_id>', methods=['GET'])
def get_stock(item_id):
    """
    Endpoint to retrieve a stock item by its item_id (Name).
    - Fetches the item from DynamoDB using the provided item_id.
    - If the item exists, returns it as a JSON response. If not, returns a 404 error.
    """
    
    try:
        # Fetch the item from DynamoDB by item_id (Name)
        response = stock_table.get_item(Key={'item_id': item_id})
        item = response.get('Item')

        # If item is not found, return a 404 error
        if not item:
            return jsonify({"error": "Item not found"}), 404
        
        # Return the item as a JSON response
        return jsonify(item), 200
    except Exception as e:
        return jsonify({"error": f"Failed to fetch item: {str(e)}"}), 500


@app.route('/api/stock/update/<item_id>', methods=['PUT'])
def update_stock(item_id):
    """
    Endpoint to update an existing stock item.
    - Allows updating: Qty and TotalCost.
    - If Qty and TotalCost are updated, it automatically calculates CostPerUnit.
    - Ensures that the Unit cannot be changed after creation.
    - The API validates the input and updates the item in DynamoDB.
    """
    
    # Get the JSON data from the request
    data = request.json
    
    # Extract the fields that can be updated
    qty = data.get('Qty')
    unit = data.get('Unit')
    total_cost = data.get('TotalCost')

    # Retrieve the current data for the stock item from DynamoDB
    # Assuming you have a method to fetch the item from the stock table using item_id
    stock_item = stock_table.get_item(Key={'item_id': item_id}).get('Item')

    if not stock_item:
        return jsonify({"error": "Stock item not found"}), 404
    
    # Ensure that Unit cannot be modified
    if unit and unit != stock_item['Unit']:
        return jsonify({"error": "Unit cannot be changed"}), 400

    # Validate that at least one field is provided for updating
    if not any([qty, total_cost]):
        return jsonify({"error": "At least one field (Qty, TotalCost) must be provided to update"}), 400

    # Validate the input values for Qty and TotalCost
    try:
        if qty:
            qty = Decimal(str(qty))
        if total_cost:
            total_cost = Decimal(str(total_cost))
    except Exception as e:
        return jsonify({"error": f"Invalid input for Qty or TotalCost: {str(e)}"}), 400
    
    # Ensure that Qty is positive and TotalCost is non-negative
    if qty and qty <= 0:
        return jsonify({"error": "Qty must be greater than zero"}), 400
    if total_cost and total_cost < 0:
        return jsonify({"error": "TotalCost must be non-negative"}), 400

    # Calculate CostPerUnit if both Qty and TotalCost are provided
    if qty and total_cost:
        cost_per_unit = total_cost / qty
    else:
        cost_per_unit = None  # No need to update if CostPerUnit is not required

    # Prepare the update expression for DynamoDB
    update_expression = "SET "
    expression_attribute_values = {}

    # Add fields that are being updated to the update expression
    if qty:
        update_expression += "Qty = :qty, "
        expression_attribute_values[":qty"] = qty
    if total_cost:
        update_expression += "TotalCost = :total_cost, "
        expression_attribute_values[":total_cost"] = total_cost
    if cost_per_unit is not None:
        update_expression += "CostPerUnit = :cost_per_unit, "
        expression_attribute_values[":cost_per_unit"] = cost_per_unit
    
    # Remove trailing comma
    update_expression = update_expression.rstrip(', ')

    try:
        # Perform the update operation in DynamoDB
        stock_table.update_item(
            Key={'item_id': item_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        return jsonify({"message": "Stock item updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to update item: {str(e)}"}), 500

@app.route('/api/stock/delete/<item_id>', methods=['DELETE'])
def delete_stock(item_id):
    """
    Endpoint to delete a stock item from the database by its item_id.
    - Deletes the stock item from DynamoDB using the provided item_id.
    - Returns a success message upon successful deletion.
    """
    
    try:
        # Delete the stock item from DynamoDB by item_id
        stock_table.delete_item(Key={'item_id': item_id})
        return jsonify({"message": "Stock item deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to delete item: {str(e)}"}), 500

@app.route('/api/products/create', methods=['POST'])
def create_product():
    """
    Create a new product and its materials table.
    """
    data = request.json

    # Validate input data
    if not data.get('ProductName') or not data.get('Materials'):
        return jsonify({"error": "ProductName and Materials are required"}), 400

    product_name = data['ProductName']
    materials = data['Materials']

    # Validate materials input
    for material in materials:
        if 'MaterialName' not in material or 'QtyRequired' not in material:
            return jsonify({"error": "Each material must have MaterialName and QtyRequired"}), 400
        if Decimal(str(material['QtyRequired'])) <= 0:
            return jsonify({"error": f"Invalid QtyRequired for material '{material['MaterialName']}'"}), 400

    # Create a new table for product materials
    materials_table_name = f"materials_{product_name}"
    create_table_if_not_exists(
        table_name=materials_table_name,
        key_schema=[{'AttributeName': 'MaterialName', 'KeyType': 'HASH'}],
        attribute_definitions=[{'AttributeName': 'MaterialName', 'AttributeType': 'S'}],
        provisioned_throughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )

    # Add materials to the materials_<ProductName> table
    materials_table = dynamodb.Table(materials_table_name)
    try:
        with materials_table.batch_writer() as batch:
            for material in materials:
                batch.put_item(Item={
                    'MaterialName': material['MaterialName'],
                    'QtyRequired': Decimal(str(material['QtyRequired']))
                })
    except Exception as e:
        return jsonify({"error": f"Failed to populate materials table: {str(e)}"}), 500

    # Save product to the products table
    try:
        products_table.put_item(Item={
            'ProductName': product_name,
            'MaterialsTable': materials_table_name
        })

        return jsonify({
            "message": f"Product '{product_name}' created successfully."
        }), 201
    except Exception as e:
        return jsonify({"error": f"Failed to create product: {str(e)}"}), 500
@app.route('/api/products/recalculate', methods=['POST'])
def recalculate_max_products():
    data = request.json
    if not data or not data.get('ProductName'):
        return jsonify({"error": "ProductName is required."}), 400

    product_name = data['ProductName']
    product = products_table.get_item(Key={'ProductName': product_name}).get('Item')
    if not product:
        return jsonify({"error": f"Product '{product_name}' not found"}), 404

    materials_table_name = f"materials_{product_name}"
    try:
        materials_table = dynamodb.Table(materials_table_name)
        materials = materials_table.scan().get('Items', [])
    except Exception as e:
        return jsonify({"error": f"Error accessing materials: {str(e)}"}), 500

    if not materials:
        return jsonify({"error": "No materials found"}), 400

    max_products_can_be_created = float('inf')
    insufficient_materials = []

    for material in materials:
        material_name = material['MaterialName']
        qty_required = Decimal(str(material['QtyRequired']))
        stock_item = stock_table.get_item(Key={'item_id': material_name}).get('Item')

        if not stock_item:
            insufficient_materials.append({
                "MaterialName": material_name,
                "AvailableQty": 0,
                "RequiredQty": qty_required
            })
            max_products_can_be_created = 0
            break

        qty_available = Decimal(str(stock_item['Qty']))
        max_possible = qty_available // qty_required
        max_products_can_be_created = min(max_products_can_be_created, max_possible)

        if qty_available < qty_required:
            insufficient_materials.append({
                "MaterialName": material_name,
                "AvailableQty": qty_available,
                "RequiredQty": qty_required
            })
            max_products_can_be_created = 0
            break

    try:
        products_table.update_item(
            Key={'ProductName': product_name},
            UpdateExpression="SET MaxProductsCanBeCreated = :max_products",
            ExpressionAttributeValues={":max_products": int(max_products_can_be_created)}
        )
    except Exception as e:
        return jsonify({"error": f"Failed to update product: {str(e)}"}), 500

    return jsonify({
        "message": f"Max products calculated for '{product_name}'",
        "MaxProductsCanBeCreated": int(max_products_can_be_created),
        "InsufficientMaterials": insufficient_materials
    }), 200

@app.route('/api/stock/all', methods=['GET'])
def get_all_stock():
    """
    Endpoint to retrieve all data from the Stock table.
    - Fetches all items using a scan operation.
    - Returns a JSON response with all items or an error message if the operation fails.
    """
    try:
        # Ensure the stock_table is initialized
        if not stock_table:
            return jsonify({"error": "Stock table is not initialized correctly."}), 500

        # Perform a scan operation to fetch all items in the stock table
        response = stock_table.scan()
        items = response.get('Items', [])

        # Return all items as a JSON response
        return jsonify({
            "message": "Stock table data retrieved successfully.",
            "data": items
        }), 200
    except ClientError as e:
        # Handle AWS DynamoDB client errors
        return jsonify({"error": f"DynamoDB ClientError: {e.response['Error']['Message']}"}), 500
    except Exception as e:
        # Handle any other exceptions
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

# Start the Flask application
if __name__ == '__main__':
    app.run(debug=True)




