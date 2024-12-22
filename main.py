from flask import Flask, request, jsonify
import boto3
import re
from decimal import Decimal
from botocore.exceptions import ClientError
from datetime import datetime 

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
# Create the Production table with a TotalCount attribute
production_table = create_table_if_not_exists(
    table_name='production',
    key_schema=[
        {'AttributeName': 'ProductName', 'KeyType': 'HASH'}  # Partition key
    ],
    attribute_definitions=[
        {'AttributeName': 'ProductName', 'AttributeType': 'S'}
    ],
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
    stock_limit = data.get('StockLimit')

    # Validation
    if not all([name, qty, unit, total_cost, stock_limit]):
        return jsonify({"error": "All fields (Name, Qty, Unit, TotalCost, StockLimit) are required"}), 400

    try:
        qty = Decimal(str(qty))
        total_cost = Decimal(str(total_cost))
        stock_limit = Decimal(str(stock_limit))
    except Exception as e:
        return jsonify({"error": f"Invalid Qty, TotalCost, or StockLimit: {str(e)}"}), 400

    if qty <= 0 or total_cost < 0 or stock_limit < 0:
        return jsonify({"error": "Qty must be > 0, TotalCost >= 0, and StockLimit >= 0"}), 400

    if stock_limit > qty:
        return jsonify({"error": "StockLimit cannot exceed the initial Qty"}), 400

    cost_per_unit = total_cost / qty

    # Save to DynamoDB
    try:
        stock_table.put_item(Item={
            'item_id': name,
            'Name': name,
            'Qty': qty,
            'Unit': unit,
            'TotalCost': total_cost,
            'CostPerUnit': cost_per_unit,
            'StockLimit': stock_limit
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
    - Updates Qty, TotalCost, StockLimit.
    - Dynamically recalculates CostPerUnit when Qty or TotalCost is updated.
    """
    # Get the JSON data from the request
    data = request.json

    # Extract fields
    qty = data.get('Qty')
    total_cost = data.get('TotalCost')
    stock_limit = data.get('StockLimit')

    # Retrieve the current data for the stock item from DynamoDB
    stock_item = stock_table.get_item(Key={'item_id': item_id}).get('Item')

    if not stock_item:
        return jsonify({"error": "Stock item not found"}), 404

    # Validate inputs
    try:
        if qty is not None:
            qty = Decimal(str(qty))
            if qty <= 0:
                return jsonify({"error": "Qty must be greater than zero"}), 400
        if total_cost is not None:
            total_cost = Decimal(str(total_cost))
            if total_cost < 0:
                return jsonify({"error": "TotalCost must be non-negative"}), 400
        if stock_limit is not None:
            stock_limit = Decimal(str(stock_limit))
            if stock_limit < 0:
                return jsonify({"error": "StockLimit must be non-negative"}), 400
            # Ensure Qty does not go below the updated StockLimit
            if qty is not None and qty < stock_limit:
                return jsonify({"error": f"Qty ({qty}) cannot be less than StockLimit ({stock_limit})"}), 400
            elif qty is None and stock_item['Qty'] < stock_limit:
                return jsonify({
                    "error": f"Existing Qty ({stock_item['Qty']}) cannot be less than the updated StockLimit ({stock_limit})"
                }), 400
    except Exception as e:
        return jsonify({"error": f"Invalid input: {str(e)}"}), 400

    # Retrieve existing values for calculations
    current_qty = stock_item.get('Qty', Decimal(1))  # Avoid division by zero
    current_total_cost = stock_item.get('TotalCost', Decimal(0))

    # Calculate updated CostPerUnit if applicable
    updated_qty = qty if qty is not None else current_qty
    updated_total_cost = total_cost if total_cost is not None else current_total_cost
    updated_cost_per_unit = updated_total_cost / updated_qty

    # Prepare the update expression
    update_expression = "SET "
    expression_attribute_values = {}

    if qty is not None:
        update_expression += "Qty = :qty, "
        expression_attribute_values[":qty"] = updated_qty
    if total_cost is not None:
        update_expression += "TotalCost = :total_cost, "
        expression_attribute_values[":total_cost"] = updated_total_cost
    if stock_limit is not None:
        update_expression += "StockLimit = :stock_limit, "
        expression_attribute_values[":stock_limit"] = stock_limit

    # Always update CostPerUnit
    update_expression += "CostPerUnit = :cost_per_unit"
    expression_attribute_values[":cost_per_unit"] = updated_cost_per_unit

    try:
        # Perform the update in DynamoDB
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
    data = request.json

    if not data.get('ProductName') or not data.get('Materials'):
        return jsonify({"error": "ProductName and Materials are required"}), 400

    product_name = data['ProductName']
    materials = data['Materials']

    # Sanitize the product name for DynamoDB table name
    sanitized_product_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', product_name)
    materials_table_name = f"materials_{sanitized_product_name}"

    for material in materials:
        if 'MaterialName' not in material or 'QtyRequired' not in material or 'CostPerUnit' not in material:
            return jsonify({"error": "Each material must have MaterialName, QtyRequired, and CostPerUnit"}), 400
        if Decimal(str(material['QtyRequired'])) <= 0 or Decimal(str(material['CostPerUnit'])) <= 0:
            return jsonify({"error": f"Invalid QtyRequired or CostPerUnit for material '{material['MaterialName']}'"}), 400

    # Create the materials table
    create_table_if_not_exists(
        table_name=materials_table_name,
        key_schema=[{'AttributeName': 'MaterialName', 'KeyType': 'HASH'}],
        attribute_definitions=[{'AttributeName': 'MaterialName', 'AttributeType': 'S'}],
        provisioned_throughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )

    # Add materials to the table
    materials_table = dynamodb.Table(materials_table_name)
    try:
        with materials_table.batch_writer() as batch:
            for material in materials:
                batch.put_item(Item={
                    'MaterialName': material['MaterialName'],
                    'QtyRequired': Decimal(str(material['QtyRequired'])),
                    'CostPerUnit': Decimal(str(material['CostPerUnit']))
                })
    except Exception as e:
        return jsonify({"error": f"Failed to populate materials table: {str(e)}"}), 500

    # Calculate total cost
    total_cost = sum(
        Decimal(str(material['QtyRequired'])) * Decimal(str(material['CostPerUnit']))
        for material in materials
    )

    # Add product to the products table
    try:
        products_table.put_item(Item={
            'ProductName': product_name,
            'MaterialsTable': materials_table_name,
            'Cost': total_cost,
            'Timestamp': datetime.now().isoformat()
        })

        return jsonify({
            "message": f"Product '{product_name}' created successfully.",
            "Cost": float(total_cost)
        }), 201
    except Exception as e:
        return jsonify({"error": f"Failed to create product: {str(e)}"}), 500

@app.route('/api/products/recalculate', methods=['POST'])
def recalculate_max_products_and_cost():
    """
    Recalculates the maximum number of products that can be created
    and updates the total production cost dynamically in the products table.
    """
    data = request.json
    if not data or not data.get('ProductName'):
        return jsonify({"error": "ProductName is required."}), 400

    product_name = data['ProductName']
    product = products_table.get_item(Key={'ProductName': product_name}).get('Item')
    if not product:
        return jsonify({"error": f"Product '{product_name}' not found"}), 404

    materials_table_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f"materials_{product_name}")
    try:
        # Fetch all materials for the product
        materials_table = dynamodb.Table(materials_table_name)
        materials = materials_table.scan().get('Items', [])
    except Exception as e:
        return jsonify({"error": f"Error accessing materials table: {str(e)}"}), 500

    if not materials:
        return jsonify({"error": "No materials found for the product."}), 400

    # Initialize variables for calculations
    max_products_can_be_created = float('inf')
    total_cost = Decimal('0.00')
    insufficient_materials = []

    for material in materials:
        material_name = material['MaterialName']
        qty_required = Decimal(str(material['QtyRequired']))
        cost_per_unit = Decimal(str(material.get('CostPerUnit', 0)))  # Default cost is 0 if missing
        stock_item = stock_table.get_item(Key={'item_id': material_name}).get('Item')

        # Calculate maximum possible products based on available stock
        if stock_item:
            qty_available = Decimal(str(stock_item['Qty']))
            max_possible = qty_available // qty_required
            max_products_can_be_created = min(max_products_can_be_created, max_possible)
        else:
            insufficient_materials.append({
                "MaterialName": material_name,
                "AvailableQty": 0,
                "RequiredQty": qty_required
            })
            max_products_can_be_created = 0

        # Accumulate total cost for production
        total_cost += qty_required * cost_per_unit

    # Update the products table with recalculated max products and total cost
    try:
        products_table.update_item(
            Key={'ProductName': product_name},
            UpdateExpression="SET MaxProductsCanBeCreated = :max_products, Cost = :cost",
            ExpressionAttributeValues={
                ":max_products": int(max_products_can_be_created),
                ":cost": total_cost
            }
        )
    except Exception as e:
        return jsonify({"error": f"Failed to update product: {str(e)}"}), 500

    # Prepare the response
    response = {
        "message": f"Max products and cost recalculated for '{product_name}'.",
        "MaxProductsCanBeCreated": int(max_products_can_be_created),
        "TotalCost": float(total_cost)
    }
    if insufficient_materials:
        response["InsufficientMaterials"] = insufficient_materials

    return jsonify(response), 200


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
@app.route('/api/production/push', methods=['POST'])
def push_to_production():
    """
    API to push a product for production.
    - Validates the requested quantity.
    - Deducts materials from stock and updates the products table.
    - Records the production in the production table.
    """
    try:
        data = request.get_json()
        product_name = data.get('ProductName')
        quantity = data.get('Quantity')

        if not product_name or quantity is None:
            return jsonify({"error": "ProductName and Quantity are required."}), 400

        # Convert quantity to Decimal
        try:
            quantity = Decimal(str(quantity))
            if quantity <= 0:
                return jsonify({"error": "Quantity must be greater than zero."}), 400
        except (ValueError, InvalidOperation):
            return jsonify({"error": "Invalid quantity. Must be a numeric value."}), 400

        # Fetch product details
        product_details = products_table.get_item(Key={'ProductName': product_name}).get('Item')
        if not product_details:
            return jsonify({"error": f"Product '{product_name}' not found."}), 404

        materials_table_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f"materials_{product_name}")
        try:
            materials_table = dynamodb.Table(materials_table_name)
            materials = materials_table.scan().get('Items', [])
        except Exception as e:
            return jsonify({"error": f"Error accessing materials table for product '{product_name}': {str(e)}"}), 500

        if not materials:
            return jsonify({"error": "No materials found for the product."}), 400

        max_products = Decimal(product_details.get('MaxProductsCanBeCreated', 0))
        total_produced_qty = Decimal(product_details.get('TotalProducedQty', 0))  # Default to 0
        if quantity > max_products:
            return jsonify({
                "error": f"Requested quantity exceeds the maximum products that can be created. Max: {max_products}."
            }), 400

        insufficient_materials = []
        total_production_cost = Decimal('0')

        # Check stock and deduct materials
        for material in materials:
            material_name = material['MaterialName']
            qty_required_per_product = Decimal(str(material['QtyRequired']))
            cost_per_unit = Decimal(str(material['CostPerUnit']))

            total_qty_required = qty_required_per_product * quantity
            total_cost = total_qty_required * cost_per_unit
            total_production_cost += total_cost

            stock_item = stock_table.get_item(Key={'item_id': material_name}).get('Item')
            if not stock_item:
                insufficient_materials.append({
                    "MaterialName": material_name,
                    "AvailableQty": 0,
                    "RequiredQty": float(total_qty_required)
                })
                continue

            available_qty = Decimal(str(stock_item['Qty']))
            if available_qty < total_qty_required:
                insufficient_materials.append({
                    "MaterialName": material_name,
                    "AvailableQty": float(available_qty),
                    "RequiredQty": float(total_qty_required)
                })
            else:
                new_qty = available_qty - total_qty_required
                stock_table.update_item(
                    Key={'item_id': material_name},
                    UpdateExpression="SET Qty = :new_qty",
                    ExpressionAttributeValues={':new_qty': new_qty}
                )

        if insufficient_materials:
            return jsonify({
                "error": "Insufficient materials for production.",
                "InsufficientMaterials": insufficient_materials
            }), 400

        # Update product's MaxProductsCanBeCreated and TotalProducedQty
        new_max_products = max_products - quantity
        new_total_produced_qty = total_produced_qty + quantity
        products_table.update_item(
            Key={'ProductName': product_name},
            UpdateExpression="""
                SET MaxProductsCanBeCreated = :new_max,
                    TotalProducedQty = :new_produced,
                    Cost = :new_cost
            """,
            ExpressionAttributeValues={
                ':new_max': new_max_products,
                ':new_produced': new_total_produced_qty,
                ':new_cost': total_production_cost
            }
        )

        # Record production
        production_table.put_item(
            Item={
                'ProductName': product_name,
                'Quantity': quantity,
                'ProductionCost': total_production_cost,
                'Timestamp': datetime.now().isoformat()
            }
        )

        return jsonify({
            "message": f"{float(quantity)} '{product_name}' pushed for production successfully.",
            "ProductionCost": float(total_production_cost),
            "RemainingMaxProducts": float(new_max_products)
        }), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/api/production/undo', methods=['POST'])
def undo_production():
    """
    API to undo a production operation.
    - Restores stock levels for materials.
    - Updates the MaxProductsCanBeCreated and TotalProducedQty counts in the products table.
    - Deducts the proportional production cost.
    - Removes the production record from the production table.
    """
    try:
        data = request.get_json()
        product_name = data.get('ProductName')
        quantity = data.get('Quantity')

        if not product_name or quantity is None:
            return jsonify({"error": "ProductName and Quantity are required."}), 400

        # Convert quantity to Decimal
        try:
            quantity = Decimal(str(quantity))
            if quantity <= 0:
                return jsonify({"error": "Quantity must be greater than zero."}), 400
        except (ValueError, InvalidOperation):
            return jsonify({"error": "Invalid Quantity. Must be a numeric value."}), 400

        # Fetch production record
        production_record = production_table.get_item(
            Key={'ProductName': product_name}
        ).get('Item')

        if not production_record:
            return jsonify({"error": f"No production record found for '{product_name}'."}), 404

        # Get production quantity and cost
        produced_quantity = Decimal(str(production_record.get('Quantity', 0)))
        production_cost = Decimal(str(production_record.get('ProductionCost', 0)))

        if produced_quantity == 0 or quantity > produced_quantity:
            return jsonify({
                "error": f"Cannot undo production. Only {produced_quantity} products have been produced."
            }), 400

        # Calculate cost reduction
        cost_per_unit = production_cost / produced_quantity
        cost_to_reduce = cost_per_unit * quantity

        # Fetch product details
        product_details = products_table.get_item(Key={'ProductName': product_name}).get('Item')
        if not product_details:
            return jsonify({"error": f"Product '{product_name}' not found."}), 404

        # Sanitize table name
        materials_table_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f"materials_{product_name}")
        try:
            materials_table = dynamodb.Table(materials_table_name)
            materials = materials_table.scan().get('Items', [])
        except Exception as e:
            return jsonify({"error": f"Error accessing materials: {str(e)}"}), 500

        if not materials:
            return jsonify({"error": "No materials found for the product."}), 400

        # Restore stock
        for material in materials:
            material_name = material['MaterialName']
            qty_required_per_product = Decimal(str(material['QtyRequired']))

            total_qty_to_restore = qty_required_per_product * quantity

            # Fetch stock item
            stock_item = stock_table.get_item(Key={'item_id': material_name}).get('Item')
            if not stock_item:
                return jsonify({
                    "error": f"Stock item '{material_name}' not found during restoration."
                }), 400

            new_qty = Decimal(str(stock_item['Qty'])) + total_qty_to_restore
            stock_table.update_item(
                Key={'item_id': material_name},
                UpdateExpression="SET Qty = :new_qty",
                ExpressionAttributeValues={':new_qty': new_qty}
            )

        # Update product details
        current_max_products = Decimal(str(product_details.get('MaxProductsCanBeCreated', 0)))
        current_produced_qty = Decimal(str(product_details.get('TotalProducedQty', 0)))
        current_cost = Decimal(str(product_details.get('Cost', 0)))

        updated_max_products = current_max_products + quantity
        updated_total_produced_qty = current_produced_qty - quantity
        updated_cost = current_cost - cost_to_reduce

        products_table.update_item(
            Key={'ProductName': product_name},
            UpdateExpression="""
                SET MaxProductsCanBeCreated = :updated_max,
                    TotalProducedQty = :updated_qty,
                    Cost = :updated_cost
            """,
            ExpressionAttributeValues={
                ':updated_max': updated_max_products,
                ':updated_qty': updated_total_produced_qty,
                ':updated_cost': updated_cost
            }
        )

        # Update production record or delete it if quantity is fully undone
        remaining_quantity = produced_quantity - quantity
        if remaining_quantity > 0:
            new_production_cost = cost_per_unit * remaining_quantity
            production_table.update_item(
                Key={'ProductName': product_name},
                UpdateExpression="SET Quantity = :remaining_qty, ProductionCost = :new_cost",
                ExpressionAttributeValues={
                    ':remaining_qty': remaining_quantity,
                    ':new_cost': new_production_cost
                }
            )
        else:
            production_table.delete_item(Key={'ProductName': product_name})

        return jsonify({
            "message": f"Successfully undid the production of {quantity} '{product_name}'.",
            "RestoredMaterials": materials,
            "UpdatedMaxProducts": float(updated_max_products),
            "UpdatedTotalProducedQty": float(updated_total_produced_qty),
            "DeductedCost": float(cost_to_reduce),
            "RemainingQuantity": float(remaining_quantity)
        }), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
@app.route('/api/stock/check', methods=['GET'])
def check_low_stock_levels():
    """
    API to check for low stock levels.
    - Identifies items in the stock table with quantities below their StockLimit.
    - Returns details about the items with low stock.
    """
    try:
        # Fetch all stock items from the stock table
        stock_items = stock_table.scan().get('Items', [])
        
        if not stock_items:
            return jsonify({"message": "No stock items found."}), 404

        low_stock_items = []

        # Iterate through stock items to check levels
        for item in stock_items:
            item_name = item['item_id']
            current_qty = Decimal(str(item.get('Qty', 0)))
            stock_limit = Decimal(str(item.get('StockLimit', 0)))  # Assuming 'StockLimit' is defined

            # Identify low stock items
            if current_qty < stock_limit:
                low_stock_items.append({
                    "ItemName": item_name,
                    "CurrentQuantity": float(current_qty),
                    "StockLimit": float(stock_limit),
                    "Deficit": float(stock_limit - current_qty)  # Calculate how much is needed to reach the limit
                })

        if not low_stock_items:
            return jsonify({"message": "All stock levels are sufficient."}), 200

        return jsonify({
            "message": "Low stock levels detected.",
            "LowStockItems": low_stock_items
        }), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


# Start the Flask application
if __name__ == '__main__':
    app.run(debug=True)



