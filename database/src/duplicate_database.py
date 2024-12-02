# phd_package/database/src/duplicate_database.py

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from database.api.api_client import APIClient
from database.src.models import Base
from database.src.schema_to_model import SchemaToModelConverter


def validate_sensor_schema(schema: dict, sensor_type: str) -> bool:
    """
    Validates that a sensor schema follows the expected format:
    - Must be a dict with 'properties' containing 'sensors'
    - 'sensors' must be an array type with 'items'
    - 'items' must contain 'properties' with expected sensor fields
    """
    try:
        # Check basic structure
        if not isinstance(schema, dict) or 'properties' not in schema:
            raise ValueError("Schema missing 'properties'")
        
        sensors = schema['properties'].get('sensors', {})
        if not isinstance(sensors, dict) or sensors.get('type') != 'array' or 'items' not in sensors:
            raise ValueError("Invalid 'sensors' array structure")

        items = sensors['items']
        if not isinstance(items, dict) or 'properties' not in items:
            raise ValueError("Invalid 'items' structure")

        # Define expected properties for a sensor
        expected_properties = {
            'id': {'type': 'integer'},
            'value': {'type': 'number'},
            'timestamp': {'type': 'string', 'format': 'date-time'}
        }

        # Check if schema matches expected structure
        properties = items['properties']
        for prop, expected_type in expected_properties.items():
            if prop not in properties:
                raise ValueError(f"Missing required property: {prop}")
            if properties[prop].get('type') != expected_type['type']:
                raise ValueError(f"Invalid type for {prop}")

        return True

    except ValueError as e:
        print(f"Schema validation failed for {sensor_type}: {str(e)}")
        return False

def construct_table_schemas():
    """Construct the table schemas from the API data"""
    client = APIClient("database/api/config.yml")
    list_of_sensor_types = client.get_list_of_sensor_types()
    converter = SchemaToModelConverter()
    models = {}
    unexpected_schemas = []

    for sensor_type in list_of_sensor_types[:10]:
        try:
            client.update_sensor(sensor_type=sensor_type)
            schema = client.get_schema()
            
            print(f"\nProcessing schema for {sensor_type}")
            print(f"DEBUG: Schema for {sensor_type}: {schema}")

            # Validate schema structure
            if not validate_sensor_schema(schema, sensor_type):
                unexpected_schemas.append(sensor_type)
                continue

            # Extract the actual sensor properties
            sensor_properties = schema['properties']['sensors']['items']['properties']
            
            # Create a simplified schema for the converter
            simplified_schema = {
                'type': 'object',
                'properties': sensor_properties,
                'required': schema['properties']['sensors']['items'].get('required', [])
            }

            sensor_models = converter.convert_schema(simplified_schema, f"{sensor_type.replace(' ', '')}Sensor")
            models.update(sensor_models)
            print(f"Successfully processed schema for {sensor_type}")
            
        except Exception as e:
            print(f"Error processing sensor type {sensor_type}: {str(e)}")
            unexpected_schemas.append(sensor_type)
            continue
    
    if unexpected_schemas:
        print("\nWarning: The following sensor types had unexpected schema structures:")
        for sensor_type in unexpected_schemas:
            print(f"- {sensor_type}")
            
    return models, unexpected_schemas

def create_all_tables(engine):
    """Create all tables in the database"""
    models, unexpected_schemas = construct_table_schemas()
    Base.metadata.create_all(engine)
    return models, unexpected_schemas

if __name__ == "__main__":
    construct_table_schemas()
    # from database.src.database import engine
    # create_all_tables(engine)
