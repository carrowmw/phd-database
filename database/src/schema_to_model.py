from typing import Dict, Any, List
import inflect
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
p = inflect.engine()

class SchemaToModelConverter:
    """Converts JSON schemas to SQLAlchemy models."""
    
    TYPE_MAPPING = {
        'integer': Integer,
        'number': Float,
        'string': String,
        'boolean': Boolean,
        'object': None,  # Handled separately
        'array': None,   # Handled separately
    }

    def __init__(self):
        self.models = {}
        self.relationships = []

    def _sanitize_name(self, name: str) -> str:
        """Convert name to valid Python class/variable name."""
        return ''.join(c if c.isalnum() else '_' for c in name)

    def _create_table_name(self, model_name: str) -> str:
        """Create standardized table names."""
        # Convert CamelCase to snake_case and pluralize
        name = ''.join(['_' + c.lower() if c.isupper() else c for c in model_name]).lstrip('_')
        return p.plural(name)

    def _handle_property(self, prop_name: str, prop_schema: Dict[str, Any], parent_name: str = None) -> Column:
        """Convert a JSON schema property to a SQLAlchemy Column."""
        if 'type' not in prop_schema:
            return None

        prop_type = prop_schema['type']
        nullable = prop_name not in prop_schema.get('required', [])

        if prop_type in self.TYPE_MAPPING and self.TYPE_MAPPING[prop_type]:
            return Column(self._sanitize_name(prop_name), 
                        self.TYPE_MAPPING[prop_type], 
                        nullable=nullable)
        
        elif prop_type == 'array':
            # Create a new model for array items
            item_model_name = f"{parent_name}{self._sanitize_name(prop_name)}Item"
            self._create_model_from_schema(item_model_name, prop_schema['items'])
            self.relationships.append((parent_name, item_model_name, True))
            return None

        elif prop_type == 'object':
            # Create a new model for nested objects
            nested_model_name = f"{parent_name}{self._sanitize_name(prop_name)}"
            self._create_model_from_schema(nested_model_name, prop_schema)
            self.relationships.append((parent_name, nested_model_name, False))
            return None

        return None

    def _create_model_from_schema(self, model_name: str, schema: Dict[str, Any]):
        """Create a SQLAlchemy model from a JSON schema."""
        if model_name in self.models:
            return

        attrs = {
            '__tablename__': self._create_table_name(model_name),
            'id': Column(Integer, primary_key=True),
        }

        properties = schema.get('properties', {})
        for prop_name, prop_schema in properties.items():
            column = self._handle_property(prop_name, prop_schema, model_name)
            if column:
                attrs[self._sanitize_name(prop_name)] = column

        # Create the model class
        self.models[model_name] = type(model_name, (Base,), attrs)

    def _add_relationships(self):
        """Add relationships between models after all models are created."""
        for parent_name, child_name, is_array in self.relationships:
            if parent_name in self.models and child_name in self.models:
                parent_model = self.models[parent_name]
                child_model = self.models[child_name]
                
                # Add foreign key to child model
                setattr(child_model, f'{parent_name.lower()}_id', 
                       Column(Integer, ForeignKey(f'{self._create_table_name(parent_name)}.id')))
                
                # Add relationship to parent model
                relationship_name = self._sanitize_name(child_name.replace(parent_name, '').lower())
                if is_array:
                    setattr(parent_model, relationship_name, 
                           relationship(child_model, cascade="all, delete-orphan"))
                else:
                    setattr(parent_model, relationship_name, 
                           relationship(child_model, uselist=False, cascade="all, delete-orphan"))

    def convert_schema(self, schema: Dict[str, Any], base_model_name: str = "Sensor") -> Dict[str, Any]:
        """Convert a JSON schema to SQLAlchemy models."""
        self._create_model_from_schema(base_model_name, schema)
        self._add_relationships()
        return self.models