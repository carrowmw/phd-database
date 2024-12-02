from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Database connection URL
DATABASE_URL = "postgresql://test@localhost:5432/testdb"


def test_database_connection():
    try:
        # Create an engine
        engine = create_engine(DATABASE_URL)

        # Try to connect and execute a simple query
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Connection successful!")
            print("Query result:", result.scalar())

    except SQLAlchemyError as e:
        print("An error occurred while connecting to the database:")
        print(str(e))


if __name__ == "__main__":
    test_database_connection()
