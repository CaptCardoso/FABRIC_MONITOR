import re
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from fabric_monitor.shared_code.utils.helpers import setup_logging

# Create logger
logger = logging.getLogger('FabricMonitorUDFs')

class FabricMonitorUDFs:
    """
    A class that contains user-defined functions (UDFs) for text parsing.
    """

    extract_mquery_parameter_details_schema = StructType([
        StructField("Value", StringType(), True),
        StructField("IsParameterQuery", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("IsParameterQueryRequired", StringType(), True),
    ])

    extract_database_source_object_schema = StructType([
        StructField("Object", StringType(), True),
        StructField("Kind", StringType(), True),
        StructField("Schema", StringType(), True),
        StructField("Database", StringType(), True),
    ])

    @staticmethod
    @udf(returnType=extract_mquery_parameter_details_schema)
    def extract_mquery_parameter_details(expression):
        """
        Extracts parameter details from an M-query string.

        Args:
            expression (str): The M-query string.

        Returns:
            tuple: A tuple containing the extracted parameter details for the:
                - Parameter Value
                - IsParameterQuery parameter
                - Type parameter
                - IsParameterQueryRequired parameter
        """
        try:
            if not expression:
                return {
                    "Value": None,
                    "IsParameterQuery": None,
                    "Type": None,
                    "IsParameterQueryRequired": None
            }

            pattern = r'\"?([^\"]+)\"?\s+meta\s+\[IsParameterQuery=(\w+),\s+Type=\"(\w+)\",\s+IsParameterQueryRequired=(\w+)\]'
            matches = re.search(pattern, expression, re.IGNORECASE)
            if matches:
                extracted_value = matches.groups()
            else:
                extracted_value = None

            return extracted_value

        except Exception as e:
            logger.error(f'Error extracting M-query parameter details: {e}')
            raise

    @staticmethod
    def format_parameters(mquery_object_name):
        """
        Formats the M-query object name by removing quotes and adding a '#' if no quotes are found.
        M-query parameters are typically prefixed by a # but they don't have to be. If they are not
        prefixed by a #, they also will not be enclosed by quotes. This function ensures that all
        parameters are prefixed by a # they can be identified compared to non-parameter values.

        Args:
            mquery_object_name (str): The M-query object name.

        Returns:
            str: The formatted M-query object name.
        """
        try:
            updated_mquery_object_name = mquery_object_name
            if updated_mquery_object_name.find('"') == -1:
                updated_mquery_object_name = '#' + updated_mquery_object_name
            updated_mquery_object_name = updated_mquery_object_name.replace('"', '')

            return updated_mquery_object_name

        except Exception as e:
            logger.error(f'Error formatting M-query object name: {e}')
            raise


    @staticmethod
    @udf(returnType=extract_database_source_object_schema)
    def extract_database_source_object(source_expression):
        """
        Extracts database source object values from an M-query string.

        Args:
            source_expression (str): The M-query string.

        Returns:
            dict: A dictionary containing the extracted database source object values.
        """
        try:
            if not source_expression:
                return {
                    "Object": None,
                    "Kind": None,
                    "Schema": None,
                    "Database": None
            }

            pattern1 = r'(Item|Schema|Catalog)=([^,\]]*)'
            pattern2 = r'\[Name\s*=\s*(.*?)\s*,\s*Kind\s*=\s*"(.*?)"\]'
            pattern3 = r'Sql\.Database\((".*?"),\s*(".*?")\)'

            matches_pattern1 = re.findall(pattern1, source_expression)
            matches_pattern2 = re.findall(pattern2, source_expression)
            matches_pattern3 = re.findall(pattern3, source_expression)

            mapping = {
                'Catalog': 'Database',
                'Schema': 'Schema',
                'Item': 'Object',
                'Database': 'Database',
                'Schema': 'Schema',
                'Table': 'Object',
                'View': 'Object'
            }

            output = {}
            if matches_pattern1:
                for _match in matches_pattern1:
                    output[mapping[_match[0]]] = _match[1]
            if matches_pattern2:
                for _match in matches_pattern2:
                    output[mapping[_match[1]]] = _match[0]
            if matches_pattern3:
                for _match in matches_pattern3:
                    output['Database'] = _match[1]

            for key, value in output.items():
                output[key] = FabricMonitorUDFs.format_parameters(value)

            return output

        except Exception as e:
            logger.error(f'Error extracting database source object details: {e}')
            raise

    @staticmethod
    @udf(returnType=StringType())
    def extract_mquery_connection_type(source_expression):
        """
        Extracts the connection type from an M-query string.

        Args:
            source_expression (str): The M-query string.

        Returns:
            str: The extracted connection type.
        """
        try:
            if not source_expression:
                return None

            cleaned_expression = source_expression.replace("#(tab)", " ").replace("#(lf)", " ").replace("  ", " ")
            pattern = r'(?:Source =)\s+([a-zA-Z0-9.#_]+)'
            _match = re.search(pattern, cleaned_expression, re.IGNORECASE)

            return _match.group(1).strip() if _match else None

        except Exception as e:
            logger.error(f'Error extracting connection type details: {e}')
            raise