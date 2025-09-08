#!/usr/bin/env python3
"""
Databricks Job for Tabular Data Generation

This job runs on a Databricks cluster with access to:
- Spark/PySpark
- dbldatagen library  
- ai_query() function for LLM integration
- Databricks volumes for storage

Parameters passed from the app:
- table_name: Name for the generated table
- row_count: Number of rows to generate
- columns: JSON string containing column configurations
- company_name: Company context
- company_sector: Business sector context  
- timestamp: Generation timestamp
- endpoint_name: LLM endpoint to use for GenAI Text columns
- volume_path: Databricks volume path for storage
"""

import json
import os
import sys
from datetime import datetime

def main():
    """Main job execution function."""
    
    # Get parameters from Databricks job
    try:
        # For jobs triggered via jobs.run_now(), parameters are accessed via sys.argv or task context
        # Try to get from task context first, then fall back to environment variables
        
        # Debug: Print command line arguments
        print(f"🔍 DATABRICKS JOB: Command line arguments: {sys.argv}")
        
        def get_job_parameter(param_name, default_value):
            """Get parameter from various Databricks job parameter sources."""
            try:
                # Method 1: Try dbutils.widgets (for job parameters) - need to create widget first
                if 'dbutils' in globals():
                    try:
                        # First try to create the widget, then get its value
                        dbutils.widgets.text(param_name, default_value, param_name)
                        value = dbutils.widgets.get(param_name)
                        if value != default_value:
                            print(f"✅ Found {param_name} via dbutils.widgets: {value}")
                            return value
                    except Exception as e:
                        print(f"⚠️  dbutils.widgets failed for {param_name}: {e}")
                
                # Method 2: Try environment variables with job parameter prefixes
                # Databricks often prefixes job parameters
                for prefix in ['', 'DATABRICKS_', 'JOB_', 'TASK_']:
                    env_key = f"{prefix}{param_name}"
                    env_value = os.environ.get(env_key)
                    if env_value is not None:
                        print(f"✅ Found {param_name} via environment variable {env_key}: {env_value}")
                        return env_value
                    
                    # Try uppercase version
                    env_key_upper = env_key.upper()
                    env_value = os.environ.get(env_key_upper)
                    if env_value is not None:
                        print(f"✅ Found {param_name} via uppercase environment variable {env_key_upper}: {env_value}")
                        return env_value
                
                # Method 3: Try accessing all environment variables that might contain our parameters
                # Look for any env var that contains our parameter name
                for env_key, env_value in os.environ.items():
                    if param_name.lower() in env_key.lower():
                        print(f"✅ Found {param_name} via matching environment variable {env_key}: {env_value}")
                        return env_value
                
                # Method 4: Try command line arguments (for script-based jobs)
                for i, arg in enumerate(sys.argv):
                    if f"--{param_name}" in arg:
                        if "=" in arg:
                            value = arg.split("=", 1)[1]
                            print(f"✅ Found {param_name} via command line argument: {value}")
                            return value
                        elif i + 1 < len(sys.argv):
                            value = sys.argv[i + 1]
                            print(f"✅ Found {param_name} via command line argument: {value}")
                            return value
                
                # Return default if nothing found
                print(f"⚠️  Using default value for {param_name}: {default_value}")
                return default_value
            except Exception as e:
                print(f"❌ Error getting parameter {param_name}: {e}")
                return default_value
        
        # Get parameters using the robust parameter access method
        table_name = get_job_parameter("table_name", "sample_table")
        row_count = int(get_job_parameter("row_count", "1000"))
        columns_json = get_job_parameter("columns", "[]")
        company_name = get_job_parameter("company_name", "Sample Company")
        company_sector = get_job_parameter("company_sector", "Technology")
        timestamp = get_job_parameter("timestamp", datetime.now().strftime("%Y%m%d_%H%M%S"))
        endpoint_name = get_job_parameter("endpoint_name", "databricks-gpt-oss-120b")
        volume_path = get_job_parameter("volume_path", "conor_smith.synthetic_data_app.synthetic_data_volume")
        
        print("🚀 DATABRICKS JOB: Starting tabular data generation")
        
        # Debug: Print ALL available environment variables to find job parameters
        print("🔍 DATABRICKS JOB: ALL environment variables:")
        all_env_vars = dict(os.environ)
        for key in sorted(all_env_vars.keys()):
            value = all_env_vars[key]
            # Truncate very long values but show first part
            if len(str(value)) > 100:
                print(f"   - {key}: {value[:100]}...")
            else:
                print(f"   - {key}: {value}")
        
        print("🔍 DATABRICKS JOB: Filtered environment variables (containing our parameter names):")
        relevant_env_vars = {k: v for k, v in os.environ.items() if any(param in k.lower() for param in ['table', 'row', 'column', 'company', 'timestamp', 'endpoint', 'volume', 'databricks', 'job', 'task'])}
        for key, value in relevant_env_vars.items():
            print(f"   - {key}: {value[:100]}..." if len(str(value)) > 100 else f"   - {key}: {value}")
        
        print("📋 DATABRICKS JOB: Resolved parameters:")
        print(f"   - Table name: {table_name}")
        print(f"   - Row count: {row_count}")
        print(f"   - Company: {company_name} ({company_sector})")
        print(f"   - Timestamp: {timestamp}")
        print(f"   - Endpoint: {endpoint_name}")
        print(f"   - Volume: {volume_path}")
        
        # Parse column configurations
        columns = json.loads(columns_json)
        print(f"   - Columns: {len(columns)} configured")
        
        # If no columns are configured, add some sample columns for testing
        if len(columns) == 0:
            print("⚠️  DATABRICKS JOB: No columns configured, adding sample columns for testing")
            columns = [
                {"name": "id", "data_type": "Integer", "min_value": 1, "max_value": 1000},
                {"name": "first_name", "data_type": "First Name"},
                {"name": "last_name", "data_type": "Last Name"},
                {"name": "sample_text", "data_type": "GenAI Text", "prompt": "Write a short professional bio for <first_name> <last_name>", "max_tokens": 100}
            ]
            print(f"   - Added {len(columns)} sample columns for testing")
        
    except Exception as e:
        print(f"❌ Error parsing job parameters: {e}")
        raise e
    
    try:
        # Import required libraries (available on Databricks cluster)
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import expr, col, lit
        from dbldatagen import DataGenerator, fakerText
        
        print("📦 DATABRICKS JOB: Libraries imported successfully")
        
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("TabularDataGeneration").getOrCreate()
        
        print("⚡ DATABRICKS JOB: Spark session obtained")
        
        # Set partition parameters for optimal performance
        partitions_requested = min(8, max(1, row_count // 1000))  # 1 partition per 1000 rows, max 8
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions_requested))
        
        print(f"🔧 DATABRICKS JOB: Set partitions to {partitions_requested}")
        
        # Create DataGenerator
        data_gen = DataGenerator(spark, rows=row_count, partitions=partitions_requested)
        
        print("🏗️  DATABRICKS JOB: DataGenerator created")
        
        # Add columns based on configuration
        genai_columns = []
        
        for col_config in columns:
            col_name = col_config.get('name', 'unnamed_column')
            col_type = col_config.get('data_type', 'Integer')
            
            print(f"   - Adding column '{col_name}' of type '{col_type}'")
            
            if col_type == 'Integer':
                min_val = col_config.get('min_value', 1)
                max_val = col_config.get('max_value', 100)
                data_gen = data_gen.withColumn(col_name, "integer", minValue=min_val, maxValue=max_val)
                
            elif col_type == 'First Name':
                # Try faker first, fall back to predefined lists if faker not available
                try:
                    data_gen = data_gen.withColumn(col_name, text=fakerText("first_name"))
                except Exception as e:
                    print(f"⚠️  Faker not available for first names ({e}), using predefined list")
                    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
                                  "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
                                  "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Helen", "Daniel", "Nancy",
                                  "Matthew", "Betty", "Anthony", "Dorothy", "Mark", "Lisa", "Donald", "Sandra"]
                    data_gen = data_gen.withColumn(col_name, values=first_names)
                
            elif col_type == 'Last Name':
                # Try faker first, fall back to predefined lists if faker not available  
                try:
                    data_gen = data_gen.withColumn(col_name, text=fakerText("last_name"))
                except Exception as e:
                    print(f"⚠️  Faker not available for last names ({e}), using predefined list")
                    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                                 "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
                                 "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris",
                                 "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen"]
                    data_gen = data_gen.withColumn(col_name, values=last_names)
                
            elif col_type == 'GenAI Text':
                # For GenAI Text, add placeholder column first, then process with ai_query
                data_gen = data_gen.withColumn(col_name, "string", values=[""])
                genai_columns.append(col_config)
                
            elif col_type == 'Custom Values':
                # For Custom Values, use values and optional weights
                custom_values = col_config.get('custom_values', [''])
                use_weights = col_config.get('use_weights', False)
                custom_weights = col_config.get('custom_weights', [1])
                
                # Filter out empty values
                filtered_values = [v for v in custom_values if v.strip()]
                if not filtered_values:
                    filtered_values = ['DefaultValue']  # Fallback if all values are empty
                
                if use_weights and len(custom_weights) >= len(filtered_values):
                    # Use weights if enabled and we have enough weights
                    filtered_weights = custom_weights[:len(filtered_values)]
                    data_gen = data_gen.withColumn(col_name, values=filtered_values, weights=filtered_weights)
                else:
                    # Use values without weights
                    data_gen = data_gen.withColumn(col_name, values=filtered_values)
        
        print("📊 DATABRICKS JOB: Building initial DataFrame...")
        
        # Generate the initial DataFrame
        df = data_gen.build()
        
        print(f"✅ DATABRICKS JOB: DataFrame created with {df.count()} rows")
        
        # Process GenAI Text columns using ai_query
        if genai_columns:
            print(f"🤖 DATABRICKS JOB: Processing {len(genai_columns)} GenAI Text columns with ai_query")
            
            for col_config in genai_columns:
                col_name = col_config.get('name', 'unnamed_column')
                prompt_template = col_config.get('prompt', '')
                
                if prompt_template:
                    print(f"   - Column '{col_name}': Generating AI text for all {row_count} rows")
                    
                    # Add table formatting note to the prompt
                    enhanced_prompt = f"{prompt_template} Note: This will be text data in a table so omit all special formatting."
                    
                    # Get max_tokens from column config (default 500 if not specified)
                    max_tokens = col_config.get('max_tokens', 500)
                    
                    # Create dynamic prompt with column substitution
                    prompt_expression = substitute_column_references_spark(enhanced_prompt, columns)
                    
                    print(f"   - Using prompt expression: {prompt_expression}")
                    
                    # Use ai_query to generate text based on the dynamic prompt
                    # Note: ai_query signature is ai_query(endpoint => string, request => string)
                    df = df.withColumn(
                        col_name,
                        expr(
                            f"ai_query(endpoint => '{endpoint_name}', request => {prompt_expression})"
                        )
                    )
                    
                    print(f"   ✅ Column '{col_name}': AI text generation complete")
        
        print("💾 DATABRICKS JOB: Saving to volume...")
        
        # Save to Databricks volume
        filename = f"{table_name}_{timestamp}.csv"
        volume_file_path = f"/Volumes/{volume_path}/{filename}"
        
        # Write DataFrame to volume as CSV
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(volume_file_path)
        
        print(f"✅ DATABRICKS JOB: Successfully saved to {volume_file_path}")
        
        # Get some statistics for logging
        total_rows = df.count()
        total_cols = len(df.columns)
        
        print("📈 DATABRICKS JOB: Generation Statistics")
        print(f"   - Total rows: {total_rows}")
        print(f"   - Total columns: {total_cols}")
        print(f"   - GenAI columns: {len(genai_columns)}")
        print(f"   - File: {filename}")
        
        print("🎉 DATABRICKS JOB: Tabular data generation completed successfully!")
        
        return {
            "status": "success",
            "filename": filename,
            "volume_path": volume_file_path,
            "row_count": total_rows,
            "column_count": total_cols
        }
        
    except Exception as e:
        print(f"❌ DATABRICKS JOB: Error during data generation: {e}")
        import traceback
        traceback.print_exc()
        raise e


def substitute_column_references_spark(prompt_template, columns):
    """
    Substitute column references in prompt template for Spark SQL.
    Converts <Column Name> patterns to concat() expressions for dynamic prompts.
    """
    import re
    
    # Find all column references in the format <Column Name>
    column_refs = re.findall(r'<([^<>]+)>', prompt_template)
    
    if not column_refs:
        # No column references, return as literal string
        return f"'{prompt_template}'"
    
    # Build a concat expression for dynamic prompt
    parts = []
    current_pos = 0
    
    for match in re.finditer(r'<([^<>]+)>', prompt_template):
        col_name = match.group(1)
        start_pos = match.start()
        end_pos = match.end()
        
        # Add text before the column reference
        if start_pos > current_pos:
            literal_text = prompt_template[current_pos:start_pos]
            if literal_text:
                parts.append(f"'{literal_text}'")
        
        # Add the column reference
        # Check if column exists in the configuration
        valid_columns = [col.get('name', 'unnamed_column') for col in columns]
        if col_name in valid_columns:
            parts.append(f"coalesce(cast({col_name} as string), 'NULL')")
        else:
            # Column not found, use literal text
            parts.append(f"'<{col_name}>'")
        
        current_pos = end_pos
    
    # Add remaining text after the last column reference
    if current_pos < len(prompt_template):
        literal_text = prompt_template[current_pos:]
        if literal_text:
            parts.append(f"'{literal_text}'")
    
    # Join all parts with concat
    if len(parts) == 1:
        return parts[0]
    else:
        return f"concat({', '.join(parts)})"


if __name__ == "__main__":
    main()
