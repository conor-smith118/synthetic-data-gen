# Databricks Job Parameter Setup Instructions

## Issue: Job Parameters Not Being Passed

The comprehensive debugging should show exactly how parameters are (or aren't) being passed from the app to the job.

## Expected Debug Output Analysis

When you run tabular generation, look for these patterns in the job output:

### 1. **dbutils Methods Available**
```
✅ dbutils is available
   - dbutils.widgets methods: ['get', 'text', 'dropdown', 'combobox', ...]
   - dbutils.jobs methods: [...]
   - dbutils.notebook methods: [...]
```

### 2. **Environment Variables**
Look for any variables containing our parameter names:
```
🔍 DATABRICKS JOB: Filtered environment variables:
   - DATABRICKS_JOB_ID: 635184344270819
   - DATABRICKS_RUN_ID: [some_run_id]
   - [parameters might appear here with different names]
```

### 3. **Parameter Patterns**
```
🔍 DATABRICKS JOB: Looking for Databricks-specific parameter patterns...
   - Found PARAMETER_ pattern variables:
     * PARAMETER_table_name: sample_table
     * PARAMETER_columns: [{"name": "test_col", ...}]
```

## Possible Solutions

### Solution 1: Configure Job with Parameter Widgets

The job might need to be configured with parameter widgets. In the Databricks job configuration:

1. **Edit the job (ID: 635184344270819)**
2. **Add parameters section:**
   ```
   Parameters:
   - table_name: sample_table
   - row_count: 1000  
   - columns: []
   - company_name: Sample Company
   - company_sector: Technology
   - timestamp: 20250908_000000
   - endpoint_name: databricks-gpt-oss-120b
   - volume_path: conor_smith.synthetic_data_app.synthetic_data_volume
   ```

### Solution 2: Use Task Parameters Instead

Change the app to use `notebook_params` instead of `job_parameters`:

```python
# In SyntheticDataGenerator.py, change:
run = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters)

# To:
run = w.jobs.run_now(
    job_id=job_id, 
    notebook_params=job_parameters  # Use notebook_params instead
)
```

### Solution 3: Use Job Tasks with Parameters

The job might need to be configured as a task-based job where each task can accept parameters.

### Solution 4: Alternative Parameter Passing

If the debugging shows parameters in environment variables with different names, update the job script accordingly.

## Quick Test

After looking at the debug output, try this simple test to verify parameter passing:

1. **Manually set a parameter** in the job configuration
2. **Run the job** and see if it appears in the debug output
3. **Update the job script** to look for the correct parameter access method

## Next Steps

1. **Run the job again** with enhanced debugging
2. **Analyze the debug output** to see what's actually available
3. **Configure the job** based on what the debugging reveals
4. **Update parameter access** in the job script accordingly

The comprehensive debugging should reveal exactly how Databricks passes parameters to jobs, allowing us to fix the parameter access method.
