# Databricks Notebook Job Setup Instructions

## ✅ Notebook Conversion Complete!

I've successfully converted the Python script to a Databricks notebook (`tabular_data_job.ipynb`) which will provide:

### **🎯 Benefits of Notebook Approach**

1. **✅ Automatic Parameter Handling** - Databricks automatically creates widgets from job parameters
2. **✅ Cell-by-Cell Visibility** - See exactly what succeeds/fails at each step
3. **✅ Better Debugging** - Clear output from each operation
4. **✅ Progressive Execution** - Can run/re-run individual cells
5. **✅ Sample Data Display** - Visual confirmation of generated data

### **📋 Notebook Structure**

**Cell 1:** Import libraries  
**Cell 2:** Get job parameters via widgets (automatic from job)  
**Cell 3:** Parse and validate column configurations  
**Cell 4:** Initialize Spark and DataGenerator  
**Cell 5:** Add columns to DataGenerator  
**Cell 6:** Generate DataFrame and process GenAI columns  
**Cell 7:** Save to volume and complete job  

### **🔧 Setup Required**

#### **Step 1: Upload Notebook to Databricks**
1. Upload `tabular_data_job.ipynb` to your Databricks workspace
2. Note the full workspace path (e.g., `/Users/your.email/tabular_data_job`)

#### **Step 2: Update Databricks Job Configuration**
1. **Edit job ID `635184344270819`**
2. **Change the task type** from "Python script" to "Notebook"
3. **Set notebook path** to the uploaded notebook location
4. **Configure parameters** (these will automatically become widgets):
   ```
   table_name: sample_table
   row_count: 1000
   columns: []
   company_name: Sample Company
   company_sector: Technology
   timestamp: 20250908_000000
   endpoint_name: databricks-gpt-oss-120b
   volume_path: conor_smith/synthetic_data_app/synthetic_data_volume
   ```

#### **Step 3: Test the Setup**
1. **Run tabular generation** from the app
2. **Monitor notebook execution** in Databricks
3. **Check cell outputs** for detailed progress

### **📊 Expected Notebook Output**

When the job runs, you'll see clear output from each cell:

```
Cell 1: 📦 Libraries imported successfully
Cell 2: 🎯 Job Parameters Retrieved:
        - Table name: your_table
        - Row count: 1000
        - Columns JSON length: 150 characters

Cell 3: ✅ Parsed 2 column configurations
        1. test_col (Integer)
           → Range: 1 to 100
        2. bio (GenAI Text)
           → Prompt: Write a bio for...

Cell 4: ⚡ Spark session initialized
        🔧 Spark optimized for 1000 rows → 1 partitions

Cell 5: 📊 Adding column 'test_col' (Integer)
        ✅ Integer: 1 to 100
        📊 Adding column 'bio' (GenAI Text)  
        ✅ GenAI placeholder (will use ai_query)

Cell 6: 🏗️ Building DataFrame with 1000 rows...
        ✅ DataFrame created: 1000 rows × 2 columns
        🎯 Processing GenAI column 'bio'
        ✅ ai_query completed for 'bio'

Cell 7: 💾 Saving data to volume...
        ✅ Successfully saved to volume!
        🎉 Job Completed Successfully!
```

### **🐛 Debugging Benefits**

With the notebook approach:
- **✅ Parameter visibility** - See exact values passed from app
- **✅ Step-by-step progress** - Know exactly where issues occur
- **✅ Data samples** - Visual confirmation of generated data
- **✅ Error isolation** - Identify which cell/operation fails
- **✅ Re-run capability** - Fix and re-run specific cells

### **🔄 Migration Steps**

1. **Upload the notebook** to Databricks workspace
2. **Update job configuration** to use notebook instead of script
3. **Test with sample data** to confirm parameter passing
4. **Run full tabular generation** and monitor cell outputs

The notebook approach should completely solve the parameter passing issues since Databricks automatically handles job parameters as notebook widgets! 🚀
