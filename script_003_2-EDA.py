#!/usr/bin/env python
# coding: utf-8

# ## Tratamento de Dados de Registro de Empresas
# 
# Script para tratamento de dados e compactacao dos registros de empresas de Mogi das Cruzes por Período.
# 
# This notebook contains basic statistical analysis and visualization of the data.
# 
# ### Data Sources
# - summary : Processed file from notebook 1-Data_Prep
# 
# ### Changes
# - 01-23-2024 : Started project

# In[ ]:


import pandas as pd
from pathlib import Path
from datetime import datetime
import seaborn as sns


# In[ ]:


# get_ipython().run_line_magic('matplotlib', 'inline')


# ### File Locations

# In[ ]:


today = datetime.today()
in_file = Path.cwd() / "data" / "processed" / f"summary_{today:%b-%d-%Y}.pkl"
report_dir = Path.cwd() / "reports"
report_file = report_dir / "Excel_Analysis_{today:%b-%d-%Y}.xlsx"


# In[ ]:


df = pd.read_pickle(in_file)


# ### Perform Data Analysis

# In[ ]:





# ### Save Excel file into reports directory
# 
# Save an Excel file with intermediate results into the report directory

# In[ ]:


writer = pd.ExcelWriter(report_file, engine='xlsxwriter')


# In[ ]:


df.to_excel(writer, sheet_name='Report')


# In[ ]:


writer.save()

