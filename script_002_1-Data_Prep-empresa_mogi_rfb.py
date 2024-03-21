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

# In[1]:


import pandas as pd
from pathlib import Path
from datetime import datetime


# ### File Locations

# In[2]:


today = datetime.today()

startswith_file = 'CNPJ DE MOGI'

in_dir_origem = Path.cwd() / "data" / "raw"
in_dir_destino = Path.cwd() / "data" / "processed"


# In[3]:


in_files_origem = [f for f in in_dir_origem.glob('*.xlsx') 
                    if f.name.startswith(startswith_file)]

in_files_destino = [f for f in in_dir_destino.glob('*.parquet') 
                    if f.name.startswith(startswith_file)]


# In[4]:


# Lista somente os arquivos que não contém o seu respectivo .parquet na pasta processed
file_name_destino = [f.name.split('.')[0] for f in in_files_destino]
in_files = [f for f in in_files_origem if f.name.split('.')[0] not in file_name_destino]
in_files


# In[5]:


dtypes = {
    'FAX': str,
    'DDD FAX': str,
    'DDD 1': str,
    'TELEFONE 1': str,
    'DDD 2': str,
    'TELEFONE 2': str,
    'DATA SITUAÇÃO': str,
    'INICIO ATIV': str,
    'CEP': str,
    'CNPJ': str,
    'CNAE PRINCIPAL': str,
    'CNAE SECUNDARIA': str,
}

df_dict = {f.name.split('.')[0]: pd.read_excel(f, dtype=dtypes) for f in in_files}
for name, dfi in df_dict.items():...
dfi.columns


# ### Column Cleanup
# 
# - Remove all leading and trailing spaces
# - Rename the columns for consistency.

# In[6]:


for name, dfi in df_dict.items():
    # https://stackoverflow.com/questions/30763351/removing-space-in-dataframe-python
    dfi.columns = [x.strip().lower() for x in dfi.columns]
    print(name)


# In[7]:


for name, dfi in df_dict.items():...
{col: '' for col in dfi.columns}


# In[8]:


cols_to_rename = {
    'identif m/f': 'id_matriz_filial',
    'nome empresarial': 'nome_empresarial',
    'nome fantasia': 'nome_fantasia',
    'natureza juridica': 'natureza_juridica',
    'capital social': 'capital_social',
    'situação cadastral': 'situcacao_cadastral',
    'data situação': 'data_situacao',
    'motivo da situação': 'motivo_situacao',
    'inicio ativ': 'inicio_atividade',
    'cnae principal': 'cnae',
    'cnae secundaria': 'cnae_secundaria',
    'tp lograd': 'tp_logradouro',
    'compl': 'complemento',
    'ddd 1': 'ddd1',
    'telefone 1': 'telefone1',
    'ddd 2': 'ddd2',
    'telefone 2': 'telefone2',
    'ddd fax': 'ddd_fax',
}
for name, dfi in df_dict.items():
    dfi.rename(columns=cols_to_rename, inplace=True)


# In[9]:


for name, dfi in df_dict.items():...    
dfi.head(3)


# ### Clean Up Data Types

# In[10]:


for name, dfi in df_dict.items():...
dfi.dtypes


# In[11]:


for name, dfi in df_dict.items():...
dfi.info()


# In[12]:


# Retira espaços em branco nas estremidades dos nomes das colunas
for name, dfi in df_dict.items():
    for col in dfi.columns:
        if dfi[col].dtype == 'object':
            dfi[col] = dfi[col].str.strip()


# In[13]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi.isnull().sum())


# ### Data Manipulation

# In[14]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[['cnpj']].head())
    dfi['cnpj'] = dfi['cnpj'].str.replace(r'[^0-9]', '', regex=True).str.rjust(14, '0')
    print(dfi[['cnpj']].head())
    


# In[15]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[['capital_social']].head())
    dfi['capital_social'] = (dfi['capital_social'].str.replace('.', '', regex=False)
                                                .str.replace(',', '.', regex=False))
    print(dfi[['capital_social']].head())


# In[16]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi['inicio_atividade'])
    print(dfi['data_situacao'])
    dfi['inicio_atividade'] = dfi['inicio_atividade'].str[:4]  +'-'+ dfi['inicio_atividade'].str[4:6] +'-'+dfi['inicio_atividade'].str[6:] 
    dfi['data_situacao'] = dfi['data_situacao'].str[:4]  +'-'+ dfi['data_situacao'].str[4:6] +'-'+dfi['data_situacao'].str[6:] 
    print(dfi['inicio_atividade'])
    print(dfi['data_situacao'])


# In[17]:


import re
import numpy as np

def parse_date(date_string):
    if date_string is np.nan: return np.nan
    # Check for the presence of a 4-digit year
    four_digit_year_pattern = r'^\d{2}-\d{2}-\d{4}$'
    # Check for the presence of a 2-digit year
    two_digit_year_pattern = r'^\d{2}-\d{2}-\d{2}$'
    
    four_digit_year_pattern2 = r'^\d{4}-\d{2}-\d{2}$'

    # Check for the presence of a 4-digit year br
    br_four_digit_year_pattern = r'^\d{2}/\d{2}/\d{4}$'
    # Check for the presence of a 2-digit year br
    br_two_digit_year_pattern = r'^\d{2}/\d{2}/\d{2}$'

    if re.match(four_digit_year_pattern2, date_string):
        return datetime.strptime(date_string, '%Y-%m-%d')
    if re.match(four_digit_year_pattern, date_string):
        return datetime.strptime(date_string, "%d-%m-%Y")
    elif re.match(two_digit_year_pattern, date_string):
        return datetime.strptime(date_string, "%d-%m-%y")
    elif re.match(br_four_digit_year_pattern, date_string):
        try:
            data = datetime.strptime(date_string, "%d/%m/%Y")
            return data 
        except:
            print('>>>>>>>>>>>>', date_string)
            mes, dia, ano = date_string.split('/')
            return datetime.strptime(f'{dia}/{mes}/{ano[-2:]}', "%d/%m/%y")

    elif re.match(br_two_digit_year_pattern, date_string):
        return datetime.strptime(date_string, "%d/%m/%y")
    return None


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    dfi['data_situacao'] = dfi['data_situacao'].apply(parse_date)
    print(dfi[['data_situacao']].head(5))


    dfi['inicio_atividade'] = pd.to_datetime(dfi['inicio_atividade'])
    print(dfi[['inicio_atividade']].head(5))


# In[18]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi['numero'].astype(str).str.replace(r'[0-9]', '', regex=True).unique())


# In[19]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    for col in dfi.columns:
        print(f'{col:.<30}:', max([len(str(v)) for v in dfi[col]]))


# In[20]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[dfi['nome_empresarial'].str.contains('BANCO DO BRASIL')])


# In[21]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[['inicio_atividade', 'cnae', 'cnae_secundaria', 'tp_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', ]].head(3))


# In[22]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    dfi['cep'] = dfi['cep'].str.rjust(8, '0')
    print(dfi[['cep']])


# In[23]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[['cnae']])
    dfi['cnae'] = dfi['cnae'].str.replace(r'[^0-9]', '', regex=True)    #.str.rjust(7, '0')
    dfi['cnae'] = dfi['cnae'].str[:-3] +'-'+ dfi['cnae'].str[-3:-2] +'/'+ dfi['cnae'].str[-2:]
    print(dfi[['cnae']])


# In[24]:


cnae = '6920601'
cnae[:-3] +'-'+ cnae[-3:-2] +'/'+ cnae[-2:]


# ### Save output file into processed directory
# 
# Save a file in the processed directory that is cleaned properly. It will be read in and used later for further analysis.
# 
# Other options besides pickle include:
# - feather
# - msgpack
# - parquet

# In[25]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    summary_file = in_dir_destino / f'{name}.parquet'

    dfi.to_parquet(summary_file)

