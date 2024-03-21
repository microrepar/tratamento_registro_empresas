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
import numpy as np


# ### File Locations

# In[2]:


today = datetime.today()

startswith_file = 'LISTAGEM CADASTRO'

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
    'TELEFONE': str,
    'CEP': str,
    'RAMAL': str,
    'CNPJ/CPF': str,
    # 'FAX': 'string'
}
df_dict = {f.name.split('.')[0]: pd.read_excel(f, dtype=dtypes, sheet_name='FINAL') for f in in_files}


# In[6]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi.columns)


# ### Column Cleanup
# 
# - Remove all leading and trailing spaces
# - Rename the columns for consistency.

# In[7]:


for name, dfi in df_dict.items():
    # https://stackoverflow.com/questions/30763351/removing-space-in-dataframe-python
    dfi.columns = [x.strip().lower() for x in dfi.columns]
    print(dfi.columns)


# In[8]:


for name, dfi in df_dict.items(): pass
{col: '' for i, col in enumerate(dfi.columns)}


# In[9]:


cols_to_rename = {
    'cnpj/cpf': 'cnpj_cpf',
    'rua': 'logradouro',
    'complem': 'complemento',
    'e-mail': 'email',
    'quantidade de empregados': 'qtde_empregados',
    'descrição do cnae': 'descricao_cnae'
}

for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    dfi.rename(columns=cols_to_rename, inplace=True)
    print(dfi.columns)


# ### Clean Up Data Types

# In[10]:


for name, dfi in df_dict.items():...
dfi.dtypes


# In[11]:


for name, dfi in df_dict.items():...
dfi.info()


# In[12]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi.isnull().sum())


# In[13]:


# Adiciona o ddd 11 para os números de telefones fixos e móveis válidos sem o ddd
def adiciona_ddd(v):
    if v is np.nan: return v
    if len(v) == 10 and v.startswith('19') :
        return f'{v:1>11}'
    if len(v) == 9 and v.startswith('9'):
        return f'{v:1>11}'
    elif len(v) == 8 and v[0] not in ['9','1']:
        return f'{v:1>10}'
    return v

for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    dfi['telefone'] = dfi['telefone'].apply(adiciona_ddd)
    dfi['telefone'] = dfi['telefone'].replace({'0': np.nan},)
    print(dfi['telefone'])


# ### Data Manipulation

# In[14]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    dfi['ramal'] = dfi['ramal'].replace({0: np.nan})
    print(dfi[dfi['ramal'].notnull()  & (dfi['ramal'] != '0')]['ramal'])


# In[15]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[['cnae']].head(3))
    # dfi['cnae'] = dfi['cnae'].str.replace(r'[^0-9]', '', regex=True)
    print(dfi[['cnae']].head(3))


# In[16]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    dfi['cnpj_cpf'] = dfi['cnpj_cpf'].str.replace(r'[^0-9]', '', regex=True).str.rjust(14, '0')
    print(dfi[dfi.duplicated(subset=['cnpj_cpf'])])


# In[17]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[dfi.duplicated(subset=['cnpj_cpf'])]['cnpj_cpf'])


# In[18]:


def set_startswith_0(v):
    if len(v) == 7:
        return f'{v:0>8}'
    return v

for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    print(dfi[['cep']])
    dfi['cep'] = dfi['cep'].str.replace(r'[^0-9]', '', regex=True)
    dfi['cep'] = dfi['cep'].apply(set_startswith_0)
    print(dfi[['cep']])


# In[19]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    for col in dfi.columns:
        if dfi[col].dtype == 'object':
            dfi[col] = dfi[col].str.strip()


# In[20]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    for col in dfi.columns:
        print(f'{col:.<30}:', max([len(str(v)) for v in dfi[col]]))
        print(f'{col:.<30}:', min([len(str(v)) for v in dfi[col]]))


# In[21]:


for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)

    dfi['cnpj_cpf'] = dfi['cnpj_cpf'].str.rjust(14, '0')
    print(dfi[dfi['cnpj_cpf'].isin(['00028500097841'])])


# In[22]:


for name, dfi in df_dict.items():...
dfi.head(3)


# In[23]:


dfi.columns


# In[24]:


import re

padrao_cnae = re.compile(r'^\d{4}-\d\/\d{2}$')



cnae_dict = {}

for name, dfi in df_dict.items():

    tabela_cnae_empresas = {
        'cnpj_cpf': [],
        'cnae': [],
    }

    for index, row in dfi.iterrows():
        
        cnpj_cpf = row['cnpj_cpf']
        if cnpj_cpf == '191' and 'BANCO DO' not in row['nome']: continue

        for i, cel in enumerate(row):
            if i < 13 or not type(cel) == str: continue

            if padrao_cnae.match(cel):
                tabela_cnae_empresas['cnpj_cpf'].append(cnpj_cpf)
                tabela_cnae_empresas['cnae'].append(cel)
    
    cnae_dict[name] = tabela_cnae_empresas

cnae_dict


# In[27]:


df_cnae_dict = {}
for name, tabela in cnae_dict.items():
    print('>>>>>>>>>>', name)
    print(len(tabela['cnae']))
    print(len(tabela['cnpj_cpf']))

    df_cnae = pd.DataFrame(tabela)
    df_cnae_dict[name] = df_cnae
    print(df_cnae)


# ### Save output file into processed directory
# 
# Save a file in the processed directory that is cleaned properly. It will be read in and used later for further analysis.
# 
# Other options besides pickle include:
# - feather
# - msgpack
# - parquet

# In[28]:


for name, dfi in df_cnae_dict.items():
    novo_nome = f'{name} - outros cnae.parquet'
    summary_file_cnae = in_dir_destino / novo_nome
    dfi.to_parquet(summary_file_cnae)

for name, dfi in df_dict.items():
    print('>>>>>>>>>>', name)
    summary_file = in_dir_destino / f'{name}.parquet'

    dfi.to_parquet(summary_file)

