# Accessing Data Through Dremio

In this document we are going to analyse how to connect Python and Dremio (Coder).

## Port Fowarding

First install the Coder CLI:

```bash
curl -L https://coder.com/install.sh | sh
```

Configure the credentials and start port fowarding:

```bash
coder login https://coder.deployment
coder config-ssh
coder port-forward mydremio --tcp 32010:32010
```

Install the Dremio Arrow Flight library:

```
pip install dremio-arrow
```

Test the connection:

```python
from dremioarrow import DremioArrowClient

client = DremioArrowClient(
    host='127.0.0.1',
    port='32010',
    username='admin',
    password='admin'
) #

sql = 'SELECT * FROM myschema."mytable" LIMIT 10' 

data = client.query(sql) 

# preview result data from dremio flight server
print(data) # 
```

## Jupyter in Coder Workspace

Install the Dremio Arrow Flight library:

```
pip install dremio-arrow
```

Test the connection:

```python
from dremioarrow import DremioArrowClient

client = DremioArrowClient(
    host='dremio-coderuser-workspacename',
    port='32010',
    username='admin',
    password='admin'
) #

sql = 'SELECT * FROM myschema."mytable" LIMIT 10' 

data = client.query(sql) 

# preview result data from dremio flight server
print(data) # 
```

## Performing BINPACK

Set a higher limits for fields 
```python
sql = 'alter system set limits.single_field_size_bytes = 99000;'

data = client.query(sql) 
print(data) # 
```

Perform BINPACK

```python
sql = 'OPTIMIZE TABLE nessie."youtube-video-comment" REWRITE DATA USING BIN_PACK'
data = client.query(sql) 
print(data) # 
```

```sql
ALTER TABLE nessie."youtube-video-comments" SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled'='true');
```