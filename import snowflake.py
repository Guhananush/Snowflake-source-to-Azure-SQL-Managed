import snowflake.connector
import pyodbc
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

 
@contextmanager 
def get_sql_conn():   
    sql_conn_str = (
        'DRIVER=xxxxx;'
        'SERVER=xxxxx;'
        'DATABASE=xxxxx;'
        'UID=xxxxx;'
        'PWD=xxxxx;'
    )
    sql_conn = pyodbc.connect(sql_conn_str)
    try:
        print("Connected to AzureSQLMI")
        yield sql_conn
    finally:
        sql_conn.close()
        
def col_signature(cols):
    sigs = set()
    for name, dtype, length, prec, scale in cols:
        sigs.add(f"{name.lower()}:{dtype.lower()}:{length}:{prec}:{scale}")
    return sigs

def log_table_operation(sql_cursor, action, config_id, table_schema, table_name, load_type,
                        watermark=None, IsSchemaChange=None, source_rows=None,
                        updated_rows=None, inserted_rows=None,
                        total_merged=None, target_rows=None, delete_rows=None,
                        log_id=None, status=None, error_message=None):
    """
    Handles insert and update into Audit.Table_Logs.

    Parameters:
        action: "START" or "END"
        status: "Success" / "Failure" (only for END)
        error_message: error text if failure, else "No Error"
    """

    if action == "START":
        sql_cursor.execute("""
            INSERT INTO Audit.Table_Logs
                (config_id, Table_schema, Table_name, LoadType, WaterMarkDate, IsSchemaChange, SourceRowCount,
                 LoadprocessStartTime, LoadStatus, Created_at)
            OUTPUT INSERTED.Logid
            VALUES (?, ?, ?, ?, ?, ?, ?,  SYSDATETIME(), 'InProgress', SYSDATETIME())
        """, (config_id, table_schema, table_name, load_type, watermark, IsSchemaChange, source_rows))
        log_id = sql_cursor.fetchone()[0]
        return log_id

    elif action == "END":
        if status == "Success":
            error_message = "No Error"
        elif not error_message:
            error_message = "Unknown Error"
            
        target_rows_calc = target_rows
        sql_cursor.execute("""
            UPDATE Audit.Table_Logs WITH (ROWLOCK, UPDLOCK)
            SET UpdatedRows = ?,
                InsertedRows = ?,
                TotalMergedRows = ?,
                TargetRowCount = ?,
                DeletedRows = ?,
                LoadprocessEndTime = SYSDATETIME(),
                LoadStatus = ?,
                ErrorMessage = ?,
                RowMatchFlag = CASE WHEN SourceRowCount = ? THEN 'Y' ELSE 'N' END,
                Loadrunduration = LTRIM(CASE 
                WHEN DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) = 0 
                THEN 'Less than 1s'
                Else
                    CASE WHEN DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) / 86400 > 0 
                         THEN CAST(DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) / 86400 AS VARCHAR) + 'd ' 
                         ELSE '' END
                  + CASE WHEN (DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) % 86400) / 3600 > 0 
                         THEN CAST((DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) % 86400) / 3600 AS VARCHAR) + 'h ' 
                         ELSE '' END
                  + CASE WHEN (DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) % 3600) / 60 > 0 
                         THEN CAST(((DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) % 3600) / 60) AS VARCHAR) + 'm ' 
                         ELSE '' END
                  + CASE WHEN DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) % 60 > 0 
                         THEN CAST(DATEDIFF(SECOND, loadprocessstarttime, SYSDATETIME()) % 60 AS VARCHAR) + 's' 
                         ELSE '' END
                END)
            WHERE Logid = ?
        """, (updated_rows, inserted_rows, total_merged, target_rows, delete_rows,
              status, error_message, target_rows_calc, log_id))
        return log_id


def map_snowflake_to_azuresql(snowflake_type, precision=None, scale=None):
    snowflake_type = snowflake_type.upper().strip()

    # If precision & scale not provided, try to parse from type string
    if precision is None or scale is None:
        match = re.match(r'NUMBER\((\d+),\s*(\d+)\)', snowflake_type)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
        else:
            match = re.match(r'NUMBER\((\d+)\)', snowflake_type)
            if match:
                precision = int(match.group(1))
                scale = 0

    # Now handle NUMBER cases
    if snowflake_type.startswith('NUMBER'):
        if precision is not None and scale is not None:
            if precision > 38:
                precision = 38
            if scale > precision:
                scale = precision
            return f'DECIMAL({precision},{scale})'
        elif precision is not None:
            if precision > 38:
                precision = 38
            return f'DECIMAL({precision},0)'
        else:
            # Bare NUMBER → integer semantics
            return 'DECIMAL(38,0)'

    # Rest of your mappings unchanged
    if snowflake_type in ['INTEGER', 'INT', 'BIGINT']:
        return 'BIGINT'
    if snowflake_type in ['SMALLINT', 'TINYINT']:
        return snowflake_type
    if snowflake_type in ['FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE']:
        return 'FLOAT'
    if snowflake_type == 'REAL':
        return 'REAL'
    if snowflake_type == 'BOOLEAN':
        return 'BIT'
    if re.match(r'VARCHAR\(\s*16777216\s*\)', snowflake_type):
        return 'NVARCHAR(MAX)'
    if re.match(r'VARCHAR\(\d+\)', snowflake_type):
        size = int(re.findall(r'\d+', snowflake_type)[0])
        return f'NVARCHAR({size})' if size <= 4000 else 'NVARCHAR(MAX)'
    if snowflake_type in ['TEXT', 'STRING']:
        return 'NVARCHAR(MAX)'
    if re.match(r'CHAR\(\d+\)', snowflake_type):
        size = int(re.findall(r'\d+', snowflake_type)[0])
        return f'NVARCHAR({size})' if size <= 4000 else 'NVARCHAR(MAX)'
    if snowflake_type in ['CHAR', 'CHARACTER']:
        return 'NVARCHAR(255)'
    if snowflake_type == 'DATE':
        return 'DATE'
    if snowflake_type == 'TIME':
        return 'TIME(7)'
    if snowflake_type == 'TIMESTAMP_NTZ':
        return 'DATETIME2'
    if snowflake_type == 'TIMESTAMP':
        return 'DATETIME'
    if snowflake_type in ['TIMESTAMP_LTZ', 'TIMESTAMP_TZ']:
        return 'DATETIMEOFFSET'
    if snowflake_type in ['BINARY', 'VARBINARY']:
        return 'VARBINARY(MAX)'
    if snowflake_type in ['ARRAY', 'OBJECT', 'VARIANT']:
        return 'NVARCHAR(MAX)'
    if 'UUID_STRING' in snowflake_type:
        return 'UNIQUEIDENTIFIER'

    return 'NVARCHAR(MAX)'


def incremental_load(table_configs):
    
    for config_id, table_schema, table_name, id_col, mod_col, load_type in table_configs:
        try:           
            sf_conn = snowflake.connector.connect(
            user='xxxxx',
            password='xxxxx',
            account='xxxxx',
            warehouse='xxxxx',
            database='xxxxx'
            )
            sf_cursor = sf_conn.cursor()
            print("Connected to Snowflake")

            sql_conn_str = (
                'DRIVER=xxxxx;'
                'SERVER=xxxxx;'
                'DATABASE=xxxxx;'
                'UID=xxxxx;'
                'PWD=xxxxx;'
            )
            sql_conn = pyodbc.connect(sql_conn_str)
            sql_cursor = sql_conn.cursor()
            print("Connected to Azure SQL MI Database")

            #Check if target table exists; if not, create it dynamically
            sql_cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}';
            """)
            table_exists = sql_cursor.fetchone()[0] > 0

            if not table_exists:
                print(f"Target table [{table_schema}].[{table_name}] not found — creating it from Snowflake metadata.")
                
                # Pull schema from Snowflake
                sf_cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """)
                columns_meta = sf_cursor.fetchall()

                column_defs = []
                for col_name, data_type, precision, scale in columns_meta:
                    mapped_type = map_snowflake_to_azuresql(data_type, precision, scale)
                    clean_col = col_name.replace(" ", "_").replace("-", "_")
                    column_defs.append(f"[{clean_col}] {mapped_type}")

                create_sql = f"CREATE TABLE [{table_schema}].[{table_name}] ({', '.join(column_defs)})"
                sql_cursor.execute(create_sql)
                sql_conn.commit()
                print(f"Created missing target table: [{table_schema}].[{table_name}]")
            
            
            full_table_name = f"[{table_schema}].[stg_{table_name}]"
            print(f"\nProcessing table: {table_name} → {full_table_name}")

            # Drop table if exists
            sql_cursor.execute(f"DROP TABLE IF EXISTS [{table_schema}].[stg_{table_name}];")
            sql_conn.commit()

            # 2. Create staging table
            sf_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            columns_meta = sf_cursor.fetchall()
            column_names = [col[0] for col in columns_meta]

            column_defs = []
            for col_name, data_type, precision, scale in columns_meta:
                mapped_type = map_snowflake_to_azuresql(data_type, precision, scale)
                clean_col = col_name.replace(" ", "_").replace("-", "_")
                column_defs.append(f"[{clean_col}] {mapped_type}")
            create_stmt = f"CREATE TABLE {full_table_name} ({', '.join(column_defs)})"
            sql_cursor.execute(create_stmt)
            sql_conn.commit()
            print(f"Created table: {full_table_name}")
            
            # Compare target and staging schema — recreate target if mismatch
            print(f"Checking schema consistency between [{table_schema}].[{table_name}] and [{table_schema}].[stg_{table_name}]")

            # Get column metadata for both
            sql_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ISNULL(CHARACTER_MAXIMUM_LENGTH,0), ISNULL(NUMERIC_PRECISION,0), ISNULL(NUMERIC_SCALE,0)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION;
            """)
            target_cols = sql_cursor.fetchall()

            sql_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ISNULL(CHARACTER_MAXIMUM_LENGTH,0), ISNULL(NUMERIC_PRECISION,0), ISNULL(NUMERIC_SCALE,0)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = 'stg_{table_name}'
            ORDER BY ORDINAL_POSITION;
            """)
            staging_cols = sql_cursor.fetchall()

            target_sig = col_signature(target_cols)
            staging_sig = col_signature(staging_cols)

            if target_sig != staging_sig:
                SchemaChange = 'Y'
                print(f"Schema mismatch detected for [{table_schema}].[{table_name}]. Recreating target table...")

                # Drop target table
                sql_cursor.execute(f"DROP TABLE IF EXISTS [{table_schema}].[{table_name}]")
                sql_conn.commit()

                # Pull schema from Snowflake
                sf_cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """)
                columns_meta = sf_cursor.fetchall()

                column_defs = []
                for col_name, data_type, precision, scale in columns_meta:
                    mapped_type = map_snowflake_to_azuresql(data_type, precision, scale)
                    clean_col = col_name.replace(" ", "_").replace("-", "_")
                    column_defs.append(f"[{clean_col}] {mapped_type}")

                create_sql = f"CREATE TABLE [{table_schema}].[{table_name}] ({', '.join(column_defs)})"
                sql_cursor.execute(create_sql)
                sql_conn.commit()
                print(f"Recreated mismatched target table: [{table_schema}].[{table_name}]")
            else:
                SchemaChange = 'N'
                print(f"Schema of target table [{table_schema}].[{table_name}] matches staging — no action needed.")
                
            # 1. Get watermark from target
            sql_cursor.execute(
                f"SELECT ISNULL(MAX(CAST([{mod_col}] AS DATETIME2(7))), '1900-01-01') FROM {table_schema}.[{table_name}]"
            )
            max_date = sql_cursor.fetchone()[0]
            print(f"Watermark in target: {max_date}")
            
            sf_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.{table_name};
                """)
            source_rows = sf_cursor.fetchone()[0]

            # Insert log entry with START
            log_id = log_table_operation(sql_cursor, "START", config_id, table_schema, table_name, load_type,
                                         watermark=max_date, IsSchemaChange=SchemaChange, source_rows=source_rows)
            sql_conn.commit()
            print(f"Log started for {table_name} (LogID={log_id})")


            if load_type.upper() == 'DELETEINSERT':
                          
                # 3. Pull from Snowflake incrementally (IDs only)
                sf_query = f"""
                    SELECT DISTINCT {id_col}
                    FROM {table_schema}.{table_name}
                    WHERE {mod_col} > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                incremental_rows = [row[0] for row in sf_cursor.fetchall()]  # extract IDs into a list

                full_rows = []
                if incremental_rows:
                    print(f"Found {len(incremental_rows)} incremental IDs")

                    chunk_size = 200000
                    for i in range(0, len(incremental_rows), chunk_size):
                        chunk = incremental_rows[i:i + chunk_size]

                        if isinstance(chunk[0], str):
                            id_list = ",".join([f"'{x}'" for x in chunk])
                        else:
                            id_list = ",".join([str(x) for x in chunk])

                        sf_incremental_query = f"""
                            SELECT *
                            FROM {table_schema}.{table_name}
                            WHERE {id_col} IN ({id_list})
                        """
                        sf_cursor.execute(sf_incremental_query)
                        rows = sf_cursor.fetchall()
                        full_rows.extend(rows)
                        print(f"    ➕ Pulled {len(rows)} rows in batch {i//chunk_size + 1}")

                    print(f"Fetched total {len(full_rows)} rows for incremental load")

                else:
                    print("No new rows found")
              
                
                # 4. Load into staging table
                if full_rows:
                    insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                    sql_cursor.fast_executemany = True

                    batch_size = 10000
                    total_inserted = 0
                    for i in range(0, len(full_rows), batch_size):
                        batch = full_rows[i:i + batch_size]
                        sql_cursor.executemany(insert_stmt, batch)
                        sql_conn.commit()
                        total_inserted += len(batch)
                        print(f"    ➕ Inserted {total_inserted} rows")

                    print(f"Finished loading staging for table: {table_name}")
                else:
                    print(f"No rows to insert into staging for {table_name}")
                
                # 5. Count updated & inserted rows
                sql_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM(
                    SELECT DISTINCT t.*
                    FROM {table_schema}.[{table_name}] t
                    JOIN {full_table_name} s
                      ON t.{id_col} = s.{id_col} and t.{mod_col} <> s.{mod_col}
                ) as A;
                """)
                updated_rows = sql_cursor.fetchone()[0]

                sql_cursor.execute(f"""
                SELECT COUNT(*)
                FROM {full_table_name} s
                LEFT JOIN {table_schema}.[{table_name}] t
                  ON t.{id_col} = s.{id_col}
                WHERE t.{id_col} IS NULL;
                """)
                inserted_rows = sql_cursor.fetchone()[0]

                # 6. Merge
                merge_sql = f"""
                DELETE t
                FROM {table_schema}.[{table_name}] t
                JOIN (select distinct {id_col} from {full_table_name}) s
                  ON t.{id_col} = s.{id_col};

                INSERT INTO {table_schema}.[{table_name}] ({",".join('['+c+']' for c in column_names)})
                SELECT {",".join('s.['+c+']' for c in column_names)}
                FROM {full_table_name} s;
                """
                sql_cursor.execute(merge_sql)
                sql_conn.commit()

                total_merged = updated_rows + inserted_rows
                print(f"Updated {updated_rows}, Inserted {inserted_rows}, Total {total_merged} for {table_name}")

                # 7. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 8. Update log entry with results
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=0,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
                
            elif load_type.upper() == 'UPSERT':              
                
                # 3. Pull from Snowflake incrementally
                sf_query = f"""
                    SELECT * 
                    FROM {table_schema}.{table_name}
                    WHERE {mod_col} > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                sql_cursor.fast_executemany = True

                batch_size = 10000
                total_inserted = 0
                while True:
                    rows = sf_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    sql_cursor.executemany(insert_stmt, rows)
                    sql_conn.commit()
                    total_inserted += len(rows)
                    print(f"    ➕ Inserted {total_inserted} rows")

                print(f"Finished loading staging for table: {table_name}")
                
                # 4. Count updated & inserted rows
                sql_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM(
                    SELECT DISTINCT t.*
                    FROM {table_schema}.[{table_name}] t
                    JOIN {full_table_name} s
                      ON t.{id_col} = s.{id_col}
                     AND t.{mod_col} < s.{mod_col}
                ) as A;
                """)
                updated_rows = sql_cursor.fetchone()[0]

                sql_cursor.execute(f"""
                SELECT COUNT(*)
                FROM {full_table_name} s
                LEFT JOIN {table_schema}.[{table_name}] t
                  ON t.{id_col} = s.{id_col}
                WHERE t.{id_col} IS NULL;
                """)
                inserted_rows = sql_cursor.fetchone()[0]

                # 5. Merge
                update_assignments = ", ".join([f"T.[{c}] = S.[{c}]" for c in column_names if c.lower() not in [id_col.lower()]])
                merge_sql = f"""
                MERGE {table_schema}.[{table_name}] AS T
                USING {full_table_name} AS S
                ON T.{id_col} = S.{id_col}
                WHEN MATCHED AND T.{mod_col} < S.{mod_col} THEN
                    UPDATE SET 
                        {update_assignments}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({",".join('['+c+']' for c in column_names)})
                    VALUES ({",".join('S.['+c+']' for c in column_names)});
                """
                sql_cursor.execute(merge_sql)
                sql_conn.commit()

                total_merged = updated_rows + inserted_rows
                print(f"Updated {updated_rows}, Inserted {inserted_rows}, Total {total_merged} for {table_name}")

                # 6. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 7. Update log entry with results
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=0,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
                
            elif load_type.upper() == 'UPSERTDELETE':              
                
                # 3. Pull from Snowflake incrementally
                sf_query = f"""
                    SELECT * 
                    FROM {table_schema}.{table_name}
                    WHERE {mod_col} > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                sql_cursor.fast_executemany = True

                batch_size = 10000
                total_inserted = 0
                while True:
                    rows = sf_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    sql_cursor.executemany(insert_stmt, rows)
                    sql_conn.commit()
                    total_inserted += len(rows)
                    print(f"    ➕ Inserted {total_inserted} rows")

                print(f"Finished loading staging for table: {table_name}")
                
                # 4. Count updated & inserted rows
                sql_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM(
                    SELECT DISTINCT t.*
                    FROM {table_schema}.[{table_name}] t
                    JOIN {full_table_name} s
                      ON t.{id_col} = s.{id_col}
                     AND t.{mod_col} < s.{mod_col}
                ) as A;
                """)
                updated_rows = sql_cursor.fetchone()[0]

                sql_cursor.execute(f"""
                SELECT COUNT(*)
                FROM {full_table_name} s
                LEFT JOIN {table_schema}.[{table_name}] t
                  ON t.{id_col} = s.{id_col}
                WHERE t.{id_col} IS NULL;
                """)
                inserted_rows = sql_cursor.fetchone()[0]

                # 5. Merge
                update_assignments = ", ".join([f"T.[{c}] = S.[{c}]" for c in column_names if c.lower() not in [id_col.lower()]])
                merge_sql = f"""
                MERGE {table_schema}.[{table_name}] AS T
                USING {full_table_name} AS S
                ON T.{id_col} = S.{id_col}
                WHEN MATCHED AND T.{mod_col} < S.{mod_col} THEN
                    UPDATE SET 
                        {update_assignments}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({",".join('['+c+']' for c in column_names)})
                    VALUES ({",".join('S.['+c+']' for c in column_names)});
                """
                sql_cursor.execute(merge_sql)
                sql_conn.commit()

                total_merged = updated_rows + inserted_rows
                print(f"Updated {updated_rows}, Inserted {inserted_rows}, Total {total_merged} for {table_name}")
                
                # 5b. Delete records in target that no longer exist in source
                print("Checking for deleted records in source...")

                # --- Step 1: Fetch all IDs from source (Snowflake) ---
                sf_cursor.execute(f"SELECT DISTINCT {id_col} FROM {table_schema}.{table_name}")
                source_ids = [str(row[0]) for row in sf_cursor.fetchall()]
                source_ids_set = set(source_ids)
                print(f"Fetched {len(source_ids)} IDs from source for deletion check.")

                # --- Step 2: Fetch all IDs from target (SQL MI) ---
                sql_cursor.execute(f"SELECT DISTINCT {id_col} FROM {table_schema}.[{table_name}]")
                target_ids = [str(row[0]) for row in sql_cursor.fetchall()]
                print(f"Fetched {len(target_ids)} IDs from target for deletion check.")
                
                deleted_ids = list(set(target_ids) - source_ids_set)
                
                # 4. Load into staging table
                if deleted_ids:
                    print(f"Found {len(deleted_ids)} deleted IDs in source. Loading into _dlt table...")
                    
                    # Create temporary _dlt table
                    dlt_table_name = f"{table_schema}.[{table_name}_dlt]"
                    sql_cursor.execute(f"IF OBJECT_ID('{dlt_table_name}', 'U') IS NOT NULL DROP TABLE {dlt_table_name};")
                    sql_cursor.execute(f"CREATE TABLE {dlt_table_name} (id_col NVARCHAR(4000));")
                    sql_conn.commit()
                    insert_dlt_stmt = f"INSERT INTO {dlt_table_name} (id_col) VALUES (?)"
                    sql_cursor.fast_executemany = True
                    
                    deleted_ids = [str(x) for x in deleted_ids]

                    chunk_size = 10000
                    total_delete_rows = 0
                    for i in range(0, len(deleted_ids), chunk_size):
                        chunk = deleted_ids[i:i + chunk_size]
                        batch_values = [(x,) for x in chunk]
                        sql_cursor.executemany(insert_dlt_stmt, batch_values)
                        sql_conn.commit()
                        total_delete_rows += len(chunk)
                        print(f"    ➕ Inserted {total_delete_rows} rows")

                    print(f"Finished loading dlt for table: {table_name}")
                    
                    # Delete from target using _dlt table
                    print(f"Deleting records from {table_name} using _dlt table...")
                    
                    sql_cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM(
                        SELECT DISTINCT t.*
                        FROM {table_schema}.[{table_name}] t
                        JOIN {dlt_table_name} s
                          ON {id_col} = s.id_col
                    ) as A;
                    """)
                    delete_rows = sql_cursor.fetchone()[0]
                    
                    
                    delete_sql = f"""
                        DELETE t
                        FROM {table_schema}.[{table_name}] t
                        JOIN {dlt_table_name} d on {id_col} = d.id_col;
                    """
                    sql_cursor.execute(delete_sql)
                    sql_conn.commit()
                    
                    # Drop _dlt table
                    sql_cursor.execute(f"DROP TABLE {dlt_table_name}")
                    sql_conn.commit()
                    print(f"Dropped temporary table {table_name}_dlt")
                    
                else:
                    print(f"No rows to insert into dlt for {table_name}")
                    delete_rows = 0
                
                print(f"Deleted {delete_rows} rows from target that no longer exist in source.")
                    

                # 6. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 7. Update log entry with results
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=delete_rows,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
                
            elif load_type.upper() == 'INSERTONLY':
                
                # 3. Pull new rows from Snowflake incrementally (only inserts)
                sf_query = f"""
                    SELECT * 
                    FROM {table_schema}.{table_name}
                    WHERE "{mod_col}" > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                sql_cursor.fast_executemany = True

                batch_size = 10000
                total_inserted = 0
                while True:
                    rows = sf_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    sql_cursor.executemany(insert_stmt, rows)
                    sql_conn.commit()
                    total_inserted += len(rows)
                    print(f"    ➕ Inserted {total_inserted} new rows")

                print(f"Finished loading staging for table: {table_name}")

                # 4. Insert only into target (no updates, no deletes)
                insert_sql = f"""
                    INSERT INTO {table_schema}.[{table_name}] ({",".join('['+c+']' for c in column_names)})
                    SELECT {",".join('s.['+c+']' for c in column_names)}
                    FROM {full_table_name} s
                    ;
                """
                sql_cursor.execute(insert_sql)
                sql_conn.commit()

                # Count how many got inserted
                inserted_rows = total_inserted
                updated_rows = 0
                total_merged = inserted_rows

                print(f"Inserted {inserted_rows} new rows for {table_name}")

                # 5. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 6. Update log entry
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=0,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
                
            elif load_type.upper() == 'TRUNCATEINSERT':
                           
                 # 2. Drop staging
                sql_cursor.execute(f"Truncate TABLE {table_schema}.{table_name}")
                sql_conn.commit()
                print(f"Truncated table {table_schema}.{table_name}")
                
                # 3. Pull new rows from Snowflake incrementally (only inserts)
                sf_query = f"""
                    SELECT * 
                    FROM {table_schema}.{table_name}
                """
                sf_cursor.execute(sf_query)
                insert_stmt = f"INSERT INTO {table_schema}.{table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                sql_cursor.fast_executemany = True

                batch_size = 10000
                total_inserted = 0
                while True:
                    rows = sf_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    sql_cursor.executemany(insert_stmt, rows)
                    sql_conn.commit()
                    total_inserted += len(rows)
                    print(f"    ➕ Inserted {total_inserted} new rows")

                print(f"Finished loading table: {table_name}")

                # Count how many got inserted
                inserted_rows = total_inserted
                updated_rows = 0
                total_merged = inserted_rows

                print(f"Inserted {inserted_rows} new rows for {table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]
                
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")

                # 6. Update log entry
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=0,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")         

            elif load_type.upper() == 'DELETEINSERTMANY':
                
                # 3. Pull from Snowflake incrementally (IDs only)
                sf_query = f"""
                    SELECT DISTINCT CONCAT({id_col})
                    FROM {table_schema}.{table_name}
                    WHERE {mod_col} > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                incremental_rows = [row[0] for row in sf_cursor.fetchall()]  # extract IDs into a list

                full_rows = []
                if incremental_rows:
                    print(f"Found {len(incremental_rows)} incremental IDs")

                    chunk_size = 200000
                    for i in range(0, len(incremental_rows), chunk_size):
                        chunk = incremental_rows[i:i + chunk_size]

                        if isinstance(chunk[0], str):
                            id_list = ",".join([f"'{x}'" for x in chunk])
                        else:
                            id_list = ",".join([str(x) for x in chunk])

                        sf_incremental_query = f"""
                            SELECT *
                            FROM {table_schema}.{table_name}
                            WHERE CONCAT({id_col}) IN ({id_list})
                        """
                        sf_cursor.execute(sf_incremental_query)
                        rows = sf_cursor.fetchall()
                        full_rows.extend(rows)
                        print(f"    ➕ Pulled {len(rows)} rows in batch {i//chunk_size + 1}")

                    print(f"Fetched total {len(full_rows)} rows for incremental load")

                else:
                    print("No new rows found")
              
                
                # 4. Load into staging table
                if full_rows:
                    insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                    sql_cursor.fast_executemany = True

                    batch_size = 10000
                    total_inserted = 0
                    for i in range(0, len(full_rows), batch_size):
                        batch = full_rows[i:i + batch_size]
                        sql_cursor.executemany(insert_stmt, batch)
                        sql_conn.commit()
                        total_inserted += len(batch)
                        print(f"    ➕ Inserted {total_inserted} rows")

                    print(f"Finished loading staging for table: {table_name}")
                else:
                    print(f"No rows to insert into staging for {table_name}")
                    
                # 5. Count updated & inserted rows
                join_expr = "CONCAT(" + ", ".join([f"t.{col.strip()}" for col in id_col.split(",")]) + ")" \
                             + " = CONCAT(" + ", ".join([f"s.{col.strip()}" for col in id_col.split(",")]) + ")"

                
                # 5. Count updated & inserted rows
                sql_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM(
                    SELECT DISTINCT t.*
                    FROM {table_schema}.[{table_name}] t
                    JOIN {full_table_name} s
                      ON {join_expr} and t.{mod_col} <> s.{mod_col}
                ) as A;
                """)
                updated_rows = sql_cursor.fetchone()[0]

                sql_cursor.execute(f"""
                SELECT COUNT(*)
                FROM {full_table_name} s
                LEFT JOIN {table_schema}.[{table_name}] t
                  ON {join_expr}
                WHERE { " AND ".join([f"t.{col.strip()} IS NULL" for col in id_col.split(",")]) };
                """)
                inserted_rows = sql_cursor.fetchone()[0]
                

                # 6. Merge
                merge_sql = f"""
                DELETE t
                FROM {table_schema}.[{table_name}] t
                JOIN (select distinct {id_col} from {full_table_name}) s
                  ON {join_expr};

                INSERT INTO {table_schema}.[{table_name}] ({",".join('['+c+']' for c in column_names)})
                SELECT {",".join('s.['+c+']' for c in column_names)}
                FROM {full_table_name} s;          
                """
                sql_cursor.execute(merge_sql)
                sql_conn.commit()

                total_merged = updated_rows + inserted_rows
                print(f"Updated {updated_rows}, Inserted {inserted_rows}, Total {total_merged} for {table_name}")

                # 7. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 8. Update log entry with results
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=0,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
                
            elif load_type.upper() == 'DELETEREINSERTMANY':
                
                # 3. Pull from Snowflake incrementally (IDs only)
                sf_query = f"""
                    SELECT DISTINCT CONCAT({id_col})
                    FROM {table_schema}.{table_name}
                    WHERE {mod_col} > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                incremental_rows = [row[0] for row in sf_cursor.fetchall()]  # extract IDs into a list

                full_rows = []
                if incremental_rows:
                    print(f"Found {len(incremental_rows)} incremental IDs")

                    chunk_size = 200000
                    for i in range(0, len(incremental_rows), chunk_size):
                        chunk = incremental_rows[i:i + chunk_size]

                        if isinstance(chunk[0], str):
                            id_list = ",".join([f"'{x}'" for x in chunk])
                        else:
                            id_list = ",".join([str(x) for x in chunk])

                        sf_incremental_query = f"""
                            SELECT *
                            FROM {table_schema}.{table_name}
                            WHERE CONCAT({id_col}) IN ({id_list})
                        """
                        sf_cursor.execute(sf_incremental_query)
                        rows = sf_cursor.fetchall()
                        full_rows.extend(rows)
                        print(f"    ➕ Pulled {len(rows)} rows in batch {i//chunk_size + 1}")

                    print(f"Fetched total {len(full_rows)} rows for incremental load")

                else:
                    print("No new rows found")
              
                
                # 4. Load into staging table
                if full_rows:
                    insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                    sql_cursor.fast_executemany = True

                    batch_size = 10000
                    total_inserted = 0
                    for i in range(0, len(full_rows), batch_size):
                        batch = full_rows[i:i + batch_size]
                        sql_cursor.executemany(insert_stmt, batch)
                        sql_conn.commit()
                        total_inserted += len(batch)
                        print(f"    ➕ Inserted {total_inserted} rows")

                    print(f"Finished loading staging for table: {table_name}")
                else:
                    print(f"No rows to insert into staging for {table_name}")
                    
                # 5. Count updated & inserted rows
                join_expr = "CONCAT(" + ", ".join([f"t.{col.strip()}" for col in id_col.split(",")]) + ")" \
                             + " = CONCAT(" + ", ".join([f"s.{col.strip()}" for col in id_col.split(",")]) + ")"

                
                # 5. Count updated & inserted rows
                sql_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM(
                    SELECT DISTINCT t.*
                    FROM {table_schema}.[{table_name}] t
                    JOIN {full_table_name} s
                      ON {join_expr} and t.{mod_col} <> s.{mod_col}
                ) as A;
                """)
                updated_rows = sql_cursor.fetchone()[0]

                sql_cursor.execute(f"""
                SELECT COUNT(*)
                FROM {full_table_name} s
                LEFT JOIN {table_schema}.[{table_name}] t
                  ON {join_expr}
                WHERE { " AND ".join([f"t.{col.strip()} IS NULL" for col in id_col.split(",")]) };
                """)
                inserted_rows = sql_cursor.fetchone()[0]
                

                # 6. Merge
                merge_sql = f"""
                DELETE t
                FROM {table_schema}.[{table_name}] t
                JOIN (select distinct {id_col} from {full_table_name}) s
                  ON {join_expr};

                INSERT INTO {table_schema}.[{table_name}] ({",".join('['+c+']' for c in column_names)})
                SELECT {",".join('s.['+c+']' for c in column_names)}
                FROM {full_table_name} s;          
                """
                sql_cursor.execute(merge_sql)
                sql_conn.commit()

                total_merged = updated_rows + inserted_rows
                print(f"Updated {updated_rows}, Inserted {inserted_rows}, Total {total_merged} for {table_name}")
                
                # 5b. Delete records in target that no longer exist in source
                print("Checking for deleted records in source...")

                # --- Step 1: Fetch all IDs from source (Snowflake) ---
                sf_cursor.execute(f"SELECT DISTINCT CONCAT({id_col}) FROM {table_schema}.{table_name}")
                source_ids = [str(row[0]) for row in sf_cursor.fetchall()]
                source_ids_set = set(source_ids)
                print(f"Fetched {len(source_ids)} IDs from source for deletion check.")

                # --- Step 2: Fetch all IDs from target (SQL MI) ---
                sql_cursor.execute(f"SELECT DISTINCT CONCAT({id_col}) FROM {table_schema}.[{table_name}]")
                target_ids = [str(row[0]) for row in sql_cursor.fetchall()]
                print(f"Fetched {len(target_ids)} IDs from target for deletion check.")
                
                deleted_ids = list(set(target_ids) - source_ids_set)
                
                # 4. Load into staging table
                if deleted_ids:
                    print(f"Found {len(deleted_ids)} deleted IDs in source. Loading into _dlt table...")
                    
                    # Create temporary _dlt table
                    dlt_table_name = f"{table_schema}.[{table_name}_dlt]"
                    sql_cursor.execute(f"IF OBJECT_ID('{dlt_table_name}', 'U') IS NOT NULL DROP TABLE {dlt_table_name};")
                    sql_cursor.execute(f"CREATE TABLE {dlt_table_name} (id_col NVARCHAR(4000));")
                    sql_conn.commit()
                    insert_dlt_stmt = f"INSERT INTO {dlt_table_name} (id_col) VALUES (?)"
                    sql_cursor.fast_executemany = True
                    
                    deleted_ids = [str(x) for x in deleted_ids]

                    chunk_size = 10000
                    total_delete_rows = 0
                    for i in range(0, len(deleted_ids), chunk_size):
                        chunk = deleted_ids[i:i + chunk_size]
                        batch_values = [(x,) for x in chunk]
                        sql_cursor.executemany(insert_dlt_stmt, batch_values)
                        sql_conn.commit()
                        total_delete_rows += len(chunk)
                        print(f"    ➕ Inserted {total_delete_rows} rows")

                    print(f"Finished loading dlt for table: {table_name}")
                    
                    # Delete from target using _dlt table
                    print(f"Deleting records from {table_name} using _dlt table...")
                    
                    sql_cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM(
                        SELECT DISTINCT t.*
                        FROM {table_schema}.[{table_name}] t
                        JOIN {dlt_table_name} s
                          ON CONCAT({id_col}) = s.id_col
                    ) as A;
                    """)
                    delete_rows = sql_cursor.fetchone()[0]
                    
                    
                    delete_sql = f"""
                        DELETE t
                        FROM {table_schema}.[{table_name}] t
                        JOIN {dlt_table_name} d on CONCAT({id_col}) = d.id_col;
                    """
                    sql_cursor.execute(delete_sql)
                    sql_conn.commit()
                    
                    # Drop _dlt table
                    sql_cursor.execute(f"DROP TABLE {dlt_table_name}")
                    sql_conn.commit()
                    print(f"Dropped temporary table {table_name}_dlt")
                
                else:
                    print(f"No rows to insert into dlt for {table_name}")
                    delete_rows = 0
                
                print(f"Deleted {delete_rows} rows from target that no longer exist in source.")


                # 7. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 8. Update log entry with results
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=delete_rows,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
                
            elif load_type.upper() == 'DELETEREINSERT':
                          
                # 3. Pull from Snowflake incrementally (IDs only)
                sf_query = f"""
                    SELECT DISTINCT {id_col}
                    FROM {table_schema}.{table_name}
                    WHERE {mod_col} > TO_TIMESTAMP('{max_date}', 'YYYY-MM-DD HH24:MI:SS.FF')
                """
                sf_cursor.execute(sf_query)
                incremental_rows = [row[0] for row in sf_cursor.fetchall()]  # extract IDs into a list

                full_rows = []
                if incremental_rows:
                    print(f"Found {len(incremental_rows)} incremental IDs")

                    chunk_size = 200000
                    for i in range(0, len(incremental_rows), chunk_size):
                        chunk = incremental_rows[i:i + chunk_size]

                        if isinstance(chunk[0], str):
                            id_list = ",".join([f"'{x}'" for x in chunk])
                        else:
                            id_list = ",".join([str(x) for x in chunk])

                        sf_incremental_query = f"""
                            SELECT *
                            FROM {table_schema}.{table_name}
                            WHERE {id_col} IN ({id_list})
                        """
                        sf_cursor.execute(sf_incremental_query)
                        rows = sf_cursor.fetchall()
                        full_rows.extend(rows)
                        print(f"    ➕ Pulled {len(rows)} rows in batch {i//chunk_size + 1}")

                    print(f"Fetched total {len(full_rows)} rows for incremental load")

                else:
                    print("No new rows found")
              
                
                # 4. Load into staging table
                if full_rows:
                    insert_stmt = f"INSERT INTO {full_table_name} VALUES ({', '.join(['?' for _ in column_names])})"
                    sql_cursor.fast_executemany = True

                    batch_size = 10000
                    total_inserted = 0
                    for i in range(0, len(full_rows), batch_size):
                        batch = full_rows[i:i + batch_size]
                        sql_cursor.executemany(insert_stmt, batch)
                        sql_conn.commit()
                        total_inserted += len(batch)
                        print(f"    ➕ Inserted {total_inserted} rows")

                    print(f"Finished loading staging for table: {table_name}")
                else:
                    print(f"No rows to insert into staging for {table_name}")
                
                # 5. Count updated & inserted rows
                sql_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM(
                    SELECT DISTINCT t.*
                    FROM {table_schema}.[{table_name}] t
                    JOIN {full_table_name} s
                      ON t.{id_col} = s.{id_col} and t.{mod_col} <> s.{mod_col}
                ) as A;
                """)
                updated_rows = sql_cursor.fetchone()[0]

                sql_cursor.execute(f"""
                SELECT COUNT(*)
                FROM {full_table_name} s
                LEFT JOIN {table_schema}.[{table_name}] t
                  ON t.{id_col} = s.{id_col}
                WHERE t.{id_col} IS NULL;
                """)
                inserted_rows = sql_cursor.fetchone()[0]

                # 6. Merge
                merge_sql = f"""
                DELETE t
                FROM {table_schema}.[{table_name}] t
                JOIN (select distinct {id_col} from {full_table_name}) s
                  ON t.{id_col} = s.{id_col};

                INSERT INTO {table_schema}.[{table_name}] ({",".join('['+c+']' for c in column_names)})
                SELECT {",".join('s.['+c+']' for c in column_names)}
                FROM {full_table_name} s;
                """
                sql_cursor.execute(merge_sql)
                sql_conn.commit()

                total_merged = updated_rows + inserted_rows
                print(f"Updated {updated_rows}, Inserted {inserted_rows}, Total {total_merged} for {table_name}")
                
                # 5b. Delete records in target that no longer exist in source
                print("Checking for deleted records in source...")

                # --- Step 1: Fetch all IDs from source (Snowflake) ---
                sf_cursor.execute(f"SELECT DISTINCT {id_col} FROM {table_schema}.{table_name}")
                source_ids = [str(row[0]) for row in sf_cursor.fetchall()]
                source_ids_set = set(source_ids)
                print(f"Fetched {len(source_ids)} IDs from source for deletion check.")

                # --- Step 2: Fetch all IDs from target (SQL MI) ---
                sql_cursor.execute(f"SELECT DISTINCT {id_col} FROM {table_schema}.[{table_name}]")
                target_ids = [str(row[0]) for row in sql_cursor.fetchall()]
                print(f"Fetched {len(target_ids)} IDs from target for deletion check.")
                
                deleted_ids = list(set(target_ids) - source_ids_set)
                
                # 4. Load into staging table
                if deleted_ids:
                    print(f"Found {len(deleted_ids)} deleted IDs in source. Loading into _dlt table...")
                    
                    # Create temporary _dlt table
                    dlt_table_name = f"{table_schema}.[{table_name}_dlt]"
                    sql_cursor.execute(f"IF OBJECT_ID('{dlt_table_name}', 'U') IS NOT NULL DROP TABLE {dlt_table_name};")
                    sql_cursor.execute(f"CREATE TABLE {dlt_table_name} (id_col NVARCHAR(4000));")
                    sql_conn.commit()
                    insert_dlt_stmt = f"INSERT INTO {dlt_table_name} (id_col) VALUES (?)"
                    sql_cursor.fast_executemany = True
                    
                    deleted_ids = [str(x) for x in deleted_ids]

                    chunk_size = 10000
                    total_delete_rows = 0
                    for i in range(0, len(deleted_ids), chunk_size):
                        chunk = deleted_ids[i:i + chunk_size]
                        batch_values = [(x,) for x in chunk]
                        sql_cursor.executemany(insert_dlt_stmt, batch_values)
                        sql_conn.commit()
                        total_delete_rows += len(chunk)
                        print(f"    ➕ Inserted {total_delete_rows} rows")

                    print(f"Finished loading dlt for table: {table_name}")
                    
                    # Delete from target using _dlt table
                    print(f"Deleting records from {table_name} using _dlt table...")

                    sql_cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM(
                        SELECT DISTINCT t.*
                        FROM {table_schema}.[{table_name}] t
                        JOIN {dlt_table_name} s
                          ON {id_col} = s.id_col
                    ) as A;
                    """)
                    delete_rows = sql_cursor.fetchone()[0]
                    
                    
                    delete_sql = f"""
                        DELETE t
                        FROM {table_schema}.[{table_name}] t
                        JOIN {dlt_table_name} d on {id_col} = d.id_col;
                    """
                    sql_cursor.execute(delete_sql)
                    sql_conn.commit()
                    
                    # Drop _dlt table
                    sql_cursor.execute(f"DROP TABLE {dlt_table_name}")
                    sql_conn.commit()
                    print(f"Dropped temporary table {table_name}_dlt")
                    
                else:
                    print(f"No rows to insert into dlt for {table_name}")
                    delete_rows = 0
                
                print(f"Deleted {delete_rows} rows from target that no longer exist in source.")
                    
                # 7. Drop staging
                sql_cursor.execute(f"DROP TABLE {full_table_name}")
                sql_conn.commit()
                print(f"Dropped staging {full_table_name}")
                
                sql_cursor.execute(f"""
                SELECT COUNT(*) FROM {table_schema}.[{table_name}];
                """)
                target_rows = sql_cursor.fetchone()[0]

                # 8. Update log entry with results
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    updated_rows=updated_rows, inserted_rows=inserted_rows,
                                    total_merged=total_merged, target_rows=target_rows, delete_rows=delete_rows,
                                    log_id=log_id, status="Success")
                sql_conn.commit()
                print(f"Log updated for {table_name} (LogID={log_id})")
 
                
        except pyodbc.Error as db_err:
            # SQL Server error
            error_message = str(db_err)
            print(f"SQL Error processing {table_name}: {error_message}")

            if log_id:
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    log_id=log_id, status="Failure", error_message=str(db_err))
                sql_conn.commit()

        except Exception as e:
            print(f"Error processing {table_name}: {e}")
            if log_id:
                log_table_operation(sql_cursor, "END", config_id, table_schema, table_name, load_type,
                                    log_id=log_id, status="Failure", error_message=str(e))
                sql_conn.commit()

            sql_conn.close()
            sf_conn.close()
                    
                
def main():
    
    with get_sql_conn() as sql_conn:
        sql_cursor =  sql_conn.cursor()
        
        # Pull config from dbo.Table_Config instead of scanning Snowflake only
        sql_cursor.execute("""
            SELECT config_id, table_schema, table_name, id_column, modified_date_column, LoadType
            FROM Audit.Table_Config where is_active = 'Y'
            order by config_id asc
        """)
        table_configs = sql_cursor.fetchall()
        print(f"Found {len(table_configs)} tables in Table_Config.")
        
        # Run in parallel with a pool of workers
        max_workers = 10   # you can tune this (10–20 is safe)
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for config in table_configs:
                futures.append(executor.submit(incremental_load, [config]))  # pass single config at a time
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"Thread failed: {e}")
        
        sql_query = """
        INSERT [AthenaHealth].[Audit].[LoadProcess] 
              ([LoadDate]
              ,[AnySchemaChange]
              ,[TotalProcessedrows]
              ,[LoadprocessStartTime]
              ,[LoadprocessEndTime]
              ,[Loadrunduration]
              ,[LoadStatus]
              ,[TotalFailures]
              ,[Created_at]
          )
          SELECT  CAST(Created_at AS date) AS LoadDate,
                case when SUM(case when IsSchemaChange = 'Y' 
                            then 1  else 0 end) > 0 THEN 'Y'
                            ELSE 'N' END as AnySchemaChange,
                SUM([TotalMergedRows]+[DeletedRows]) AS [TotalProcessedrows],
                MIN(LoadProcessEndTime) AS LoadStartTime,
                MAX(LoadProcessEndTime) AS LoadEndTime,
                LTRIM(
                    CONCAT(
                        CASE WHEN DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) / 86400 > 0 
                            THEN CONCAT(DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) / 86400, 'd ') ELSE '' END,
                        CASE WHEN (DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) % 86400) / 3600 > 0 
                            THEN CONCAT((DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) % 86400) / 3600, 'h ') ELSE '' END,
                        CASE WHEN (DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) % 3600) / 60 > 0 
                            THEN CONCAT((DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) % 3600) / 60, 'm ') ELSE '' END,
                        CASE WHEN (DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) % 60) > 0 
                            THEN CONCAT((DATEDIFF(SECOND, MIN(LoadProcessEndTime), MAX(LoadProcessEndTime)) % 60), 's') ELSE '' END
                    )
                ) AS Duration,
                case when SUM(case when LoadStatus = 'Failure'
                            OR RowMatchFlag = 'N' 
                            OR ErrorMessage <> 'No Error' 
                            then 1  else 0 end) > 0 THEN 'Partially Completed'
                        Else 'Fully Completed' END as LoadStatus,
                SUM(case when LoadStatus = 'Failure' then 1  else 0 end) as TotalFailures,
                SYSDATETIME()
        FROM Athenahealth.audit.table_logs
        WHERE CAST(Created_at AS date) = cast(getdate() as date)
        group by CAST(Created_at AS date);
        """

        sql_cursor.execute(sql_query)
        sql_conn.commit()

        print("Load summary inserted successfully into Audit.LoadProcess")

        print("All tables processed in parallel")
                
        
        print("\nAll tables migrated successfully.")
    
    
if __name__ == "__main__":
    main()



								

