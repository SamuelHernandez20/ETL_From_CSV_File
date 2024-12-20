## Documentation of the ETL Process from a CSV file with PL/SQL.

![](ETL/Images/ETL_Process_Diagram_.png)

### 📝 Description:
Small project developed in the **PL/SQL** procedural language. It consists of a process of **extraction**, **transformation** and **data loading** of a **CSV file** and the **automated export** of the
information transformed to a **new CSV file**.

### 🛠️ Environment & Version:
- **Language**: PL/SQL.
- **Database**: Oracle Database 21c.
- **Tools/Technologies Used**:
  
  - `External Tables.`
  - `Bulk Collect.`
  - `UTL_FILE.`
  - `DBMS_SCHEDULER.`
    
- **Operating System**: Windows 10.
- **IDE**: Oracle SQL Developer.

### 🎯 Objetives:
1. **Extract** the raw data from a **CSV file**, using: `External Table.`
2. Perform the necessary **transformations** on the extracted data.
3. Handling of possible **errors** and **transactions**.
4. **Load** the formatted data into a **destination table**.
5. **Export** the data loaded in the destination table in a **new CSV file**, using: `UTL_FILE.`
6. **Automate** the entire process with the help of: `DBMS_SCHEDULER.`

## ⚙ Initial Settings:
1. Create a new folder where the CSV file will be housed, in the following path:
   
- `C:\app\SYSTEM_USER\product\21c\admin\xe\YOUR_NEW_FOLDER`

2. Then, I will register the directory in Oracle using the following command:
```sql
CREATE OR REPLACE DIRECTORY YOUR_NEW_FOLDER_ALIAS AS 'C:\app\SYSTEM_USER\product\21c\admin\xe\YOUR_NEW_FOLDER';
```
3. After that, it will be important to assign the appropriate directory permissions and move the CSV file to the create folder:
```sql
GRANT READ, WRITE ON DIRECTORY YOUR_NEW_FOLDER_ALIAS TO YOUR_ORACLE_USER;
```
4. Verify that the directory has been registered correctly:
```sql
 SELECT * FROM ALL_DIRECTORIES WHERE DIRECTORY_NAME = 'YOUR_NEW_FOLDER_ALIAS';
```
## 📋 Code Table of Contents:

| Component | Name | Function |
|----------|----------|----------|
| **External Table** | *ventas_ext*. | Load data from csv file. |
| **Destination Table** | *ventas_final*. | Load the transformed data. |
| **Sub-Procedure** | *format_data*. | format the data type of the external table. |
| **Sub-Procedure** | *adjust_mount*. | Clean dirty values ​​from the amount. |
| **Sub-Procedure** | *international_category*. | Determine the Category according to the country. |
| **Main Procedure** | *run_etl_process*. | Encapsulate the main logic and previous subprocedures. |
| **Procedure** | *ExportFormatDataToNew_CSV_File*. | Export the information transformed and loaded into the destination table to a new csv file. |
| **Job** | *RUN_ETL_JOB_AND_EXPORT_FORMAT_DATA*. | Automate the execution and export process of the ETL process. |

## 💻 Code explanation:

### External table and Destination table creation.

1. This part of the code, I create the **External Table** for the `ventas.csv` file. I specify the columns of the csv file, the access parameters and the location of the csv file.

```sql
 CREATE TABLE ventas_ext
(
ventas_id NUMBER(4),
fecha CHAR(10),
artista_id NUMBER(10),
monto NUMBER(10,2),
pais_origen VARCHAR(50)
)
ORGANIZATION EXTERNAL
(
 TYPE ORACLE_LOADER
 DEFAULT DIRECTORY ETL_FILES_DIR
 ACCESS PARAMETERS (
   RECORDS DELIMITED BY NEWLINE
   CHARACTERSET WE8ISO8859P1
   FIELDS TERMINATED BY ','
   MISSING FIELD VALUES ARE NULL
   (
   ventas_id CHAR(4),
   fecha CHAR(10),
   artista_id CHAR(10),
   monto CHAR(10),
   pais_origen CHAR(50)
 )
 )
 LOCATION ('ventas.csv')
)
REJECT LIMIT UNLIMITED;
```
2. I create the destination table: `ventas_final`, adding an additional column: `category`.
   
```sql
CREATE TABLE ventas_final(
    ventas_final_id NUMBER PRIMARY KEY,
    fecha DATE,
    artista_id NUMBER,
    monto NUMBER,
    pais_origen VARCHAR2(50),
    categoria VARCHAR2(20) 
)
```
3. To avoid oracle error: `ORA-10027` (buffer overflow), run:
   
```sql
EXECUTE dbms_session.reset_package;
SET SERVEROUTPUT ON SIZE UNLIMITED
EXECUTE sys.dbms_output.enable(NULL);
CLEAR SCREEN
```
4. Perform a simple query to check the information in the **External Table**. 
 
```sql   
select * from ventas_ext;
```
5. Using this query you can see the information of the **External Table** in more detail:
   
```sql   
   SELECT column_name, data_type, data_length, data_precision, data_scale
   FROM user_tab_columns
   WHERE table_name = 'VENTAS_EXT';
```
### The three subprocedures of the code:
  - **Firts Subprocedure**: `format_data`.
    
1. I create and define procedure parameters.

```sql   
create or replace procedure format_data (
    p_ventas_id_sin_convertir IN OUT ventas_ext.ventas_id%type,
    p_cadena_fecha_sin_convertir IN OUT ventas_ext.fecha%type,
    p_artista_id_sin_convertir IN OUT ventas_ext.artista_id%type,
    p_monto_sin_convertir IN OUT ventas_ext.monto%type)   
as
```
2. Here I am declaring the **boolean constants** that I will use in the **IF condition**.

```sql   
 v_condicion CONSTANT BOOLEAN :=
 p_ventas_id_sin_convertir IS NOT NULL 
 AND p_cadena_fecha_sin_convertir IS NOT NULL 
 AND p_artista_id_sin_convertir IS NOT NULL
 AND p_monto_sin_convertir IS NOT NULL;
```
3. In the body of the procedure I evaluate that **no parameter is null**, and **if true, I report the data conversion**.  

```sql   
BEGIN
    IF v_condicion THEN
      p_ventas_id_sin_convertir := TO_NUMBER(p_ventas_id_sin_convertir);
      
      p_cadena_fecha_sin_convertir := TO_DATE(p_cadena_fecha_sin_convertir, 'YYYY-MM-DD');

      p_artista_id_sin_convertir := TO_NUMBER(p_artista_id_sin_convertir);

      p_monto_sin_convertir := TO_NUMBER(p_monto_sin_convertir);
    END IF;
END;
/
```
  - **Second Subprocedure**: `adjust_mount`.
1. I create and define procedure parameters. **Remove possible decimal values** ​​from the beginning using: `trunc(arg1, integer)` in the input parameter: `p_monto`.  

```sql   
create or replace procedure adjust_amount (p_monto IN OUT NUMBER)   
as
BEGIN
-- Elimino posibles valores decimales desde el principio:
 p_monto := trunc(p_monto,0);
```



  
  - **third Subprocedure**: `international_category`.        
  




