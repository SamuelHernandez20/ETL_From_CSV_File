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

- External table and Destination table creation

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
  




