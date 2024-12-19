## ETL process from a CSV file with PL/SQL.

## 📝 Description:
Small project developed in the **PL/SQL** procedural language. It consists of a process of **extraction**, **transformation** and **data loading** of a **CSV file** and the **automated export** of the
information transformed to a **new CSV file**.

### 🎯 Objetives:
1. **Extract** the raw data from a **CSV file**, using: `External Table.`
2. Perform the necessary **transformations** on the extracted data.
3. Handling of possible **errors** and **transactions**.
4. **Load** the formatted data into a **destination table**.
5. **Export** the data loaded in the destination table in a **new CSV file**, using: `UTL_FILE.`
6. **Automate** the entire process with the help of: `DBMS_SCHEDULER.`

## ⚙ Initial Settings:
1. Create a new folder where the CSV file from which data extraction will be performed will be hosted on this route:
   
```
C:\app\SYSTEM_USER\product\21c\admin\xe\YOUR_NEW_FOLDER
```
2. Then, I will register the directory in Oracle using the following command:
   
```
CREATE OR REPLACE DIRECTORY YOUR_NEW_FOLDER`_NAME AS 'C:\app\SYSTEM_USER\product\21c\admin\xe\YOUR_NEW_FOLDER';
```


