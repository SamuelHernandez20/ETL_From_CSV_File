## ETL process from a CSV file with PL/SQL.
Pequeño proyecto desarrollado en el lenguaje procedural **PL/SQL**. Consiste en un proceso de **extracción**, **transformación** y **carga de datos** de un **archivo CSV** y la **exportación automatizada** de la
información transformada a un **nuevo archivo CSV**.

### Objetives:
1. Extraer los datos en crudo desde un archivo CSV. Usando: `External Table`.
2. Realizar las transformaciones necesarias sobre los datos extraídos.
3. Manejo de posibles errores y transacciones.
4. Carga de los datos formateados en una tabla de destino.
5. Exportar los datos cargados en la tabla de destino en un nuevo archivo CSV. Haciendo uso de: `UTL_FILE`.
6. Automatizar todo el proceso. Mediante: `DBMS_SCHEDULER `.

### Initial Settings:


