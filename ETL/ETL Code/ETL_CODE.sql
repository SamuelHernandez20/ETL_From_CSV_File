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


CREATE TABLE ventas_final(
    ventas_final_id NUMBER PRIMARY KEY,
    fecha DATE,
    artista_id NUMBER,
    monto NUMBER,
    pais_origen VARCHAR2(50),
    categoria VARCHAR2(20) 
)


EXECUTE dbms_session.reset_package;
SET SERVEROUTPUT ON SIZE UNLIMITED
EXECUTE sys.dbms_output.enable(NULL);
CLEAR SCREEN

 
select * from ventas_ext;
   
 
SELECT column_name, data_type, data_length, data_precision, data_scale
FROM user_tab_columns
WHERE table_name = 'VENTAS_EXT';
   

create or replace procedure format_data (
   p_ventas_id_sin_convertir IN OUT ventas_ext.ventas_id%type,
   p_cadena_fecha_sin_convertir IN OUT ventas_ext.fecha%type,
   p_artista_id_sin_convertir IN OUT ventas_ext.artista_id%type,
   p_monto_sin_convertir IN OUT ventas_ext.monto%type)   
as
-- Declaración de la constante para la condición:
 v_condicion CONSTANT BOOLEAN :=
 p_ventas_id_sin_convertir IS NOT NULL 
 AND p_cadena_fecha_sin_convertir IS NOT NULL 
 AND p_artista_id_sin_convertir IS NOT NULL
 AND p_monto_sin_convertir IS NOT NULL;

BEGIN
    IF v_condicion THEN
    
      p_ventas_id_sin_convertir := TO_NUMBER(p_ventas_id_sin_convertir);
      
      p_cadena_fecha_sin_convertir := TO_DATE(p_cadena_fecha_sin_convertir, 'YYYY-MM-DD');

      p_artista_id_sin_convertir := TO_NUMBER(p_artista_id_sin_convertir);
      
      p_monto_sin_convertir := TO_NUMBER(p_monto_sin_convertir);
    END IF;
END;
/

create or replace procedure adjust_amount (p_monto IN OUT NUMBER)   
as
BEGIN
 p_monto := trunc(p_monto,0);
 
IF p_monto < -999999 THEN
 p_monto := 0;
ELSIF p_monto >= -999999 AND p_monto <= -1 THEN
 p_monto := ABS(p_monto);
END IF;

IF p_monto > 999999 THEN
 p_monto := 999999;
END IF;

END; 
/

create or replace procedure international_category (p_pais_origen VARCHAR2, p_categoria OUT VARCHAR2)   
as
v_categoría_internacional VARCHAR2(20) := 'Internacional';
v_categoría_nacional VARCHAR2(20) := 'Nacional';
BEGIN
 
 IF p_pais_origen != 'España' AND p_pais_origen != 'México' THEN
    p_categoria := v_categoría_internacional;
 ELSE
    p_categoria := v_categoría_nacional;
 END IF;
END;
/

CREATE OR REPLACE PROCEDURE run_etl_process 
IS
BEGIN
-- Creación del Bloque Anónimo:
DECLARE 
-- Colección para la extracción de los datos la External Table:
   TYPE ventas_artistas IS TABLE OF ventas_ext%rowtype INDEX BY PLS_INTEGER;
   
-- Bloque de Declaración de Variables y Constantes:
   VENTAS_ART ventas_artistas;
   v_categoría VARCHAR2(20);
   v_error BOOLEAN := FALSE;
   
   v_num_registros NUMBER := 0;
   v_contador_commit NUMBER := 0;
   v_num_commit NUMBER := 0;
   v_división_registros_entre_2 NUMBER;
   v_resultado_entre_2 NUMBER;
   v_división_registros_entre_4 NUMBER;
   v_resultado_entre_4 NUMBER;
   v_contador_commit_valor NUMBER := 0;
   
   RANGO_1 BOOLEAN := FALSE;
   RANGO_2 BOOLEAN := FALSE;
   CUATRO_COMMITS BOOLEAN := FALSE;
   DOS_COMMITS BOOLEAN := FALSE;
   
BEGIN
 
   SELECT * BULK COLLECT INTO VENTAS_ART FROM ventas_ext;
   
   DBMS_OUTPUT.PUT_LINE('## Borrado Previo de la Tabla de Destino: ' || '"ventas_final" ##');
   select count(*) into v_num_registros from ventas_final;
   
   IF v_num_registros > 0 THEN
     DBMS_OUTPUT.PUT_LINE('La Tabla de Destino está llena. Ejecutando operación previa de borrado.');
    delete from ventas_final;
    ELSE
    DBMS_OUTPUT.PUT_LINE('??? La Tabla de Destino está vacía ???. Omitiendo operación previa de borrado.');
   END IF;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('## Transformación y Carga de los datos en la Tabla de Destino: ' || '"ventas_final" ##');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Información sobre los COMMITS realizados');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('---------------------------------------------------------------------');
    
    v_división_registros_entre_4 := v_num_registros / 4;
    v_división_registros_entre_2 := v_num_registros / 2;
    
    v_resultado_entre_4 := v_división_registros_entre_4;
    v_resultado_entre_2 := v_división_registros_entre_2;
    
    RANGO_1 := v_num_registros >= 1000 AND v_num_registros <= 10000;
    RANGO_2 := v_num_registros >= 100 AND v_num_registros <= 999;


    FOR i IN 1..VENTAS_ART.COUNT LOOP
      
       SAVEPOINT iteracion_inicio;
       BEGIN
      
      format_data(VENTAS_ART(i).ventas_id, VENTAS_ART(i).fecha, VENTAS_ART(i).artista_id, VENTAS_ART(i).monto); 
      
      adjust_amount (VENTAS_ART(i).monto);
      
      international_category(VENTAS_ART(i).pais_origen, v_categoría);
      
     INSERT INTO ventas_final VALUES(
     VENTAS_ART(i).ventas_id, VENTAS_ART(i).fecha, 
     VENTAS_ART(i).artista_id, VENTAS_ART(i).monto, 
     VENTAS_ART(i).pais_origen, v_categoría);
     
     v_contador_commit := v_contador_commit + i;
   
     CUATRO_COMMITS := v_contador_commit = v_división_registros_entre_4;
     DOS_COMMITS := v_contador_commit = v_división_registros_entre_2;
     
 IF RANGO_1 OR RANGO_2 THEN 
 
    IF RANGO_1 AND CUATRO_COMMITS THEN
    
    COMMIT;
    v_contador_commit_valor := v_contador_commit;
    v_división_registros_entre_4 := v_división_registros_entre_4 + v_resultado_entre_4;
    v_num_commit := v_num_commit + 1;
    DBMS_OUTPUT.PUT_LINE('COMMIT ' || v_num_commit || ' ITERACIÓN: ' || v_contador_commit_valor);
    
   ELSIF RANGO_2 AND DOS_COMMITS THEN
   
    COMMIT;
    v_contador_commit_valor := v_contador_commit;
    v_división_registros_entre_2 := v_división_registros_entre_2 + v_resultado_entre_2;
    v_num_commit := v_num_commit + 1;
    DBMS_OUTPUT.PUT_LINE('COMMIT ' || v_num_commit || ' ITERACIÓN: ' || v_contador_commit_valor);
    
   ELSE
   v_contador_commit := 0;
   v_num_commit := v_num_commit;     
   END IF; 

 END IF;  
 
      EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK TO SAVEPOINT iteracion_inicio;
                v_error := TRUE;
                v_contador_commit := 0;
                v_num_commit := 0;  
      END; 
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('---------------------------------------------------------------------');
    
      IF v_error THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Surgió un error en alguna iteración del bucle.');
        ELSE
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('El proceso se completó de forma correcta.');
      END IF; 
END;
END; 

CREATE OR REPLACE PROCEDURE ExportFormatDataToNew_CSV_File 
IS
DECLARE
ARCHIVO_VENTAS_FORMATEADO UTL_FILE.FILE_TYPE;
BEGIN
ARCHIVO_VENTAS_FORMATEADO := UTL_FILE.FOPEN('ETL_FILES_DIR','ventas_formateado.csv','W');

FOR i in (SELECT column_name FROM user_tab_columns WHERE table_name = 'VENTAS_FINAL')
LOOP
 UTL_FILE.PUT (ARCHIVO_VENTAS_FORMATEADO, i.COLUMN_NAME||',');
END LOOP;
UTL_FILE.PUT (ARCHIVO_VENTAS_FORMATEADO, CHR(10));

FOR i in (select * from ventas_final order by artista_id asc)
LOOP
 UTL_FILE.PUT_LINE (ARCHIVO_VENTAS_FORMATEADO, i.VENTAS_FINAL_ID||','||i.FECHA||','||i.ARTISTA_ID||','||i.MONTO
                    ||','||i.PAIS_ORIGEN||','||i.CATEGORIA);
END LOOP;
UTL_FILE.FCLOSE(ARCHIVO_VENTAS_FORMATEADO);
END;
/
 
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'RUN_ETL_JOB_AND_EXPORT_FORMAT_DATA',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN run_etl_process(); ExportFormatDataToNew_CSV_File(); END;',
        start_date      => SYSDATE,
        repeat_interval => 'FREQ=DAILY; BYHOUR=18; BYMINUTE=30; BYSECOND=0',
        enabled         => TRUE);
        
END;

SELECT * FROM user_scheduler_jobs WHERE job_name = 'RUN_ETL_JOB_AND_EXPORT_FORMAT_DATA';
