create or replace PROCEDURE SP_LIMPIA_TABLA (p_table_name VARCHAR2,
                                            p_schema_name VARCHAR2 DEFAULT 'SCH_UNIVERSAL')
/*-------------------------------------------------------------------------------------------------------------------------------------------------
--NOMBRE              : SP_LIMPIA_TABLA
--NRO_RQ/DC/EX/OT     : Proyecto Ecosistema UGI
--OBJETIVO            : Procedimiento que remueve saltos de línea y espacios en blanco en los extremos de una columna,
                        para todas las columnas VARCHAR de una tabla
--AUTOR               : Christian Arutaype
--FECHA               : 21/01/2020
--PARAMETROS_ENTRADA  : Nombre de tabla y esquema
--PARAMETROS_SALIDA   : Registros por campo actualizados y duración total de la actualización
-------------------------------------------------------------------------------------------------------------------------------------------------
MODIFICACIONES
-- FECHA       USUARIO    NRO_RQ   DESCRIPCION_DEL_CAMBIO
------------------------------------------------------------------------------------------------------------------------------------------------*/
IS
  --spv_excp_no_table EXCEPTION;
  spv_update_ini    DATE;
  spv_update_fin    DATE;
  spv_table_name    VARCHAR2(30):= UPPER(p_table_name);
  spv_schema_name   VARCHAR2(30):= UPPER(p_schema_name);
  spv_sql_stmt      VARCHAR2(8000);
  spv_col_stmt      VARCHAR2(30);
  cursor c_upd      (v_tn VARCHAR2, v_sn VARCHAR2) is
                    select 'update /*+ PARALLEL('||table_name||',4) */ '||table_name||' set '||column_name||'=trim(replace(replace('||column_name||',chr(10),''''),chr(13),'''')) where (instr('||column_name||',chr(10))>0 or instr('||column_name||',chr(13))>0 or instr('||column_name||','' '',1,1)=1 or instr('||column_name||','' '',-1,1)=1)'
                      , column_name
                    from all_tab_columns
                    where data_type like 'VARCHAR%' and table_name=v_tn and owner=v_sn;
BEGIN
  DBMS_OUTPUT.ENABLE(1000000);
  --EXECUTE IMMEDIATE 'SET SERVEROUTPUT ON SIZE 100000';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
  OPEN c_upd(spv_table_name, spv_schema_name);
  DBMS_OUTPUT.PUT_LINE('Iniciando revisión y limpieza de columnas VARCHAR...');
  spv_update_ini:= sysdate;
    LOOP
      FETCH c_upd INTO spv_sql_stmt, spv_col_stmt;
      --IF c_upd%NOTFOUND THEN RAISE spv_excp_no_table; END IF;
      EXIT WHEN c_upd%NOTFOUND;
      DBMS_OUTPUT.PUT_LINE('Ejecutando update a registros identificados en la columna: '||spv_col_stmt);
      EXECUTE IMMEDIATE spv_sql_stmt;
      DBMS_OUTPUT.PUT_LINE('Registros actualizados: '||sql%rowcount);
      --DBMS_OUTPUT.PUT_LINE();
      COMMIT;
    END LOOP;
  DBMS_OUTPUT.PUT_LINE('Se revisaron '||c_upd%ROWCOUNT||' columnas');
  spv_update_fin:= sysdate;
  CLOSE c_upd;
  DBMS_OUTPUT.PUT_LINE(spv_update_fin||' - Limpieza finalizada en '||TRUNC((spv_update_fin-spv_update_ini)*(60*60*24))||' seg.');

EXCEPTION
  --WHEN spv_excp_no_table THEN DBMS_OUTPUT.PUT_LINE('La tabla no existe.');
  WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('Error: '||SQLERRM||' -> traza: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);  
END SP_LIMPIA_TABLA;