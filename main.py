from flask import Flask, request, jsonify
import gspread
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import pool
import traceback
import sys
import os
from google.auth import default
from datetime import datetime

app = Flask(__name__)

# ── Configuración de credenciales ─────────────────────────────────────
creds, _ = default()
gc = gspread.authorize(creds)

# ── PostgreSQL Connection Pool (Transaction Pooler) ────────────────────
# Usar Transaction Pooler en lugar de conexión directa
# Ideal para serverless/Cloud Run

DATABASE_URL = os.environ['DATABASE_URL']  # postgresql://...@pooler.supabase.com:6543/...

try:
    # Crear pool de conexiones con Transaction Pooler
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=20,  # Máximo de conexiones en el pool
        dsn=DATABASE_URL,
        connect_timeout=5
    )
    print("✅ Pool de conexiones PostgreSQL creado (Transaction Pooler)", file=sys.stderr)
except Exception as e:
    print(f"❌ Error creando pool: {e}", file=sys.stderr)
    raise


def get_db_connection():
    """Obtiene una conexión del pool"""
    try:
        conn = connection_pool.getconn()
        conn.autocommit = False  # Transacciones explícitas
        return conn
    except Exception as e:
        print(f"❌ Error obteniendo conexión: {e}", file=sys.stderr)
        raise


def return_db_connection(conn):
    """Devuelve la conexión al pool"""
    try:
        if conn:
            connection_pool.putconn(conn)
    except Exception as e:
        print(f"❌ Error devolviendo conexión: {e}", file=sys.stderr)


# ── Funciones de Conversión ───────────────────────────────────────────
def convertir_tipos_para_postgresql(df):
    """Convierte tipos de datos de pandas a tipos seguros para PostgreSQL"""
    df_convertido = df.copy()
    
    for col in df_convertido.columns:
        df_convertido[col] = df_convertido[col].where(pd.notna(df_convertido[col]), None)
        
        if pd.api.types.is_datetime64_any_dtype(df_convertido[col]):
            df_convertido[col] = df_convertido[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        elif pd.api.types.is_numeric_dtype(df_convertido[col]):
            df_convertido[col] = df_convertido[col].apply(
                lambda x: int(x) if isinstance(x, (int, np.integer)) or (isinstance(x, float) and x.is_integer()) else float(x) if pd.notna(x) else None
            )
    
    return df_convertido


def insertar_datos_postgresql(tabla, datos, columnas):
    """Inserta datos en PostgreSQL usando Connection Pool"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Construir query INSERT
        placeholders = ','.join(['%s'] * len(columnas))
        cols_str = ','.join(columnas)
        query = f"INSERT INTO {tabla} ({cols_str}) VALUES ({placeholders})"
        
        # Insertar por lotes (1000 registros por transacción)
        filas_insertadas = 0
        batch_size = 1000
        
        for i in range(0, len(datos), batch_size):
            lote = datos[i:i+batch_size]
            
            try:
                # Convertir diccionarios a tuplas en el orden correcto
                valores_lote = [tuple(row[col] for col in columnas) for row in lote]
                cursor.executemany(query, valores_lote)
                conn.commit()
                filas_insertadas += len(lote)
                print(f"  ✅ Insertadas {len(lote)} filas (total: {filas_insertadas})", file=sys.stderr)
                
            except Exception as e:
                conn.rollback()
                print(f"❌ Error en lote {i}: {str(e)}", file=sys.stderr)
                raise
        
        cursor.close()
        return filas_insertadas
        
    except Exception as e:
        print(f"❌ Error al insertar en {tabla}: {str(e)}", file=sys.stderr)
        raise
    finally:
        if conn:
            return_db_connection(conn)


def obtener_duplicados_postgresql(tabla, columnas_clave, datos):
    """Obtiene registros duplicados de PostgreSQL"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        duplicados = []
        
        for row_idx, row in enumerate(datos):
            # Construir WHERE clause
            where_parts = []
            where_values = []
            
            for col in columnas_clave:
                valor = row.get(col)
                if valor is None:
                    where_parts.append(f"{col} IS NULL")
                else:
                    where_parts.append(f"{col} = %s")
                    where_values.append(valor)
            
            where_clause = ' AND '.join(where_parts)
            query = f"SELECT * FROM {tabla} WHERE {where_clause} LIMIT 1"
            
            try:
                cursor.execute(query, where_values)
                resultado = cursor.fetchone()
                
                if resultado:
                    duplicados.append({
                        '_db_row': resultado,
                        '_df_index': row_idx
                    })
                    
            except Exception as e:
                print(f"Error en búsqueda de duplicados fila {row_idx}: {str(e)}", file=sys.stderr)
                continue
        
        cursor.close()
        return duplicados
        
    except Exception as e:
        print(f"Error al obtener duplicados: {str(e)}", file=sys.stderr)
        return []
    finally:
        if conn:
            return_db_connection(conn)


# ── Funciones de Procesamiento ────────────────────────────────────────

def procesar_comisiones(spreadsheet_id, gid):
    """Procesa la hoja de comisiones según tu schema"""
    try:
        print(f"Descargando hoja de comisiones (gid={gid})...", file=sys.stderr)
        
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        raw = pd.read_csv(url, header=None)
        
        fecha_inicial = raw.iloc[2, 1]
        fecha_final = raw.iloc[4, 1]
        fecha_inicial = pd.to_datetime(fecha_inicial, format="%m/%d/%Y", errors="coerce")
        fecha_final = pd.to_datetime(fecha_final, format="%m/%d/%Y", errors="coerce")
        
        print(f"Fechas: {fecha_inicial} a {fecha_final}", file=sys.stderr)
        
        comisiones = pd.read_csv(url)
        
        comisiones = comisiones.iloc[1:, 5:]
        comisiones.columns = comisiones.iloc[0]
        comisiones = comisiones.iloc[1:].reset_index(drop=True)
        
        cols_eliminar = [1, 3, 7, 9, 14, 17, 21]
        calc_com = comisiones.drop(columns=comisiones.columns[cols_eliminar])
        calc_com.columns.name = None
        cols_num = calc_com.columns.drop("Sucursal")
        
        calc_com[cols_num] = (
            calc_com[cols_num]
            .astype(str)
            .replace(r"[^\d\.-]", "", regex=True)
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
        )
        
        # Renombrar columnas según el schema
        rename_map = {
            "Comision Vendedor": "comision_vendedor",
            "Comision Chapas": "comision_chapas",
            "Comision Instalaciones": "comision_instalaciones",
            "Comision Vendedor HC": "comision_vendedor_hc",
            "Comision Chapas HC": "comision_chapas_hc",
            "Total": "total",
            "Total Chapa": "total_chapa",
            "Total Puertas HC": "total_puertas_hc",
            "Total  C HC": "total_c_hc",
            "Instalaciones Vendedor": "instalaciones_vendedor",
            "Total Instalaciones": "total_instalaciones",
            "Puertas": "puertas",
            "Instalaciones": "instalaciones",
            "Chapas": "chapas",
            "Coordinador": "coordinador",
            "Elena": "elena",
            "Osvaldo": "osvaldo",
            "July": "july",
            "Sucursal": "sucursal"
        }
        
        calc_com = calc_com.rename(columns=rename_map)
        
        # Calcular porcentajes de comisión
        calc_com["comision_vendedor_puertas"] = calc_com.apply(
            lambda row: row["comision_vendedor"] / (row["total"] + row["total_puertas_hc"]) 
            if (row["total"] + row["total_puertas_hc"]) != 0 else 0,
            axis=1
        )
        calc_com["comision_vendedor_chapas"] = calc_com.apply(
            lambda row: row["comision_chapas"] / (row["total_chapa"] + row["total_c_hc"])
            if (row["total_chapa"] + row["total_c_hc"]) != 0 else 0,
            axis=1
        )
        calc_com["comision_vendedor_instalaciones"] = calc_com.apply(
            lambda row: row["comision_instalaciones"] / row["instalaciones_vendedor"]
            if row["instalaciones_vendedor"] != 0 else 0,
            axis=1
        )
        
        calc_com["comision_supervisor_puertas"] = calc_com.apply(
            lambda row: row["puertas"] / (row["total"] + row["total_puertas_hc"])
            if (row["total"] + row["total_puertas_hc"]) != 0 else 0,
            axis=1
        )
        calc_com["comision_supervisor_chapas"] = calc_com.apply(
            lambda row: row["chapas"] / (row["total_chapa"] + row["total_c_hc"])
            if (row["total_chapa"] + row["total_c_hc"]) != 0 else 0,
            axis=1
        )
        calc_com["comision_supervisor_instalaciones"] = calc_com.apply(
            lambda row: row["instalaciones"] / row["instalaciones_vendedor"]
            if row["instalaciones_vendedor"] != 0 else 0,
            axis=1
        )
        
        # Seleccionar columnas según schema
        columnas_finales = [
            "sucursal",
            "total",
            "total_chapa",
            "instalaciones_vendedor",
            "total_instalaciones",
            "total_puertas_hc",
            "total_c_hc",
            "comision_vendedor",
            "comision_chapas",
            "comision_instalaciones",
            "comision_vendedor_hc",
            "comision_chapas_hc",
            "puertas",
            "instalaciones",
            "chapas",
            "coordinador",
            "elena",
            "osvaldo",
            "july",
            "comision_vendedor_puertas",
            "comision_vendedor_chapas",
            "comision_vendedor_instalaciones",
            "comision_supervisor_puertas",
            "comision_supervisor_chapas",
            "comision_supervisor_instalaciones"
        ]
        
        calc_com = calc_com[columnas_finales]
        
        calc_com.replace([np.inf, -np.inf], 0, inplace=True)
        calc_com.fillna(0, inplace=True)
        
        # Redondear porcentajes
        calc_com["comision_vendedor_puertas"] = calc_com["comision_vendedor_puertas"].round(4)
        calc_com["comision_vendedor_chapas"] = calc_com["comision_vendedor_chapas"].round(4)
        calc_com["comision_vendedor_instalaciones"] = calc_com["comision_vendedor_instalaciones"].round(4)
        calc_com["comision_supervisor_puertas"] = calc_com["comision_supervisor_puertas"].round(4)
        calc_com["comision_supervisor_chapas"] = calc_com["comision_supervisor_chapas"].round(4)
        calc_com["comision_supervisor_instalaciones"] = calc_com["comision_supervisor_instalaciones"].round(4)
        
        # Agregar fechas
        calc_com.insert(0, "fecha_inicial", fecha_inicial)
        calc_com.insert(1, "fecha_final", fecha_final)
        
        print(f"Comisiones procesadas: {len(calc_com)} filas", file=sys.stderr)
        return calc_com
    
    except Exception as e:
        print(f"Error procesando comisiones: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise


def procesar_ventas(spreadsheet_id, gid):
    """Procesa la hoja de ventas"""
    try:
        print(f"Descargando hoja de ventas (gid={gid})...", file=sys.stderr)
        
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        df = pd.read_csv(url, dtype=str)
        df = df.drop_duplicates()
        
        print(f"Filas iniciales: {len(df)}", file=sys.stderr)
        
        # Convertir tipos de datos según schema
        df['folio'] = pd.to_numeric(df['folio'], errors='coerce').astype('Int64')
        df['unidades_vendidas'] = pd.to_numeric(df['unidades_vendidas'], errors='coerce').astype('Int64')
        df['total'] = pd.to_numeric(df['total'], errors='coerce')
        df['pago_recibido'] = pd.to_numeric(df['pago_recibido'], errors='coerce')
        df['fecha_venta'] = pd.to_datetime(df['fecha_venta'], errors='coerce')
        
        print(f"Ventas procesadas: {len(df)} filas", file=sys.stderr)
        return df
    
    except Exception as e:
        print(f"Error procesando ventas: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise


# ── ENDPOINTS ─────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de salud"""
    return jsonify({"status": "ok", "servicio": "Sync PostgreSQL v2 (Transaction Pooler)"}), 200


@app.route('/validar', methods=['POST'])
def validar():
    """Validar datos antes de insertar"""
    try:
        data = request.get_json()
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"ENDPOINT: /validar", file=sys.stderr)
        print(f"Payload: {data}", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)

        spreadsheet_id = data.get('spreadsheet_base_id')
        gid_ventas = data.get('gid_ventas')
        gid_comisiones = data.get('gid_comisiones')
        tabla_ventas = data.get('tabla_ventas', 'ventas')
        tabla_comisiones = data.get('tabla_comisiones', 'comisiones')
        columnas_clave_ventas = data.get('columnas_clave_ventas', ['folio', 'sucursal', 'fecha_venta'])
        columnas_clave_comisiones = data.get('columnas_clave_comisiones', ['fecha_inicial', 'fecha_final', 'sucursal'])

        if not all([spreadsheet_id, gid_ventas, gid_comisiones]):
            return jsonify({
                "status": "error",
                "mensaje": "spreadsheet_base_id, gid_ventas, gid_comisiones requeridos"
            }), 400

        # VALIDACIÓN VENTAS
        print("[1/3] Procesando y validando VENTAS...", file=sys.stderr)
        df_ventas = procesar_ventas(spreadsheet_id, gid_ventas)
        datos_ventas = df_ventas.to_dict(orient='records')
        
        duplicados_ventas = obtener_duplicados_postgresql(tabla_ventas, columnas_clave_ventas, datos_ventas)
        
        if duplicados_ventas:
            print(f"🔴 DUPLICADOS EN VENTAS: {len(duplicados_ventas)}", file=sys.stderr)
            return jsonify({
                "status": "validacion_fallida",
                "paso": "ventas",
                "mensaje": f"❌ Se encontraron {len(duplicados_ventas)} registros duplicados en VENTAS",
                "duplicados_encontrados": len(duplicados_ventas)
            }), 200

        print("✅ Validación de VENTAS OK", file=sys.stderr)

        # VALIDACIÓN COMISIONES
        print("[2/3] Procesando y validando COMISIONES...", file=sys.stderr)
        df_comisiones = procesar_comisiones(spreadsheet_id, gid_comisiones)
        datos_comisiones = df_comisiones.to_dict(orient='records')
        
        duplicados_comisiones = obtener_duplicados_postgresql(tabla_comisiones, columnas_clave_comisiones, datos_comisiones[:1])
        
        if duplicados_comisiones:
            print(f"⚠️  DUPLICADOS EN COMISIONES: {len(duplicados_comisiones)}", file=sys.stderr)
            return jsonify({
                "status": "validacion_parcial",
                "mensaje": "⚠️  Las COMISIONES ya existen en la base de datos",
                "duplicados_comisiones": True
            }), 200

        print("✅ Validación de COMISIONES OK", file=sys.stderr)

        # TODO OK
        print("[3/3] ✅ VALIDACIÓN COMPLETA OK", file=sys.stderr)

        return jsonify({
            "status": "validacion_exitosa",
            "mensaje": "✅ Validación completada exitosamente",
            "accion": "Procede a llamar /subirdatos para insertar",
            "ventas": {
                "status": "ok",
                "filas_procesadas": len(df_ventas),
                "duplicados": False
            },
            "comisiones": {
                "status": "ok",
                "filas_procesadas": len(df_comisiones),
                "duplicados": False
            }
        }), 200

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}\n", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify({"status": "error", "mensaje": str(e)}), 500


@app.route('/subirdatos', methods=['POST'])
def subirdatos():
    """Insertar datos en PostgreSQL usando Transaction Pooler"""
    try:
        data = request.get_json()
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"ENDPOINT: /subirdatos", file=sys.stderr)
        print(f"Payload: {data}", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)

        spreadsheet_id = data.get('spreadsheet_base_id')
        gid_ventas = data.get('gid_ventas')
        gid_comisiones = data.get('gid_comisiones')
        tabla_ventas = data.get('tabla_ventas', 'ventas')
        tabla_comisiones = data.get('tabla_comisiones', 'comisiones')

        if not all([spreadsheet_id, gid_ventas, gid_comisiones]):
            return jsonify({
                "status": "error",
                "mensaje": "spreadsheet_base_id, gid_ventas, gid_comisiones requeridos"
            }), 400

        # PROCESAMIENTO
        print("[1/4] Procesando VENTAS...", file=sys.stderr)
        df_ventas = procesar_ventas(spreadsheet_id, gid_ventas)
        df_ventas = convertir_tipos_para_postgresql(df_ventas)
        datos_ventas = df_ventas.to_dict(orient='records')
        
        columnas_ventas = list(df_ventas.columns)
        
        print("[2/4] Insertando VENTAS...", file=sys.stderr)
        filas_ventas = insertar_datos_postgresql(tabla_ventas, datos_ventas, columnas_ventas)

        print("[3/4] Procesando COMISIONES...", file=sys.stderr)
        df_comisiones = procesar_comisiones(spreadsheet_id, gid_comisiones)
        df_comisiones = convertir_tipos_para_postgresql(df_comisiones)
        datos_comisiones = df_comisiones.to_dict(orient='records')
        
        columnas_comisiones = list(df_comisiones.columns)
        
        print("[4/4] Insertando COMISIONES...", file=sys.stderr)
        filas_comisiones = insertar_datos_postgresql(tabla_comisiones, datos_comisiones, columnas_comisiones)

        print("\n✅ INSERCIÓN COMPLETADA\n", file=sys.stderr)

        return jsonify({
            "status": "insercion_exitosa",
            "mensaje": "✅ Datos insertados en PostgreSQL exitosamente",
            "ventas": {
                "tabla": tabla_ventas,
                "filas_insertadas": filas_ventas,
                "status": "ok"
            },
            "comisiones": {
                "tabla": tabla_comisiones,
                "filas_insertadas": filas_comisiones,
                "status": "ok"
            }
        }), 200

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}\n", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify({"status": "error", "mensaje": str(e)}), 500


if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8080))
    print(f"\n🚀 PostgreSQL Sync v2 (Transaction Pooler) en puerto {port}\n", file=sys.stderr)
    app.run(host='0.0.0.0', port=port, debug=False)