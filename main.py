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

# ── PostgreSQL Lazy Connection Pool ────────────────────
DATABASE_URL = os.environ['DATABASE_URL']
connection_pool = None  # ✅ Lazy initialization

def init_connection_pool():
    """Inicializa el pool LAZY (solo cuando se necesita)"""
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=20,
                dsn=DATABASE_URL,
                connect_timeout=5
            )
            print("✅ Pool de conexiones PostgreSQL creado (Lazy Transaction Pooler)", file=sys.stderr)
        except Exception as e:
            print(f"❌ Error creando pool: {e}", file=sys.stderr)
            # No raise aquí - permite que la app arranque
            connection_pool = None

def get_db_connection():
    """Obtiene una conexión del pool, inicializándolo si es necesario"""
    try:
        if connection_pool is None:
            init_connection_pool()
        if connection_pool is None:
            raise Exception("No se pudo inicializar el pool de conexiones")
            
        conn = connection_pool.getconn()
        conn.autocommit = False
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
                        '_df_index': row_idx,
                        '_row_data': row  # Agregar los datos originales
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


def escribir_duplicados_en_sheets(spreadsheet_id, gid_duplicados, duplicados_ventas, df_ventas):
    """Escribe los duplicados encontrados en una hoja de Google Sheets"""
    try:
        if not duplicados_ventas:
            print(f"No hay duplicados para escribir", file=sys.stderr)
            return True
        
        print(f"Escribiendo {len(duplicados_ventas)} duplicados en Google Sheets...", file=sys.stderr)
        
        # Abre el spreadsheet
        sheet = gc.open_by_key(spreadsheet_id)
        
        # Obtén o crea la hoja de duplicados
        worksheet = None
        try:
            worksheet = sheet.worksheet("Duplicados")
            print(f"✅ Hoja 'Duplicados' encontrada", file=sys.stderr)
            # Limpiar hoja anterior
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            print(f"Creando hoja 'Duplicados'...", file=sys.stderr)
            worksheet = sheet.add_worksheet(title="Duplicados", rows=1000, cols=20)
        
        # Preparar encabezados
        headers = list(df_ventas.columns)
        
        # Preparar datos para escribir
        rows_to_write = [headers]  # Primera fila con headers
        
        for dup in duplicados_ventas:
            row_data = dup['_row_data']
            row = [row_data.get(col, '') for col in headers]
            rows_to_write.append(row)
        
        # Escribir datos en la hoja
        worksheet.update('A1', rows_to_write)
        
        print(f"✅ {len(duplicados_ventas)} registros duplicados escritos en la hoja 'Duplicados'", file=sys.stderr)
        return True
        
    except Exception as e:
        print(f"Error escribiendo duplicados en Sheets: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return False


# ── Funciones de Procesamiento ────────────────────────────────────────

def procesar_comisiones(spreadsheet_id, gid):
    """Procesa la hoja de comisiones - Exactamente como tu código original con CSV"""
    try:
        print(f"Descargando comisiones (gid={gid})...", file=sys.stderr)
        
        # Usar la misma URL que en tu código original
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        
        ventas = pd.read_csv(url)
        comisiones = pd.read_csv(url)
        raw = pd.read_csv(url, header=None)
        
        fecha_inicial = raw.iloc[2, 1]
        fecha_final = raw.iloc[4, 1]
        fecha_inicial = pd.to_datetime(fecha_inicial, format="%m/%d/%Y", errors="coerce")
        fecha_final = pd.to_datetime(fecha_final, format="%m/%d/%Y", errors="coerce")
        
        print(f"Fechas: {fecha_inicial} a {fecha_final}", file=sys.stderr)
        
        # Corte Limpio (EXACTAMENTE como tu código)
        comisiones = comisiones.iloc[1:, 5:]
        comisiones.columns = comisiones.iloc[0]
        comisiones = comisiones.iloc[1:]
        comisiones = comisiones.reset_index(drop=True)
        
        cols_eliminar = [1, 3, 7, 9, 14, 17, 21]
        calc_com = comisiones.drop(columns=comisiones.columns[cols_eliminar])
        calc_com.columns.name = None
        cols_num = calc_com.columns.drop("Sucursal")
        
        # Convertir esas columnas a número
        calc_com[cols_num] = (
            calc_com[cols_num]
            .astype(str)
            .replace(r"[^\d\.-]", "", regex=True)
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
        )
        
        comisiones = calc_com.copy()
        comisiones.insert(0, "Fecha Inicial", fecha_inicial)
        comisiones.insert(1, "Fecha Final", fecha_final)
        
        # Calcular porcentajes
        calc_com["Comision Vendedor Puertas"] = calc_com["Comision Vendedor"] / (calc_com["Total"] + calc_com["Total Puertas HC"])
        calc_com["Comision Vendedor Chapas"] = calc_com["Comision Chapas"] / (calc_com["Total Chapa"] + calc_com["Total  C HC"])
        calc_com["Comision Vendedor Instalaciones"] = calc_com["Comision Instalaciones"] / calc_com["Instalaciones Vendedor"]
        calc_com["Comision Vendedor HC"] = calc_com["Comision Vendedor HC"] / calc_com["Total Puertas HC"]
        calc_com["Comision Chapas HC"] = calc_com["Comision Chapas HC"] / calc_com["Total  C HC"]
        calc_com["Comision Supervisor Puertas"] = calc_com["Puertas"] / (calc_com["Total"] + calc_com["Total Puertas HC"])
        calc_com["Comision Supervisor Chapas"] = calc_com["Chapas"] / (calc_com["Total Chapa"] + calc_com["Total  C HC"])
        calc_com["Comision Supervisor Instalaciones"] = calc_com["Instalaciones"] / calc_com["Instalaciones Vendedor"]
        calc_com["Comision Coordinador"] = calc_com["Coordinador"] / (calc_com["Total"] + calc_com["Total Puertas HC"])
        calc_com["Comision Elena"] = calc_com["Elena"] / (calc_com["Total"] + calc_com["Total Puertas HC"])
        calc_com["Comision Osvaldo"] = calc_com["Osvaldo"] / (calc_com["Total"] + calc_com["Total Puertas HC"])
        calc_com["Comision July"] = calc_com["July"] / (calc_com["Total"] + calc_com["Total Puertas HC"])
        
        calc_com = calc_com[[
            "Sucursal",
            "Comision Vendedor Puertas",
            "Comision Vendedor Chapas",
            "Comision Vendedor Instalaciones",
            "Comision Vendedor HC",
            "Comision Chapas HC",
            "Comision Supervisor Puertas",
            "Comision Supervisor Chapas",
            "Comision Supervisor Instalaciones",
            "Comision Coordinador",
            "Comision Elena",
            "Comision Osvaldo",
            "Comision July",
        ]]
        
        # Limpiar NaN e infinitos
        calc_com.replace([np.inf, -np.inf], 0, inplace=True)
        calc_com.fillna(0, inplace=True)
        
        # Redondear a 4 decimales
        cols = calc_com.columns.drop("Sucursal")
        calc_com[cols] = calc_com[cols].round(4)
        
        # Agregar fechas al inicio
        calc_com.insert(0, "Fecha Inicial", fecha_inicial)
        calc_com.insert(1, "Fecha Final", fecha_final)
        
        # Renombrar para PostgreSQL
        rename_map = {
            "Fecha Inicial": "fecha_inicial",
            "Fecha Final": "fecha_final",
            "Sucursal": "sucursal",
            "Comision Vendedor Puertas": "comision_vendedor_puertas",
            "Comision Vendedor Chapas": "comision_vendedor_chapas",
            "Comision Vendedor Instalaciones": "comision_vendedor_instalaciones",
            "Comision Vendedor HC": "comision_vendedor_hc",
            "Comision Chapas HC": "comision_chapas_hc",
            "Comision Supervisor Puertas": "comision_supervisor_puertas",
            "Comision Supervisor Chapas": "comision_supervisor_chapas",
            "Comision Supervisor Instalaciones": "comision_supervisor_instalaciones",
            "Comision Coordinador": "comision_coordinador",
            "Comision Elena": "comision_elena",
            "Comision Osvaldo": "comision_osvaldo",
            "Comision July": "comision_july",
        }
        
        calc_com = calc_com.rename(columns=rename_map)
        
        print(f"Comisiones procesadas: {len(calc_com)} filas", file=sys.stderr)
        return calc_com
    
    except Exception as e:
        print(f"Error procesando comisiones: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise


def procesar_ventas(spreadsheet_id, gid):
    """Procesa la hoja de ventas - Usando CSV como tu código original"""
    try:
        print(f"Descargando ventas (gid={gid})...", file=sys.stderr)
        
        # Usar la misma URL que en tu código original
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
        
        df = pd.read_csv(url)
        df = df.drop_duplicates()
        
        print(f"Filas iniciales: {len(df)}", file=sys.stderr)
        print(f"📊 Columnas encontradas: {list(df.columns)}", file=sys.stderr)
        
        # Limpiar espacios en blanco de nombres de columnas
        df.columns = df.columns.str.strip()
        
        # Mapear nombres de columnas de Google Sheets a nombres estándar
        rename_map = {
            'Folio': 'folio',
            'Fecha de venta': 'fecha_venta',
            'Cliente': 'cliente',
            'Sucursal': 'sucursal',
            'Metodo de Venta': 'metodo_venta',
            'Tipo de Pago': 'tipo_pago',
            'Unidades Vendidas': 'unidades_vendidas',
            'NotaVenta': 'notaventa',
            'Total': 'total',
            'Pago Recibido': 'pago_recibido',
            'Método de Pago': 'metodo_pago',
            'Cuenta de Depósito': 'cuenta_deposito',
            'Confirmacion Pago': 'confirmacion_pago',
            'Articulo': 'articulo',
            'Se Paga': 'se_paga'
        }
        
        df = df.rename(columns=rename_map)
        print(f"📊 Columnas (después rename): {list(df.columns)}", file=sys.stderr)
        
        # Convertir tipos de datos según schema
        if 'folio' in df.columns:
            df['folio'] = pd.to_numeric(df['folio'], errors='coerce').astype('Int64')
        
        if 'unidades_vendidas' in df.columns:
            df['unidades_vendidas'] = pd.to_numeric(df['unidades_vendidas'], errors='coerce').astype('Int64')
        
        if 'total' in df.columns:
            df['total'] = pd.to_numeric(df['total'], errors='coerce')
        
        if 'pago_recibido' in df.columns:
            df['pago_recibido'] = pd.to_numeric(df['pago_recibido'], errors='coerce')
        
        if 'fecha_venta' in df.columns:
            df['fecha_venta'] = pd.to_datetime(df['fecha_venta'], errors='coerce')
        
         # ── Desaplanar: agrupar registros duplicados ──
        cols_grupo = ['folio', 'fecha_venta', 'cliente', 'sucursal', 'metodo_venta', 
                      'tipo_pago', 'articulo', 'notaventa', 'metodo_pago', 
                      'cuenta_deposito', 'confirmacion_pago', 'se_paga']
        cols_grupo = [c for c in cols_grupo if c in df.columns]
        
        cols_sum = ['unidades_vendidas', 'total', 'pago_recibido']
        cols_sum = [c for c in cols_sum if c in df.columns]
        
        df = df.groupby(cols_grupo, as_index=False, dropna=False).agg(
            {col: 'sum' for col in cols_sum}
        )
        print(f"Ventas después de agrupar: {len(df)} filas", file=sys.stderr)
        
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
    return jsonify({"status": "ok", "servicio": "Sync PostgreSQL v2 (Transaction Pooler + gspread API)"}), 200


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
        gid_duplicados = data.get('gid_duplicados', '1455156763')  # GID de la hoja Duplicados
        tabla_ventas = data.get('tabla_ventas', 'ventas')
        tabla_comisiones = data.get('tabla_comisiones', 'comisiones')
        columnas_clave_ventas = data.get('columnas_clave_ventas', ['folio', 'sucursal', 'fecha_venta', 'articulo', 'notaventa'])
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
            # Escribir duplicados en Google Sheets
            escribir_duplicados_en_sheets(spreadsheet_id, gid_duplicados, duplicados_ventas, df_ventas)
            
            return jsonify({
                "status": "validacion_fallida",
                "paso": "ventas",
                "mensaje": f"❌ Se encontraron {len(duplicados_ventas)} registros duplicados en VENTAS",
                "duplicados_encontrados": len(duplicados_ventas),
                "accion": "Duplicados escritos en la hoja 'Duplicados'"
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
        print("[1/5] Procesando VENTAS...", file=sys.stderr)
        df_ventas = procesar_ventas(spreadsheet_id, gid_ventas)
        
        print("[2/5] Procesando COMISIONES...", file=sys.stderr)
        df_comisiones = procesar_comisiones(spreadsheet_id, gid_comisiones)
        
        # JOINEAR VENTAS CON COMISIONES
        print("[3/5] Joinando VENTAS con COMISIONES...", file=sys.stderr)
        
        # Seleccionar solo las columnas de comisiones (sin fecha_inicial, fecha_final)
        cols_comisiones = [col for col in df_comisiones.columns if col.startswith('comision_') or col == 'sucursal']
        df_comisiones_join = df_comisiones[cols_comisiones]
        
        # Merge left join
        df_ventas = df_ventas.merge(
            df_comisiones_join,
            on='sucursal',
            how='left'
        )
        
        print(f"✅ Join completado. Columnas: {len(df_ventas.columns)}, Filas: {len(df_ventas)}", file=sys.stderr)
        
        # Convertir tipos para PostgreSQL
        df_ventas = convertir_tipos_para_postgresql(df_ventas)
        datos_ventas = df_ventas.to_dict(orient='records')
        
        columnas_ventas = list(df_ventas.columns)
        
        print("[4/5] Insertando VENTAS...", file=sys.stderr)
        filas_ventas = insertar_datos_postgresql(tabla_ventas, datos_ventas, columnas_ventas)

        # Convertir comisiones para PostgreSQL
        df_comisiones = convertir_tipos_para_postgresql(df_comisiones)
        datos_comisiones = df_comisiones.to_dict(orient='records')
        
        columnas_comisiones = list(df_comisiones.columns)
        
        print("[5/5] Insertando COMISIONES...", file=sys.stderr)
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
    print(f"\n🚀 PostgreSQL Sync v2 (Transaction Pooler + gspread API) en puerto {port}\n", file=sys.stderr)
    app.run(host='0.0.0.0', port=port, debug=False)