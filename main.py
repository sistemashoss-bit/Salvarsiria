from flask import Flask, request, jsonify
import gspread
import pandas as pd
import numpy as np
import re
import unicodedata
import traceback
import sys
import os
from google.auth import default
from supabase import create_client, Client
from datetime import datetime

app = Flask(__name__)

# ── Configuración de credenciales ─────────────────────────────────────
creds, _ = default()
gc = gspread.authorize(creds)

# ── Supabase ──────────────────────────────────────────────────────────
supabase_url = os.environ.get('SUPABASE_URL')
supabase_key = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(supabase_url, supabase_key)


# ── Funciones de Conversión ───────────────────────────────────────────
def convertir_tipos_para_supabase(df):
    """Convierte tipos de datos de pandas a tipos seguros para Supabase"""
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


# ── Funciones de Validación ───────────────────────────────────────────

def obtener_duplicados(df, tabla, columnas_clave, solo_primero=False):
    """
    Obtiene registros de Supabase que coinciden con los datos en el DataFrame
    
    Args:
        df: DataFrame con datos a buscar
        tabla: nombre de la tabla en Supabase
        columnas_clave: lista de columnas para identificar duplicados
        solo_primero: si True, detiene en el primer duplicado
    
    Returns:
        list de diccionarios con registros duplicados
    """
    try:
        if df.empty or not columnas_clave:
            return []
        
        duplicados_encontrados = []
        
        for idx, row in df.iterrows():
            query = supabase.table(tabla).select('*')
            
            for col in columnas_clave:
                if col not in row.index:
                    print(f"Advertencia: columna '{col}' no encontrada en DataFrame", file=sys.stderr)
                    continue
                
                valor = row[col]
                
                if pd.isna(valor):
                    continue
                
                query = query.eq(str(col), str(valor))
            
            try:
                resultado = query.execute()
                if resultado.data:
                    for dup in resultado.data:
                        dup['_df_index'] = idx
                    duplicados_encontrados.extend(resultado.data)
                    
                    if solo_primero:
                        return duplicados_encontrados
            
            except Exception as e:
                print(f"Error en búsqueda de duplicados fila {idx}: {str(e)}", file=sys.stderr)
                continue
        
        return duplicados_encontrados
    
    except Exception as e:
        print(f"Error al obtener duplicados: {str(e)}", file=sys.stderr)
        return []


def escribir_duplicados_en_sheets(spreadsheet_id, gid_duplicados, df_duplicados, tipo):
    """
    Escribe los duplicados en Google Sheets para revisión
    
    Args:
        spreadsheet_id: ID del spreadsheet
        gid_duplicados: GID de la hoja donde escribir
        df_duplicados: DataFrame con los duplicados
        tipo: "VENTAS" o "COMISIONES"
    
    Returns:
        dict con resultado
    """
    try:
        if df_duplicados.empty:
            print(f"No hay duplicados para escribir", file=sys.stderr)
            return {"status": "ok", "mensaje": "No hay duplicados"}
        
        sh = gc.open_by_key(spreadsheet_id)
        
        try:
            ws = sh.worksheet(None)
            for worksheet in sh.worksheets():
                if worksheet.id == int(gid_duplicados):
                    ws = worksheet
                    break
        except:
            ws = sh.add_worksheet(
                title=f"Duplicados_{tipo}_{datetime.now().strftime('%Y%m%d')}",
                rows=5000,
                cols=50
            )
        
        ws.clear()
        
        # Metadatos
        metadata = [
            [f"🔴 DUPLICADOS EN {tipo} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
            [f"Total encontrados: {len(df_duplicados)}"],
            [f"Acción requerida: REVISAR ANTES DE INSERTAR"],
            [],
            df_duplicados.columns.tolist()
        ]
        
        datos = metadata
        for _, row in df_duplicados.iterrows():
            fila = []
            for valor in row:
                if pd.isna(valor):
                    fila.append('')
                elif isinstance(valor, (pd.Timestamp, np.datetime64)):
                    fila.append(pd.Timestamp(valor).strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(valor, (int, float, np.integer, np.floating)) and not isinstance(valor, bool):
                    if isinstance(valor, float) and valor.is_integer():
                        fila.append(int(valor))
                    else:
                        fila.append(valor)
                else:
                    fila.append(str(valor))
            datos.append(fila)
        
        ws.update(datos, value_input_option='USER_ENTERED')
        
        print(f"✅ Escritos {len(df_duplicados)} duplicados de {tipo} en Sheets", file=sys.stderr)
        return {
            "status": "ok",
            "filas_escritas": len(df_duplicados),
            "mensaje": f"Duplicados escritos en Sheets para revisión"
        }
    
    except Exception as e:
        print(f"Error escribiendo en Sheets: {str(e)}", file=sys.stderr)
        return {
            "status": "error",
            "mensaje": f"Error al escribir en Sheets: {str(e)}"
        }


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
    return jsonify({"status": "ok", "servicio": "Supabase Sync v3"}), 200


@app.route('/validar', methods=['POST'])
def validar():
    """
    ENDPOINT 1: VALIDAR DATOS ANTES DE INSERTAR
    
    Request JSON:
    {
        "spreadsheet_base_id": "...",
        "gid_ventas": "1383532722",
        "gid_comisiones": "16410014",
        "gid_duplicados": "1455156763",
        "columnas_clave_ventas": ["folio", "sucursal", "fecha_venta"],
        "columnas_clave_comisiones": ["fecha_inicial", "fecha_final", "sucursal"]
    }
    """
    try:
        data = request.get_json()
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"ENDPOINT: /validar", file=sys.stderr)
        print(f"Payload: {data}", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)

        spreadsheet_id = data.get('spreadsheet_base_id')
        gid_ventas = data.get('gid_ventas')
        gid_comisiones = data.get('gid_comisiones')
        gid_duplicados = data.get('gid_duplicados')
        tabla_ventas = data.get('tabla_ventas', 'ventas')
        tabla_comisiones = data.get('tabla_comisiones', 'comisiones')
        columnas_clave_ventas = data.get('columnas_clave_ventas', ['folio', 'sucursal', 'fecha_venta'])
        columnas_clave_comisiones = data.get('columnas_clave_comisiones', ['fecha_inicial', 'fecha_final', 'sucursal'])

        if not all([spreadsheet_id, gid_ventas, gid_comisiones]):
            return jsonify({
                "status": "error",
                "mensaje": "spreadsheet_base_id, gid_ventas, gid_comisiones requeridos"
            }), 400

        # PASO 1: VALIDAR VENTAS
        print("[1/3] Procesando y validando VENTAS...", file=sys.stderr)
        df_ventas = procesar_ventas(spreadsheet_id, gid_ventas)
        
        duplicados_ventas = obtener_duplicados(df_ventas, tabla_ventas, columnas_clave_ventas, solo_primero=False)
        
        if duplicados_ventas:
            print(f"🔴 DUPLICADOS EN VENTAS: {len(duplicados_ventas)}", file=sys.stderr)
            
            if gid_duplicados:
                df_dups = pd.DataFrame(duplicados_ventas)
                escribir_duplicados_en_sheets(spreadsheet_id, gid_duplicados, df_dups, "VENTAS")
            
            return jsonify({
                "status": "validacion_fallida",
                "paso": "ventas",
                "mensaje": f"❌ Se encontraron {len(duplicados_ventas)} registros duplicados en VENTAS",
                "duplicados_encontrados": len(duplicados_ventas),
                "accion": f"📝 Revisar los duplicados en Google Sheets (GID: {gid_duplicados})",
                "detalles": "Los duplicados han sido escritos en la hoja de revisión. Elimínalos antes de reintentar."
            }), 200

        print("✅ Validación de VENTAS OK", file=sys.stderr)

        # PASO 2: VALIDAR COMISIONES
        print("[2/3] Procesando y validando COMISIONES...", file=sys.stderr)
        df_comisiones = procesar_comisiones(spreadsheet_id, gid_comisiones)
        
        duplicados_comisiones = obtener_duplicados(df_comisiones, tabla_comisiones, columnas_clave_comisiones, solo_primero=True)
        
        if duplicados_comisiones:
            print(f"⚠️  DUPLICADOS EN COMISIONES: {len(duplicados_comisiones)}", file=sys.stderr)
            
            return jsonify({
                "status": "validacion_parcial",
                "mensaje": f"⚠️  Las COMISIONES ya existen en la base de datos",
                "duplicados_comisiones": True,
                "accion": "Se encontró al menos 1 registro duplicado",
                "recomendacion": "Las comisiones ya están registradas. Si es necesario actualizar, bórralas primero.",
                "ventas": {
                    "status": "ok",
                    "duplicados": False,
                    "filas_procesadas": len(df_ventas)
                },
                "comisiones": {
                    "status": "duplicados",
                    "duplicados": True,
                    "filas_procesadas": len(df_comisiones)
                }
            }), 200

        print("✅ Validación de COMISIONES OK", file=sys.stderr)

        # PASO 3: TODO OK
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
    """
    ENDPOINT 2: INSERTAR DATOS EN SUPABASE
    
    Request JSON:
    {
        "spreadsheet_base_id": "...",
        "gid_ventas": "1383532722",
        "gid_comisiones": "16410014",
        "tabla_ventas": "ventas",
        "tabla_comisiones": "comisiones",
        "columnas_clave_ventas": ["folio", "sucursal", "fecha_venta"],
        "columnas_clave_comisiones": ["fecha_inicial", "fecha_final", "sucursal"]
    }
    """
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
        columnas_clave_ventas = data.get('columnas_clave_ventas', ['folio', 'sucursal', 'fecha_venta'])
        columnas_clave_comisiones = data.get('columnas_clave_comisiones', ['fecha_inicial', 'fecha_final', 'sucursal'])

        if not all([spreadsheet_id, gid_ventas, gid_comisiones]):
            return jsonify({
                "status": "error",
                "mensaje": "spreadsheet_base_id, gid_ventas, gid_comisiones requeridos"
            }), 400

        # VALIDACIÓN INTERNA
        print("[1/4] Validando VENTAS antes de insertar...", file=sys.stderr)
        df_ventas = procesar_ventas(spreadsheet_id, gid_ventas)
        
        duplicados_ventas = obtener_duplicados(df_ventas, tabla_ventas, columnas_clave_ventas, solo_primero=False)
        
        if duplicados_ventas:
            print(f"❌ Inserción cancelada: {len(duplicados_ventas)} duplicados", file=sys.stderr)
            return jsonify({
                "status": "insercion_cancelada",
                "mensaje": f"❌ No se puede insertar: hay {len(duplicados_ventas)} duplicados en VENTAS",
                "accion": "Ejecuta /validar para ver los duplicados y revisarlos en Sheets"
            }), 400

        print("✅ VENTAS validadas", file=sys.stderr)

        print("[2/4] Validando COMISIONES antes de insertar...", file=sys.stderr)
        df_comisiones = procesar_comisiones(spreadsheet_id, gid_comisiones)
        
        duplicados_comisiones = obtener_duplicados(df_comisiones, tabla_comisiones, columnas_clave_comisiones, solo_primero=True)
        
        if duplicados_comisiones:
            print(f"❌ Inserción cancelada: duplicados en comisiones", file=sys.stderr)
            return jsonify({
                "status": "insercion_cancelada",
                "mensaje": "❌ No se puede insertar: ya existen registros duplicados en COMISIONES",
                "accion": "Elimina los registros de comisiones existentes o ejecuta /validar"
            }), 400

        print("✅ COMISIONES validadas", file=sys.stderr)

        # INSERCIÓN
        print("[3/4] Insertando VENTAS...", file=sys.stderr)
        df_ventas_sql = convertir_tipos_para_supabase(df_ventas)
        datos_ventas = df_ventas_sql.to_dict(orient='records')
        
        batch_size = 1000
        filas_ventas = 0
        
        for i in range(0, len(datos_ventas), batch_size):
            lote = datos_ventas[i:i+batch_size]
            try:
                supabase.table(tabla_ventas).insert(lote).execute()
                filas_ventas += len(lote)
                print(f"  ✅ Insertadas {len(lote)} ventas (total: {filas_ventas})", file=sys.stderr)
            except Exception as e:
                print(f"❌ Error: {str(e)}", file=sys.stderr)
                return jsonify({
                    "status": "error",
                    "mensaje": f"Error al insertar ventas: {str(e)}",
                    "filas_ventas_insertadas": filas_ventas
                }), 500

        print("[4/4] Insertando COMISIONES...", file=sys.stderr)
        df_comisiones_sql = convertir_tipos_para_supabase(df_comisiones)
        datos_comisiones = df_comisiones_sql.to_dict(orient='records')
        
        filas_comisiones = 0
        
        for i in range(0, len(datos_comisiones), batch_size):
            lote = datos_comisiones[i:i+batch_size]
            try:
                supabase.table(tabla_comisiones).insert(lote).execute()
                filas_comisiones += len(lote)
                print(f"  ✅ Insertadas {len(lote)} comisiones (total: {filas_comisiones})", file=sys.stderr)
            except Exception as e:
                print(f"❌ Error: {str(e)}", file=sys.stderr)
                return jsonify({
                    "status": "error",
                    "mensaje": f"Error al insertar comisiones: {str(e)}",
                    "filas_ventas": filas_ventas,
                    "filas_comisiones": 0
                }), 500

        print("\n✅ INSERCIÓN COMPLETADA\n", file=sys.stderr)

        return jsonify({
            "status": "insercion_exitosa",
            "mensaje": "✅ Datos insertados en Supabase exitosamente",
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
    print(f"\n🚀 Supabase Sync v3 en puerto {port}\n", file=sys.stderr)
    app.run(host='0.0.0.0', port=port, debug=False)