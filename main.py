import streamlit as st
import pandas as pd
import sqlite3
import hashlib
from datetime import datetime

# --- CONFIGURACIÓN DE BASE DE DATOS ---
def init_db():
    conn = sqlite3.connect('condominio.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS pagos 
                 (id INTEGER PRIMARY KEY, casa TEXT, fecha TEXT, monto REAL, 
                  provision REAL, decimos REAL, sueldo REAL, operativo REAL, usuario TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS usuarios (user TEXT PRIMARY KEY, pw TEXT, rol TEXT)''')
    
    # Usuario por defecto: admin / clave: admin123
    pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
    c.execute("INSERT OR IGNORE INTO usuarios VALUES ('admin', ?, 'ADMINISTRADOR')", (pw_hash,))
    conn.commit()
    conn.close()

# --- LÓGICA DE DISTRIBUCIÓN (Basada en tus tablas) ---
def calcular_fondos(monto):
    # Basado en tus archivos: Provisión (~3.4%), Décimos (~4.5%), Sueldo (~20%)
    prov = round(monto * 0.0338, 2)
    deci = round(monto * 0.045, 2)
    suel = round(monto * 0.20, 2)
    oper = round(monto - prov - deci - suel, 2)
    return prov, deci, suel, oper

# --- INTERFAZ ---
st.set_page_config(page_title="Condominio 2025", layout="wide")
init_db()

st.title("🏢 Gestión Condominio NANTU")

# Login simple para el ejemplo
if 'conectado' not in st.session_state:
    st.session_state.conectado = False

if not st.session_state.conectado:
    user = st.sidebar.text_input("Usuario")
    pw = st.sidebar.text_input("Contraseña", type="password")
    if st.sidebar.button("Entrar"):
        if user == "admin" and pw == "admin123":
            st.session_state.conectado = True
            st.rerun()
        else:
            st.sidebar.error("Error de acceso")
else:
    menu = st.sidebar.radio("Menú", ["Dashboard", "Cargar Pago"])
    
    if menu == "Cargar Pago":
        st.subheader("📝 Registrar nuevo ingreso")
        casa = st.selectbox("Seleccione Casa", [f"Casa {i:02d}" for i in range(1,11)])
        monto = st.number_input("Monto Recibido ($)", min_value=0.0)
        fecha = st.date_input("Fecha del depósito")
        
        if st.button("Guardar Registro"):
            p, d, s, o = calcular_fondos(monto)
            conn = sqlite3.connect('condominio.db')
            conn.execute("INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo) VALUES (?,?,?,?,?,?,?)",
                         (casa, str(fecha), monto, p, d, s, o))
            conn.commit()
            conn.close()
            st.success(f"¡Guardado! Provisión: ${p} | Décimos: ${d}")

    elif menu == "Dashboard":
        st.subheader("📊 Movimientos Acumulados")
        conn = sqlite3.connect('condominio.db')
        df = pd.read_sql_query("SELECT * FROM pagos", conn)
        if not df.empty:
            st.dataframe(df)
            st.metric("Total en Provisión", f"${df['provision'].sum():,.2f}")
        else:
            st.write("Aún no hay datos.")
          
