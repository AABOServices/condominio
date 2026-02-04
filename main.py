import streamlit as st
import pandas as pd
import sqlite3
import hashlib
from datetime import datetime
import os

DB_PATH = "condominio.db"

# --- CONFIGURACIÓN DE BASE DE DATOS ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Tabla de pagos
    c.execute(
        """CREATE TABLE IF NOT EXISTS pagos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            casa TEXT,
            fecha TEXT,
            monto REAL,
            provision REAL,
            decimos REAL,
            sueldo REAL,
            operativo REAL,
            usuario TEXT
        )"""
    )

    # Tabla de usuarios
    c.execute(
        """CREATE TABLE IF NOT EXISTS usuarios (
            user TEXT PRIMARY KEY,
            pw TEXT,
            rol TEXT
        )"""
    )

    # Usuario por defecto: admin / clave: admin123
    pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
    c.execute("INSERT OR IGNORE INTO usuarios VALUES ('admin', ?, 'ADMINISTRADOR')", (pw_hash,))

    conn.commit()
    conn.close()


def validar_login(user: str, pw: str) -> bool:
    pw_hash = hashlib.sha256(pw.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT rol FROM usuarios WHERE user=? AND pw=?", (user, pw_hash)).fetchone()
    conn.close()
    if row:
        st.session_state.rol = row[0]
        return True
    return False


# --- LÓGICA DE DISTRIBUCIÓN (Basada en tus tablas) ---
def calcular_fondos(monto: float):
    # Provisión (~3.4%), Décimos (~4.5%), Sueldo (~20%)
    prov = round(monto * 0.0338, 2)
    deci = round(monto * 0.045, 2)
    suel = round(monto * 0.20, 2)
    oper = round(monto - prov - deci - suel, 2)
    return prov, deci, suel, oper


def guardar_pago(casa: str, fecha: str, monto: float, usuario: str):
    p, d, s, o = calcular_fondos(monto)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo, usuario) VALUES (?,?,?,?,?,?,?,?)",
        (casa, fecha, monto, p, d, s, o, usuario),
    )
    conn.commit()
    conn.close()
    return p, d, s, o


def cargar_df_pagos():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM pagos", conn)
    conn.close()
    return df


# --- INTERFAZ ---
st.set_page_config(page_title="Condominio 2025", layout="wide")
init_db()

st.title("🏢 CONDOMINIOS NANTU")

# Session state
if "conectado" not in st.session_state:
    st.session_state.conectado = False
if "user" not in st.session_state:
    st.session_state.user = ""
if "rol" not in st.session_state:
    st.session_state.rol = ""


# --- LOGIN ---
if not st.session_state.conectado:
    st.sidebar.subheader("🔐 Acceso")
    user = st.sidebar.text_input("Usuario")
    pw = st.sidebar.text_input("Contraseña", type="password")

    if st.sidebar.button("Entrar"):
        if validar_login(user, pw):
            st.session_state.conectado = True
            st.session_state.user = user
            st.rerun()
        else:
            st.sidebar.error("Error de acceso (usuario/clave incorrectos)")

    st.info("Ingresa con tus credenciales para gestionar pagos y ver el dashboard.")
    st.stop()


# --- APP PRINCIPAL ---
st.sidebar.success(f"Conectado como: {st.session_state.user}")
st.sidebar.write(f"Rol: **{st.session_state.rol}**")

menu = st.sidebar.radio("Menú", ["Dashboard", "Cargar Pago", "Administrador"])

# ---- Administrador (Carga histórico)
if menu == "Administrador":
    st.subheader("🛠 Administración del Sistema")

    st.warning(
        "Este botón ejecuta una carga masiva. Úsalo UNA sola vez para evitar duplicados. "
        "Si quieres, luego te implemento un control anti-duplicados."
    )

    if st.sidebar.button("🚀 Ejecutar Carga Histórica"):
        try:
            # Ejecuta el script externo (debe existir en el repo)
            exit_code = os.system("python importar_datos.py")
            if exit_code == 0:
                st.success("Carga histórica ejecutada correctamente. Revisa Dashboard.")
            else:
                st.error(f"La carga histórica terminó con código {exit_code}. Revisa logs.")
        except Exception as e:
            st.error(f"Error al ejecutar carga: {e}")

    st.divider()
    st.subheader("📌 Validación rápida")
    df = cargar_df_pagos()
    st.write(f"Registros en base: **{len(df)}**")
    if not df.empty:
        st.dataframe(df.tail(20), use_container_width=True)

# ---- Cargar Pago
elif menu == "Cargar Pago":
    st.subheader("📝 Registrar nuevo ingreso")

    casa = st.selectbox("Seleccione Casa", [f"Casa {i:02d}" for i in range(1, 11)])
    monto = st.number_input("Monto Recibido ($)", min_value=0.0, step=0.01)
    fecha = st.date_input("Fecha del depósito")

    if st.button("Guardar Registro"):
        p, d, s, o = guardar_pago(casa, str(fecha), float(monto), st.session_state.user)
        st.success(f"¡Guardado! Provisión: ${p} | Décimos: ${d} | Sueldo: ${s} | Operativo: ${o}")

# ---- Dashboard
elif menu == "Dashboard":
    st.subheader("📊 Movimientos Acumulados")

    df = cargar_df_pagos()

    if df.empty:
        st.info("Aún no hay datos.")
    else:
        st.dataframe(df, use_container_width=True)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Recaudado", f"${df['monto'].sum():,.2f}")
        col2.metric("Total Provisión", f"${df['provision'].sum():,.2f}")
        col3.metric("Total Décimos", f"${df['decimos'].sum():,.2f}")
        col4.metric("Total Operativo", f"${df['operativo'].sum():,.2f}")

        st.divider()
        st.subheader("Ingresos por Casa")
        por_casa = df.groupby("casa")["monto"].sum().sort_index()
        st.bar_chart(por_casa)

# ---- Logout
st.sidebar.divider()
if st.sidebar.button("Cerrar sesión"):
    st.session_state.conectado = False
    st.session_state.user = ""
    st.session_state.rol = ""
    st.rerun()
