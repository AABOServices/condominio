import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import os

DB_PATH = "condominio.db"

# ------------------ DB ------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

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

    c.execute(
        """CREATE TABLE IF NOT EXISTS usuarios (
            user TEXT PRIMARY KEY,
            pw TEXT,
            rol TEXT
        )"""
    )

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


def calcular_fondos(monto: float):
    prov = round(monto * 0.0338, 2)
    deci = round(monto * 0.045, 2)
    suel = round(monto * 0.20, 2)
    oper = round(monto - prov - deci - suel, 2)
    return prov, deci, suel, oper


def guardar_pago(casa: str, fecha: str, monto: float, usuario: str):
    p, d, s, o = calcular_fondos(monto)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo, usuario)
           VALUES (?,?,?,?,?,?,?,?)""",
        (casa, fecha, monto, p, d, s, o, usuario),
    )
    conn.commit()
    conn.close()
    return p, d, s, o


def cargar_df_pagos():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM pagos", conn)
    conn.close()

    if not df.empty:
        # Normaliza fecha para filtros
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    return df


# ------------------ UI ------------------
st.set_page_config(page_title="Condominio 2025", layout="wide")
init_db()

st.title("🏢 CONDOMINIOS NANTU")

if "conectado" not in st.session_state:
    st.session_state.conectado = False
if "user" not in st.session_state:
    st.session_state.user = ""
if "rol" not in st.session_state:
    st.session_state.rol = ""

# ------------------ LOGIN ------------------
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

    st.info("Ingresa con tus credenciales para gestionar pagos y ver el histórico.")
    st.stop()

# ------------------ APP ------------------
st.sidebar.success(f"Conectado como: {st.session_state.user}")
st.sidebar.write(f"Rol: **{st.session_state.rol}**")

menu = st.sidebar.radio("Menú", ["Dashboard", "Cargar Pago", "Histórico", "Administrador"])

# ------------------ ADMIN ------------------
if menu == "Administrador":
    st.subheader("🛠 Administración del Sistema")

    st.warning(
        "⚠️ Ejecuta la carga histórica UNA sola vez (o con control anti-duplicados). "
        "Si ya cargaste, no lo repitas."
    )

    if st.sidebar.button("🚀 Ejecutar Carga Histórica"):
        try:
            exit_code = os.system("python importar_datos.py")
            if exit_code == 0:
                st.success("Carga histórica ejecutada correctamente. Ve a 'Histórico' o 'Dashboard'.")
            else:
                st.error(f"La carga terminó con código {exit_code}. Revisa logs.")
        except Exception as e:
            st.error(f"Error al ejecutar carga: {e}")

    st.divider()
    df = cargar_df_pagos()
    st.write(f"Registros en base: **{len(df)}**")
    if not df.empty:
        st.dataframe(df.tail(50), use_container_width=True)

# ------------------ CARGAR PAGO ------------------
elif menu == "Cargar Pago":
    st.subheader("📝 Registrar nuevo ingreso")

    casa = st.selectbox("Seleccione Casa", [f"C{i:02d}" for i in range(1, 11)])
    monto = st.number_input("Monto Recibido ($)", min_value=0.0, step=0.01)
    fecha = st.date_input("Fecha del depósito")

    if st.button("Guardar Registro"):
        p, d, s, o = guardar_pago(casa, str(fecha), float(monto), st.session_state.user)
        st.success(f"¡Guardado! Provisión: ${p} | Décimos: ${d} | Sueldo: ${s} | Operativo: ${o}")

# ------------------ DASHBOARD ------------------
elif menu == "Dashboard":
    st.subheader("📊 Resumen General")

    df = cargar_df_pagos()

    if df.empty:
        st.info("Aún no hay datos.")
    else:
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Recaudado", f"${df['monto'].sum():,.2f}")
        col2.metric("Provisión", f"${df['provision'].sum():,.2f}")
        col3.metric("Décimos", f"${df['decimos'].sum():,.2f}")
        col4.metric("Sueldo", f"${df['sueldo'].sum():,.2f}")
        col5.metric("Operativo", f"${df['operativo'].sum():,.2f}")

        st.divider()
        st.subheader("Ingresos por Casa")
        por_casa = df.groupby("casa")["monto"].sum().sort_index()
        st.bar_chart(por_casa)

        st.divider()
        st.subheader("Últimos movimientos")
        st.dataframe(df.sort_values("fecha", ascending=False).head(30), use_container_width=True)

# ------------------ HISTÓRICO ------------------
elif menu == "Histórico":
    st.subheader("📚 Histórico de Pagos (Cargados + Manuales)")

    df = cargar_df_pagos()

    if df.empty:
        st.info("No hay registros aún.")
    else:
        # Filtros
        casas = ["Todas"] + sorted([c for c in df["casa"].dropna().unique().tolist()])
        casa_sel = st.selectbox("Filtrar por Casa", casas)

        min_fecha = df["fecha"].min()
        max_fecha = df["fecha"].max()

        colf1, colf2 = st.columns(2)
        with colf1:
            f_ini = st.date_input("Desde", value=min_fecha.date() if pd.notna(min_fecha) else None)
        with colf2:
            f_fin = st.date_input("Hasta", value=max_fecha.date() if pd.notna(max_fecha) else None)

        df_f = df.copy()

        if casa_sel != "Todas":
            df_f = df_f[df_f["casa"] == casa_sel]

        if f_ini and f_fin:
            df_f = df_f[(df_f["fecha"].dt.date >= f_ini) & (df_f["fecha"].dt.date <= f_fin)]

        st.caption(f"Registros filtrados: {len(df_f)}")

        # Totales del filtro
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Monto", f"${df_f['monto'].sum():,.2f}")
        c2.metric("Provisión", f"${df_f['provision'].sum():,.2f}")
        c3.metric("Décimos", f"${df_f['decimos'].sum():,.2f}")
        c4.metric("Sueldo", f"${df_f['sueldo'].sum():,.2f}")
        c5.metric("Operativo", f"${df_f['operativo'].sum():,.2f}")

        st.divider()
        st.dataframe(df_f.sort_values("fecha", ascending=False), use_container_width=True)

        # Descarga CSV
        csv_bytes = df_f.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Descargar CSV (filtrado)",
            data=csv_bytes,
            file_name="historico_filtrado.csv",
            mime="text/csv",
        )

# ------------------ LOGOUT ------------------
st.sidebar.divider()
if st.sidebar.button("Cerrar sesión"):
    st.session_state.conectado = False
    st.session_state.user = ""
    st.session_state.rol = ""
    st.rerun()

