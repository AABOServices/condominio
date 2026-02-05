import streamlit as st
import pandas as pd
import sqlite3
import subprocess
import sys
import altair as alt
import hashlib

DB_PATH = "condominio.db"


# ------------------ DB helpers ------------------
def ensure_users_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            user TEXT PRIMARY KEY,
            pw TEXT,
            rol TEXT
        )
    """)
    # admin/admin123 por defecto
    pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
    cur.execute("INSERT OR IGNORE INTO usuarios VALUES ('admin', ?, 'ADMINISTRADOR')", (pw_hash,))
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


def cargar_df_pagos():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM pagos", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()

    if not df.empty:
        # Normaliza fecha_pago
        df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], errors="coerce")
        # Asegura columnas numéricas
        for c in ["monto_a_pagar", "monto_pagado", "provision", "decimos", "sueldo", "saldo_pagar"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df


def ejecutar_importacion_con_log(script_name="importar_datos.py"):
    result = subprocess.run(
        [sys.executable, script_name],
        capture_output=True,
        text=True
    )
    return result.returncode, result.stdout, result.stderr


# ------------------ App setup ------------------
st.set_page_config(page_title="Condominio 2025", layout="wide")
ensure_users_table()

st.title("🏢 CONDOMINIOS NANTU")

# Session state
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

    st.info("Ingresa con tus credenciales para visualizar el dashboard e históricos.")
    st.stop()

st.sidebar.success(f"Conectado como: {st.session_state.user}")
st.sidebar.write(f"Rol: **{st.session_state.rol}**")

menu = st.sidebar.radio("Menú", ["Dashboard", "Histórico", "Administrador"])

# ------------------ Load data once ------------------
df = cargar_df_pagos()

# ------------------ Shared filters (aplican a TODO) ------------------
st.sidebar.divider()
st.sidebar.subheader("🎛️ Filtros (globales)")

if df.empty:
    st.sidebar.info("Aún no hay datos en la tabla 'pagos'.")
    casa_sel = "Todas"
    prop_filter = ""
    f_ini = None
    f_fin = None
    df_f = df
else:
    casas = ["Todas"] + sorted(df["casa"].dropna().unique().tolist())
    casa_sel = st.sidebar.selectbox("Casa", casas)

    propietarios = sorted(df["propietario"].dropna().unique().tolist())
    prop_filter = st.sidebar.text_input("Propietario (contiene)", value="")

    min_fecha = df["fecha_pago"].min()
    max_fecha = df["fecha_pago"].max()

    colf1, colf2 = st.sidebar.columns(2)
    with colf1:
        f_ini = st.date_input("Desde", value=min_fecha.date() if pd.notna(min_fecha) else None)
    with colf2:
        f_fin = st.date_input("Hasta", value=max_fecha.date() if pd.notna(max_fecha) else None)

    df_f = df.copy()

    if casa_sel != "Todas":
        df_f = df_f[df_f["casa"] == casa_sel]

    if prop_filter.strip():
        df_f = df_f[df_f["propietario"].str.contains(prop_filter.strip(), case=False, na=False)]

    if f_ini and f_fin:
        df_f = df_f[
            (df_f["fecha_pago"].dt.date >= f_ini) &
            (df_f["fecha_pago"].dt.date <= f_fin)
        ]


# ------------------ DASHBOARD ------------------
if menu == "Dashboard":
    st.subheader("📊 Dashboard (filtrado)")

    if df_f.empty:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Monto a pagar (Σ)", f"${df_f['monto_a_pagar'].sum():,.2f}")
        c2.metric("Monto pagado (Σ)", f"${df_f['monto_pagado'].sum():,.2f}")
        c3.metric("Saldo a pagar (Σ)", f"${df_f['saldo_pagar'].sum():,.2f}")
        c4.metric("Registros", f"{len(df_f)}")

        st.divider()

        # Histograma / barras por período (mes) del saldo
        df_chart = df_f.copy()
        df_chart["periodo"] = df_chart["fecha_pago"].dt.to_period("M").astype(str)
        df_agg = df_chart.groupby("periodo", as_index=False)["saldo_pagar"].sum()
        df_agg["signo"] = df_agg["saldo_pagar"].apply(lambda x: "Positivo" if x >= 0 else "Negativo")

        # Colores requeridos
        color_scale = alt.Scale(
            domain=["Positivo", "Negativo"],
            range=["#2e7d32", "#ef6c00"]  # verde / anaranjado
        )

        chart = (
            alt.Chart(df_agg)
            .mark_bar()
            .encode(
                x=alt.X("periodo:N", title="Período (YYYY-MM)", sort=None),
                y=alt.Y("saldo_pagar:Q", title="Saldo a pagar (Σ)", axis=alt.Axis(format=",.2f")),
                color=alt.Color("signo:N", scale=color_scale, legend=alt.Legend(title="Signo")),
                tooltip=[
                    alt.Tooltip("periodo:N", title="Período"),
                    alt.Tooltip("saldo_pagar:Q", title="Saldo (Σ)", format=",.2f"),
                    alt.Tooltip("signo:N", title="Signo")
                ],
            )
            .properties(height=380)
        )

        # Línea 0 para enfatizar negativos hacia abajo
        zero_line = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule().encode(y="y:Q")

        st.altair_chart(chart + zero_line, use_container_width=True)

        st.divider()

        # Resumen por casa (si no está filtrado por una casa)
        if casa_sel == "Todas":
            st.subheader("Resumen por Casa (Saldo Σ)")
            df_casa = df_f.groupby("casa", as_index=False)["saldo_pagar"].sum()
            df_casa["signo"] = df_casa["saldo_pagar"].apply(lambda x: "Positivo" if x >= 0 else "Negativo")

            chart2 = (
                alt.Chart(df_casa)
                .mark_bar()
                .encode(
                    x=alt.X("casa:N", title="Casa", sort="ascending"),
                    y=alt.Y("saldo_pagar:Q", title="Saldo a pagar (Σ)", axis=alt.Axis(format=",.2f")),
                    color=alt.Color("signo:N", scale=color_scale, legend=None),
                    tooltip=[
                        alt.Tooltip("casa:N", title="Casa"),
                        alt.Tooltip("saldo_pagar:Q", title="Saldo (Σ)", format=",.2f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(chart2 + zero_line, use_container_width=True)

        st.divider()
        st.subheader("📌 Tabla (filtrada)")
        st.dataframe(
            df_f.sort_values("fecha_pago", ascending=False),
            use_container_width=True
        )


# ------------------ HISTÓRICO ------------------
elif menu == "Histórico":
    st.subheader("📚 Histórico (filtrado)")

    if df_f.empty:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        st.caption(f"Registros filtrados: {len(df_f)}")
        st.dataframe(df_f.sort_values("fecha_pago", ascending=False), use_container_width=True)

        csv_bytes = df_f.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Descargar CSV (filtrado)",
            data=csv_bytes,
            file_name="historico_filtrado.csv",
            mime="text/csv"
        )


# ------------------ ADMINISTRADOR ------------------
elif menu == "Administrador":
    st.subheader("🛠 Administrador")

    st.warning(
        "Este proceso ejecuta importar_datos.py. "
        "Si tu importador está configurado para BORRAR y RECREAR la base, se perderán datos previos."
    )

    if st.sidebar.button("🚀 Ejecutar Carga Completa (recrear BD)"):
        returncode, stdout, stderr = ejecutar_importacion_con_log("importar_datos.py")

        st.write("### Resultado de ejecución")
        st.write("**Return code:**", returncode)

        st.write("**STDOUT:**")
        st.code(stdout if stdout else "(vacío)", language="text")

        st.write("**STDERR:**")
        st.code(stderr if stderr else "(vacío)", language="text")

        if returncode == 0:
            st.success("✅ Carga completa OK. Recarga la página o ve a Dashboard/Histórico.")
        else:
            st.error("❌ La carga falló. Revisa el STDERR (motivo real).")

    st.divider()
    df2 = cargar_df_pagos()
    st.write(f"Registros en base: **{len(df2)}**")
    if not df2.empty:
        st.dataframe(df2.sort_values("fecha_pago", ascending=False).head(50), use_container_width=True)


# ------------------ LOGOUT ------------------
st.sidebar.divider()
if st.sidebar.button("Cerrar sesión"):
    st.session_state.conectado = False
    st.session_state.user = ""
    st.session_state.rol = ""
    st.rerun()
