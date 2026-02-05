import streamlit as st
import pandas as pd
import sqlite3
import subprocess
import sys
import altair as alt
import hashlib

DB_PATH = "condominio.db"


# ------------------ DB: usuarios mínimos ------------------
def ensure_users():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            user TEXT PRIMARY KEY,
            pw TEXT,
            rol TEXT
        )
    """)
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


def ejecutar_importacion_con_log(script_name="importar_datos.py"):
    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


# ------------------ Datos ------------------
def cargar_df_pagos():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM pagos", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()

    if df.empty:
        return df

    needed = ["casa", "propietario", "fecha_pago", "monto_pagado", "saldo_pagar"]
    for c in needed:
        if c not in df.columns:
            df[c] = None

    df["casa"] = df["casa"].fillna("").astype(str).str.strip().str.upper()
    df["propietario"] = df["propietario"].fillna("").astype(str).str.strip()
    df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], errors="coerce")

    df["monto_pagado"] = pd.to_numeric(df["monto_pagado"], errors="coerce").fillna(0.0)
    df["saldo_pagar"] = pd.to_numeric(df["saldo_pagar"], errors="coerce").fillna(0.0)

    df["periodo_mes"] = df["fecha_pago"].dt.to_period("M").astype(str)

    return df


# ------------------ App ------------------
st.set_page_config(page_title="Condominio 2025", layout="wide")
ensure_users()

st.title("🏢 CONDOMINIOS NANTU")

if "conectado" not in st.session_state:
    st.session_state.conectado = False
if "user" not in st.session_state:
    st.session_state.user = ""
if "rol" not in st.session_state:
    st.session_state.rol = ""

# ------------------ Login ------------------
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

    st.info("Ingresa con tus credenciales para visualizar dashboard, históricos y administración.")
    st.stop()

st.sidebar.success(f"Conectado como: {st.session_state.user}")
st.sidebar.write(f"Rol: **{st.session_state.rol}**")

menu = st.sidebar.radio("Menú", ["Dashboard", "Histórico", "Administrador"])

# ------------------ Load data ------------------
df = cargar_df_pagos()

# ------------------ Filtros globales ------------------
st.sidebar.divider()
st.sidebar.subheader("🎛️ Filtros (globales)")

if df.empty:
    st.sidebar.info("No hay datos. Ejecuta la carga desde 'Administrador'.")
    df_f = df
    casa_sel = "Todas"
    prop_filter = ""
    f_ini = None
    f_fin = None
else:
    casas = ["Todas"] + sorted([c for c in df["casa"].unique().tolist() if c])
    casa_sel = st.sidebar.selectbox("Casa", casas)
    prop_filter = st.sidebar.text_input("Propietario (contiene)", value="")

    min_fecha = df["fecha_pago"].min()
    max_fecha = df["fecha_pago"].max()
    col1, col2 = st.sidebar.columns(2)
    with col1:
        f_ini = st.date_input("Desde", value=min_fecha.date() if pd.notna(min_fecha) else None)
    with col2:
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
    st.subheader("📊 Dashboard por Casa (Pagado Σ vs Saldo último período)")

    if df_f.empty:
        st.info("No hay datos para mostrar con los filtros seleccionados.")
    else:
        total_pagado = df_f["monto_pagado"].sum()

        last_date = df_f["fecha_pago"].max()
        last_period = last_date.to_period("M").strftime("%Y-%m") if pd.notna(last_date) else None
        st.caption(f"Último período detectado (según filtros): **{last_period if last_period else 'N/D'}**")

        # 1) Pagado: suma en rango filtrado
        pagado_by_casa = df_f.groupby("casa", as_index=False)["monto_pagado"].sum()
        pagado_by_casa.rename(columns={"monto_pagado": "pagado_sum"}, inplace=True)

        # 2) Último saldo por casa (según fecha máxima de cada casa dentro de filtros)
        df_last = df_f.dropna(subset=["fecha_pago"]).sort_values(["casa", "fecha_pago"])
        idx = df_last.groupby("casa")["fecha_pago"].idxmax()
        last_rows = df_last.loc[idx, ["casa", "fecha_pago", "saldo_pagar"]].copy()

        # series separadas según signo
        last_rows["saldo_ultimo_neg"] = last_rows["saldo_pagar"].apply(lambda x: x if x < 0 else 0.0)
        last_rows["saldo_ultimo_pos"] = last_rows["saldo_pagar"].apply(lambda x: x if x > 0 else 0.0)

        saldo_by_casa = last_rows[["casa", "fecha_pago", "saldo_ultimo_neg", "saldo_ultimo_pos"]].copy()

        # merge
        agg = pd.merge(pagado_by_casa, saldo_by_casa, on="casa", how="left")
        agg["saldo_ultimo_neg"] = agg["saldo_ultimo_neg"].fillna(0.0)
        agg["saldo_ultimo_pos"] = agg["saldo_ultimo_pos"].fillna(0.0)

        total_saldo_neg = agg["saldo_ultimo_neg"].sum()
        total_saldo_pos = agg["saldo_ultimo_pos"].sum()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Monto Pagado (Σ)", f"${total_pagado:,.2f}")
        c2.metric("Saldo último período (Σ negativos)", f"${total_saldo_neg:,.2f}")
        c3.metric("Saldo último período (Σ positivos)", f"${total_saldo_pos:,.2f}")
        c4.metric("Registros (filtrados)", f"{len(df_f)}")

        st.divider()

        # Datos para chart (long)
        rows = []
        for _, r in agg.iterrows():
            rows.append({"casa": r["casa"], "categoria": "Pagado (Σ)", "valor": float(r["pagado_sum"])})
            rows.append({"casa": r["casa"], "categoria": "Saldo negativo (último, <0)", "valor": float(r["saldo_ultimo_neg"])})
            rows.append({"casa": r["casa"], "categoria": "Saldo positivo (último, >0)", "valor": float(r["saldo_ultimo_pos"])})

        chart_df = pd.DataFrame(rows)

        # Colores: verde, anaranjado, morado
        color_scale = alt.Scale(
            domain=["Pagado (Σ)", "Saldo negativo (último, <0)", "Saldo positivo (último, >0)"],
            range=["#2e7d32", "#ef6c00", "#6a1b9a"]
        )

        bars = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("casa:N", title="Casa", sort="ascending"),
                y=alt.Y("valor:Q", title="Monto (Σ) | Negativos hacia abajo", axis=alt.Axis(format=",.2f")),
                color=alt.Color("categoria:N", scale=color_scale, legend=alt.Legend(title="Serie")),
                tooltip=[
                    alt.Tooltip("casa:N", title="Casa"),
                    alt.Tooltip("categoria:N", title="Serie"),
                    alt.Tooltip("valor:Q", title="Valor", format=",.2f"),
                ],
            )
            .properties(height=420)
        )

        zero_line = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule().encode(y="y:Q")
        st.altair_chart(bars + zero_line, use_container_width=True)

        st.divider()

        st.subheader("📌 Resumen por Casa (filtrado)")
        agg_out = agg.copy()
        agg_out["pagado_sum"] = agg_out["pagado_sum"].round(2)
        agg_out["saldo_ultimo_neg"] = agg_out["saldo_ultimo_neg"].round(2)
        agg_out["saldo_ultimo_pos"] = agg_out["saldo_ultimo_pos"].round(2)

        agg_out.rename(columns={
            "pagado_sum": "monto_pagado_sum",
            "saldo_ultimo_neg": "saldo_ultimo_periodo_neg",
            "saldo_ultimo_pos": "saldo_ultimo_periodo_pos",
            "fecha_pago": "fecha_ultimo_registro"
        }, inplace=True)

        st.dataframe(agg_out.sort_values("casa"), use_container_width=True)

        st.divider()
        st.subheader("📄 Detalle (tabla filtrada)")
        st.dataframe(df_f.sort_values(["casa", "fecha_pago"], ascending=[True, False]), use_container_width=True)


# ------------------ HISTÓRICO ------------------
elif menu == "Histórico":
    st.subheader("📚 Histórico (filtrado)")

    if df_f.empty:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        st.caption(f"Registros filtrados: {len(df_f)}")
        st.dataframe(df_f.sort_values(["casa", "fecha_pago"], ascending=[True, False]), use_container_width=True)

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

    st.warning("Este botón ejecuta importar_datos.py para recargar la tabla pagos desde pagos_planos_2025.csv.")

    if st.sidebar.button("🚀 Ejecutar Carga Completa"):
        returncode, stdout, stderr = ejecutar_importacion_con_log("importar_datos.py")

        st.write("### Resultado de ejecución")
        st.write("**Return code:**", returncode)

        st.write("**STDOUT:**")
        st.code(stdout if stdout else "(vacío)", language="text")

        st.write("**STDERR:**")
        st.code(stderr if stderr else "(vacío)", language="text")

        if returncode == 0:
            st.success("✅ Carga completa OK. Ve a Dashboard/Histórico.")
            st.info("Si no ves cambios, recarga la página (F5).")
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

