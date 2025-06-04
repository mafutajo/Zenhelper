import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
import pandas as pd
import time
import pandas as pd
from google.cloud import bigquery
import os
from google.oauth2 import service_account
from google.cloud import bigquery
import json

# Charger les variables du .env
load_dotenv()
project_id = st.secrets["GCP_PROJECT"]
bq_dataset = st.secrets["BQ_DATASET"]


def load_first_letters():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["GOOGLE_CREDENTIALS"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = f"""
            SELECT DISTINCT
            LOWER(SUBSTR(TRIM(title), 1, 1)) AS first_letter
            FROM `{project_id}.{bq_dataset}.public_plan`
            WHERE title IS NOT NULL and partner_id in (7,8)
            """

    try:
        df = client.query(query).to_dataframe()
        letters = sorted(df["first_letter"].dropna().tolist())
        return letters
    except Exception as e:
        st.write(f"⚠️ Erreur lors de la requête BigQuery : {e}")
        return []


def load_plans_starting_with(letter):
    st.cache_data.clear()

    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["GOOGLE_CREDENTIALS"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = f"""
        SELECT DISTINCT
            LOWER(TRIM(title)) AS title_cleaned
        FROM `{project_id}.{bq_dataset}.public_plan`
        WHERE
            title IS NOT NULL
            AND partner_id IN (7, 8)
            AND LOWER(TRIM(title)) LIKE '{letter.lower()}%'
            AND NOT LOWER(title) LIKE '%anonymized%'
    """

    try:
        df = client.query(query).to_dataframe()
        return sorted(df["title_cleaned"].dropna().tolist())
    except Exception as e:
        st.error(f"Erreur lors de la requête BigQuery : {e}")
        return []


def search_candidates(selected_plans):
    st.cache_data.clear()

    if not selected_plans:
        return pd.DataFrame()

    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["GOOGLE_CREDENTIALS"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    selected_plans_sql_array = ", ".join([f"'{p.lower()}'" for p in selected_plans])

    query = f"""
    WITH cleaned_plans AS (
        SELECT
            user_id,
            LOWER(TRIM(title)) AS title_cleaned
        FROM `{project_id}.{bq_dataset}.public_plan`
        WHERE
            title IS NOT NULL
            AND NOT LOWER(title) LIKE '%anonymized%' AND NOT LOWER(title) LIKE '%deletedbyuser%'
            AND partner_id IN (7, 8)
    ),
    aggregated_plans AS (
        SELECT
            user_id,
            ARRAY_AGG(DISTINCT title_cleaned) AS all_plans
        FROM cleaned_plans
        GROUP BY user_id
    ),
    filtered_users AS (
        SELECT
            user_id,
            all_plans,
            ARRAY(
                SELECT plan FROM UNNEST([{selected_plans_sql_array}]) AS plan
                WHERE plan IN UNNEST(all_plans)
            ) AS matching_plans
        FROM aggregated_plans
        WHERE NOT EXISTS (
            SELECT plan FROM UNNEST([{selected_plans_sql_array}]) AS plan
            WHERE plan NOT IN UNNEST(all_plans)
        )
    )
    SELECT
        m.username,
        m.email,
        f.all_plans,
        f.matching_plans,
        ARRAY_LENGTH(f.matching_plans) AS matching_count
    FROM filtered_users AS f
    JOIN `{project_id}.{bq_dataset}.public_member` AS m
        ON f.user_id = m.id
    ORDER BY matching_count DESC
    """

    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"❌ Erreur BigQuery : {e}")
        return pd.DataFrame()

    # Nettoyage et formatage
    df["matching_plans"] = df["matching_plans"].apply(
        lambda x: ", ".join(sorted(set([p.strip() for p in x if p.strip()])))
    )
    df["all_plans"] = df["all_plans"].apply(
        lambda x: ", ".join(sorted(set([p.strip() for p in x if p.strip()])))
    )

    df["completion"] = (
        df["matching_count"].astype(str) + f" / {len(selected_plans)} plans"
    )

    # Détection des plans en plus (ceux que l'utilisateur a mais qui ne sont pas dans selected_plans)

    return df[
        [
            "username",
            "email",
            "matching_plans",
            "all_plans",
            "completion",
            "matching_count",
        ]
    ]


def load_usernames(folder="", prefix="user.csv_part"):
    usernames = []

    for file in sorted(os.listdir(folder)):
        if file.startswith(prefix) and file.endswith(".csv"):
            path = os.path.join(folder, file)
            df = pd.read_csv(path, usecols=["username"])
            usernames.extend(df["username"].dropna().astype(str).str.strip())

    return sorted(set(usernames))


# --- Utilitaire pour exclure les emails numériques ---
def is_numeric_email_name(email):
    try:
        local = email.split("@")[0]
        return local.isdigit()
    except:
        return False


# --- Requête BigQuery directe ---
def search_users_by_name(name_input):
    st.cache_data.clear()  # Forcer lecture fraîche

    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["GOOGLE_CREDENTIALS"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query = f"""
    SELECT
        LOWER(TRIM(username)) AS username,
        LOWER(TRIM(email)) AS email
    FROM `{project_id}.{bq_dataset}.public_member`
    WHERE
        LOWER(username) LIKE '%{name_input.lower()}%'
        AND NOT REGEXP_CONTAINS(SPLIT(email, "@")[OFFSET(0)], r'^[0-9]+$')
    """

    return client.query(query).to_dataframe()


# --- Mot de passe à définir ici (à sécuriser dans un environnement prod) ---
PASSWORD = st.secrets["APP_PASSWORD"]

# --- Authentification ---
if "auth" not in st.session_state:
    st.session_state.auth = False

st.set_page_config(
    page_title="Zendesk helper",
    page_icon="https://cdn-icons-png.freepik.com/512/4038/4038734.png?uid=R150887685&ga=GA1.1.1638742484.1714642249",
    layout="wide",
)


# 👉 Masquer le menu, le footer, le bouton Rerun et le bandeau "Running"
st.markdown(
    """
    <style>
        /* Cache le menu hamburger (≡) */
        #MainMenu {visibility: hidden;}

        /* Cache le footer "Made with Streamlit" */
        footer {visibility: hidden;}

        /* Cache le bouton "Rerun" */
        .stDeployButton {visibility: hidden;}

        /* Cache le bandeau "Running" */
        .stStatusWidget {visibility: hidden;}
    </style>
""",
    unsafe_allow_html=True,
)

if not st.session_state.auth:

    split = st.columns([2, 4, 2], gap="large")
    with split[1]:

        st.markdown(
            """
        <style>
        .zendesk-help {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 16px;
            border-radius: 8px;
            font-family: "Segoe UI", sans-serif;
            color: #063970;
            font-size: 4rem;
            font-weight: 600;
            margin-top: 10px;
        }

        .zendesk-help img {
            width: 4.5rem;
            height: 4.5rem;
            object-fit: contain;
        }
        </style>

        <div class="zendesk-help">
            <img src="https://cdn-icons-png.freepik.com/512/4038/4038734.png?uid=R150887685&ga=GA1.1.1638742484.1714642249"alt="Zendesk Logo">
            <span>Zendesk helper</span>
        </div>
        """,
            unsafe_allow_html=True,
        )

        mdp = st.text_input("**Entrez le mot de passe** :", type="password")

        splite = st.columns([2, 4, 2], gap="large")
        with splite[1]:
            if st.button(
                "**Valider**", use_container_width=True, icon=":material/lock:"
            ):
                if mdp == PASSWORD:
                    st.session_state.auth = True
                    st.success("Accès autorisé")
                    st.rerun()
                else:
                    st.error("**Mot de passe incorrect.**", icon=":material/error:")

else:
    if "letters" not in st.session_state:
        st.session_state["letters"] = load_first_letters()
    base = st.tabs(["**✉️ Search user**", "**🗺️ Search plan**"])

    with base[0]:
        st.markdown(
            """
        <style>
        .custom-info {
            background-color: #e6f0fa;
            border-left: 6px solid #063970;
            padding: 16px 24px;
            border-radius: 10px;
            font-family: 'Segoe UI', sans-serif;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            display: flex;
            align-items: flex-start;
            gap: 16px;
        }

        .custom-info img {
            width: 32px;
            height: 32px;
            margin-top: 4px;
        }

        .custom-info-text {
            color: #000;
        }

        .custom-info-text h4 {
            margin: 0 0 6px 0;
            color: #063970;
            font-size: 1.1rem;
        }

        .custom-info-text p {
            margin: 0;
            font-size: 0.95rem;
        }
        </style>

        <div class="custom-info">
            <img src="https://cdn-icons-png.freepik.com/512/202/202481.png?ga=GA1.1.1467359864.1748869432" alt="Icône recherche">
            <div class="custom-info-text">
                <h4>Recherche utilisateur par username</h4>
                <p>Cette section vous permet de retrouver l'adresse email associée à un username.
                Entrez un nom dans le champ à gauche pour voir les correspondances à droite.</p>
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # UI
        split = st.columns(2, gap="large")

        with split[0]:
            st.write("### 🔍 Entrez un nom d’utilisateur")
            user_name = st.text_input(
                "Entrez le nom de l'utilisateur :", "", label_visibility="collapsed"
            )

        with split[1]:
            if not user_name:
                st.write("**Liste des candidats potentiels**")
                st.info("Tapez un nom à gauche pour commencer.")
            else:
                st.cache_data.clear()

                progress_bar = st.progress(0)
                status = st.empty()

                status.markdown("📡 **Fetching data from the depths of BigQuery...**")
                for i in range(40):
                    progress_bar.progress(min(i / 100, 0.8))
                    time.sleep(0.01)

                with st.spinner(
                    f"""**Recherche de** "*{user_name}*" **en cours...**"""
                ):
                    filtered_df = search_users_by_name(user_name)

                status.markdown("🤹‍♂️ **Sorting data like a circus juggler...**")
                for i in range(40, 80):
                    progress_bar.progress(min(i / 100, 0.8))
                    time.sleep(0.01)

                progress_bar.progress(80)
                status.empty()

                if filtered_df.empty:
                    st.write("**Liste des candidats potentiels**")
                    st.warning(
                        "**Aucun utilisateur ne correspond à cette recherche.**",
                        icon=":material/warning:",
                    )
                else:
                    st.write(f"**Liste des candidats potentiels ({len(filtered_df)})**")
                    st.dataframe(filtered_df, use_container_width=True, hide_index=True)
    with base[1]:
        st.markdown(
            """
    <style>
    .custom-info {
        background-color: #e6f7ec;
        border-left: 6px solid #2e7d32;
        padding: 16px 24px;
        border-radius: 10px;
        font-family: 'Segoe UI', sans-serif;
        margin-bottom: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        display: flex;
        align-items: flex-start;
        gap: 16px;
    }

    .custom-info img {
        width: 32px;
        height: 32px;
        margin-top: 4px;
    }

    .custom-info-text {
        color: #000;
    }

    .custom-info-text h4 {
        margin: 0 0 6px 0;
        color: #2e7d32;
        font-size: 1.1rem;
    }

    .custom-info-text p {
        margin: 0;
        font-size: 0.95rem;
    }
    </style>

    <div class="custom-info">
        <img src="https://cdn-icons-png.freepik.com/512/7129/7129060.png?uid=R150887685&ga=GA1.1.1638742484.1714642249" alt="Icône recherche">
        <div class="custom-info-text">
            <h4>Recherche utilisateur par nom de plan</h4>
            <p>Sélectionnez un ou plusieurs noms de plan à gauche pour découvrir quels utilisateurs possèdent ces plans.</p>
        </div>
    </div>
    """,
            unsafe_allow_html=True,
        )

        # --- Initialisation des plans sélectionnés ---
        if "selected_plans" not in st.session_state:
            st.session_state["selected_plans"] = []

        if "letters" not in st.session_state:
            st.session_state["letters"] = load_first_letters()

        if "selected_plans" not in st.session_state:
            st.session_state["selected_plans"] = []
        if "last_selected_letter" not in st.session_state:
            st.session_state["last_selected_letter"] = st.session_state["letters"][0]

        if "plans_for_letter" not in st.session_state:
            st.session_state["plans_for_letter"] = []

        # Interface
        splot = st.columns([4, 6], gap="large")

        with splot[0]:
            st.markdown(
                """
                <style>
                .section-title {
                    font-size: 1.4rem;
                    font-weight: 600;
                    color: #2e7d32;
                    margin-bottom: 12px;
                    margin-top: 12px;
                    font-family: 'Segoe UI', sans-serif;
                    padding-left: 12px;
                }
                </style>

                <div class="section-title"> Sélection des plans</div>
                """,
                unsafe_allow_html=True,
            )

            slice = st.columns(2, gap="large")
            with slice[0]:
                selected_letter = st.selectbox(
                    "**Lettre de départ :**",
                    st.session_state["letters"],
                    index=st.session_state["letters"].index(
                        st.session_state["last_selected_letter"]
                    ),
                    key="current_letter",
                )

            with slice[1]:
                letter_already_loaded = (
                    selected_letter == st.session_state["last_selected_letter"]
                )
                st.markdown("")
                change_letter_clicked = st.button(
                    "**Charger les plans**",
                    use_container_width=True,
                    disabled=letter_already_loaded,
                    icon=":material/refresh:",
                )

            # Chargement uniquement si clic ET lettre différente
            if change_letter_clicked and not letter_already_loaded:
                with st.spinner(
                    f"**Chargement des plans commençant par '{selected_letter}'...**"
                ):
                    st.session_state["plans_for_letter"] = load_plans_starting_with(
                        selected_letter
                    )
                    st.session_state["last_selected_letter"] = selected_letter

            # Liste filtrée
            plans_options = [
                p
                for p in st.session_state["plans_for_letter"]
                if p not in st.session_state["selected_plans"]
            ]

            plans_to_add = st.multiselect(
                f"**Plans commençant par '{st.session_state['last_selected_letter']}'**",
                options=plans_options,
                key=f"plans_{st.session_state['last_selected_letter']}",
            )

            if st.button("**Ajouter le plan**", icon=":material/add_circle_outline:"):
                duplicates = [
                    p for p in plans_to_add if p in st.session_state["selected_plans"]
                ]
                new_plans = [
                    p
                    for p in plans_to_add
                    if p not in st.session_state["selected_plans"]
                ]

                if duplicates:
                    st.error(
                        f"Le(s) plan(s) suivant(s) sont déjà sélectionné(s) : {', '.join(duplicates)}",
                        icon="🚫",
                    )

                for plan in new_plans:
                    st.session_state["selected_plans"].append(plan)

            st.write(
                f"🧺 Plans sélectionnés ({len(st.session_state['selected_plans'])})"
            )
            st.write(st.session_state["selected_plans"])

            if st.button("🗑️ Réinitialiser les plans sélectionnés"):
                st.session_state["selected_plans"] = []
                st.rerun()

            launch = st.button("🔍 Lancer la recherche")

        # Résultats
        with splot[1]:
            st.write("### 📬 Résultats")

            if launch:
                st.cache_data.clear()

                if not st.session_state["selected_plans"]:
                    st.warning("Aucun plan sélectionné.")
                else:
                    # Barre de progression au démarrage
                    progress = st.progress(0, text="🔍 Lancement de la recherche...")
                    progress.progress(20, text="🔍 Lecture des données...")

                    results = search_candidates(st.session_state["selected_plans"])
                    progress.progress(80, text="📦 Traitement terminé...")

                    if results.empty:
                        progress.progress(100)
                        st.warning("Aucun résultat.")
                    else:
                        progress.progress(100, text="✅ Recherche terminée")
                        st.success(f"{len(results)} email(s) trouvés")
                        st.dataframe(results, use_container_width=True, hide_index=True)

                    # Nettoyage de la barre une fois fini
                    progress.empty()
