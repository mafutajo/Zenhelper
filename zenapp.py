import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
import pandas as pd
import re

# Charger les variables du .env
load_dotenv()


def load_plans_starting_with(letter, path="distinct_titles.csv"):
    st.cache_data.clear()

    try:
        df = pd.read_csv(path, usecols=["title_cleaned"])
    except Exception as e:
        st.error(f"Erreur lors du chargement : {e}")
        return []

    # Nettoyage renforcé
    df["title_cleaned"] = (
        df["title_cleaned"]
        .astype(str)
        .str.replace(r"[\n\r\t]", "", regex=True)  # retire les retours à la ligne
        .str.replace(r"\s+", " ", regex=True)  # normalise les espaces
        .str.strip()
        .str.lower()
    )

    # Supprimer les doublons exacts après nettoyage
    cleaned_titles = sorted(set(df["title_cleaned"].dropna()))

    # Ne garder que ceux qui commencent par la lettre spécifiée
    return [p for p in cleaned_titles if p.startswith(letter.lower())]


def search_candidates(selected_plans, path="grouped_by_email.csv"):
    st.cache_data.clear()

    df = pd.read_csv(path, usecols=["email", "title_cleaned"])
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["title_cleaned"] = df["title_cleaned"].astype(str).str.strip().str.lower()

    # Convertir la chaîne de plans en set
    df["all_plans_set"] = df["title_cleaned"].apply(lambda x: set(x.split(",")))

    selected_plans_set = set(selected_plans)

    # Garder uniquement les lignes dont les plans contiennent *au minimum tous* les plans sélectionnés
    df = df[df["all_plans_set"].apply(lambda plans: selected_plans_set.issubset(plans))]

    # Colonnes informatives
    df["matching_plans"] = df["all_plans_set"].apply(
        lambda plans: ", ".join(sorted(selected_plans_set.intersection(plans)))
    )
    df["matching_count"] = df["all_plans_set"].apply(
        lambda plans: len(selected_plans_set.intersection(plans))
    )
    df["completion"] = (
        df["matching_count"].astype(str) + f" / {len(selected_plans)} plans"
    )
    df["all_plans"] = df["all_plans_set"].apply(lambda plans: ", ".join(sorted(plans)))
    df["has_extra_plans"] = df["all_plans_set"].apply(
        lambda plans: len(plans - selected_plans_set) > 0
    )

    return df[
        [
            "email",
            "matching_plans",
            "all_plans",
            "completion",
            "has_extra_plans",
            "matching_count",
        ]
    ].sort_values(by="matching_count", ascending=False)


def load_usernames(path="Doc/user.csv"):
    df = pd.read_csv(path, usecols=["username"])
    return sorted(df["username"].dropna().astype(str).str.strip().unique())


def search_users_by_name(name_input, path="Doc/user.csv"):
    st.cache_data.clear()  # Vider tout cache résiduel

    matching_rows = []

    # Lecture par morceaux de 10 000 lignes
    chunksize = 10000
    for chunk in pd.read_csv(path, usecols=["username", "email"], chunksize=chunksize):
        chunk["username"] = chunk["username"].astype(str).str.strip()
        mask = chunk["username"].str.contains(name_input, case=False, na=False)
        matches = chunk[mask]
        if not matches.empty:
            matching_rows.append(matches)

    if matching_rows:
        result = pd.concat(matching_rows, ignore_index=True)
    else:
        result = pd.DataFrame(columns=["username", "email"])

    return result


# --- Mot de passe à définir ici (à sécuriser dans un environnement prod) ---
PASSWORD = os.environ.get("APP_PASSWORD", "changement")

# --- Authentification ---
if "auth" not in st.session_state:
    st.session_state.auth = False

st.set_page_config(
    page_title="Zendesk helper",
    page_icon="https://cdn-icons-png.freepik.com/512/4038/4038734.png?uid=R150887685&ga=GA1.1.1638742484.1714642249",
    layout="wide",
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

                with st.spinner(
                    f"""**Recherche de** "*{user_name}*" **en cours...**"""
                ):
                    filtered_df = search_users_by_name(user_name)

                if filtered_df.empty:
                    st.write("**Liste des candidats potentiels**")
                    st.warning(
                        "**Aucun utilisateur ne correspond à cette recherche.**",
                        icon=":material/warning:",
                    )
                else:
                    st.write(f"**Liste des candidats potentiels ({len(filtered_df)})**")
                    st.dataframe(filtered_df, use_container_width=True)
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

        def load_first_letters(
            letter_path="letters.csv", fallback_path="Doc/mail_list.csv"
        ):
            # ⚠️ Ne pas utiliser de cache, recharger à chaque appel

            if os.path.exists(letter_path):
                try:
                    letters_df = pd.read_csv(letter_path)
                    letters = (
                        letters_df["first_letter"]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .unique()
                        .tolist()
                    )
                    return sorted(letters)
                except Exception as e:
                    print(
                        f"⚠️ Erreur lecture letters.csv : {e} — fallback sur le fichier source."
                    )

                # Initialisation

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
                        st.dataframe(results, use_container_width=True)

                    # Nettoyage de la barre une fois fini
                    progress.empty()
