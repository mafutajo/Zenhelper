import pandas as pd
import re


def is_valid(title):
    if "anonymized" in title.lower():
        return False
    if len(re.findall(r"[a-zA-Z0-9]", title)) < 3:
        return False
    return True


def is_numeric_email_name(email):
    if not isinstance(email, str):
        return False
    parts = email.split("@")
    if len(parts) != 2:
        return False
    username, _ = parts
    return username.isdigit()


def process_mail_list(input_path="Doc/mail_list.csv"):
    df = pd.read_csv(input_path)

    # Nettoyage de title_cleaned
    df["title_cleaned"] = df["title_cleaned"].astype(str).str.strip().str.lower()
    df = df[df["title_cleaned"].apply(is_valid)]

    # Supprimer les emails avec identifiant numérique uniquement (avant le @)
    if "email" in df.columns:
        df = df[~df["email"].apply(is_numeric_email_name)]
        print("✅ Emails avec identifiant uniquement numérique supprimés.")

    # Supprimer la colonne created_at si elle existe
    if "created_at" in df.columns:
        df = df.drop(columns=["created_at"])
        print("✅ Colonne 'created_at' supprimée.")

    # === letters.csv ===
    unique_letters = sorted(set(df["title_cleaned"].str[0].dropna()))
    pd.DataFrame(unique_letters, columns=["first_letter"]).to_csv(
        "letters.csv", index=False
    )
    print("✅ letters.csv généré.")

    # === grouped_by_email.csv ===
    if "email" in df.columns:
        grouped = (
            df.groupby("email")["title_cleaned"]
            .apply(lambda x: ",".join(sorted(set(x.dropna().astype(str)))))
            .reset_index()
        )
        grouped.to_csv("grouped_by_email.csv", index=False)
        print("✅ grouped_by_email.csv généré.")
    else:
        print("⚠️ Colonne 'email' absente, grouped_by_email.csv non généré.")


# Exécution
if __name__ == "__main__":
    process_mail_list()
