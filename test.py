import os
import pandas as pd


import os


def delete_grouped_by_email_parts(folder=".", prefix="grouped_by_email_part"):
    deleted = 0
    for file in os.listdir(folder):
        if file.startswith(prefix) and file.endswith(".csv"):
            os.remove(os.path.join(folder, file))
            deleted += 1
    print(f"🗑️ {deleted} anciens fichiers supprimés.")


import pandas as pd


def split_grouped_csv(
    file_path="grouped_by_email.csv", max_mb=100, initial_chunksize=7_000_000
):
    name_without_ext = "grouped_by_email_part"
    file_prefix = os.path.join(".", name_without_ext)

    chunk_size = initial_chunksize
    file_index = 0

    print("📤 Début du split...")
    reader = pd.read_csv(file_path, chunksize=chunk_size)

    while True:
        try:
            chunk = next(reader)
            output_file = f"{file_prefix}{file_index}.csv"
            chunk.to_csv(output_file, index=False)

            file_size = os.path.getsize(output_file) / (1024 * 1024)
            print(f"✅ {output_file} — {file_size:.2f} MB")

            if file_size > max_mb:
                os.remove(output_file)
                chunk_size = chunk_size // 2
                print(f"⚠️ Trop gros ➝ réduction à {chunk_size} lignes")
                if chunk_size < 1000:
                    raise ValueError(
                        "❌ chunk_size trop petit. Impossible de continuer."
                    )
                return split_grouped_csv(
                    file_path, max_mb=max_mb, initial_chunksize=chunk_size
                )

            file_index += 1
        except StopIteration:
            break


delete_grouped_by_email_parts()
split_grouped_csv()
