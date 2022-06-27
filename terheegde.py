import configparser
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

import dropbox
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))

date_now = datetime.now().strftime("%c").replace(":", "-")

logger = logging.getLogger("terheegde_api")
logging.basicConfig(
    filename="terheegde_api_" + date.today().strftime("%V") + ".log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)  # nieuwe log elke week

login_token = requests.post(
    "https://api.terheegde.nl/api/account/login",
    json={
        "email": alg_config.get("terheegde website", "email"),
        "password": alg_config.get("terheegde website", "password"),
    },
)
if login_token.status_code != 200:
    logger.error(f"error login{login_token.status_code}")

token = "Bearer " + login_token.json()["token"]

aantal_producten = 96

producten = []

doorgaan = True


def get_products(page, aantal_producten=24):
    global doorgaan
    payload = (
        '{"page":'
        + str(page)
        + ',"query":null,"pageSize":'
        + str(aantal_producten)
        + ',"sorts":[{"field":"popularity","desc":true}],"filters":[{"ref":"voorraad","options":[{"r":"ja"}]}],"filtersChanged":true,"countsOnly":true}'
    )
    headers = {"Authorization": token, "Content-Type": "application/json"}

    response = requests.request(
        "POST",
        "https://api.terheegde.nl/api/shop/tags/1001/read-v2",
        headers=headers,
        data=payload,
    )
    producten.extend(response.json()["rows"])
    if len(response.json()["rows"]) != aantal_producten:
        doorgaan = False


for page in range(1, 15):
    if not doorgaan:
        break
    get_products(page, aantal_producten)

beschikbare_artikelen = pd.DataFrame.from_dict(producten)

beschikbare_artikelen = beschikbare_artikelen.dropna(subset = ["image"])

beschikbare_artikelen["brand"] = beschikbare_artikelen.brand.apply(pd.Series)

category = beschikbare_artikelen.category.apply(pd.Series).add_prefix("category_")

image = beschikbare_artikelen.image.apply(pd.Series).add_prefix("image_")

artikelen_terheegde = (
    pd.concat([beschikbare_artikelen, image, category], axis=1)
    .drop(columns={"image", "category", "highlights"})
    .assign(
        image_fileName=lambda x: "https://api.terheegde.nl/images/" + x["image_fileName"],
        eigen_sku=lambda x: "THG" + x["code"],
        advies_prijs="",
        gewicht="",
        url_artikel="",
        lange_omschrijving="",
        verpakings_eenheid="",
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
        price=lambda x: (x["price"] - x["lk"]).round(2),
        # brand=lambda x:x["brand"].str.title(),
    )
    .assign(ean = lambda x: pd.to_numeric(x["ean"], errors="coerce")).query("ean == ean")
)

artikelen_terheegde.to_csv("THG_artikelen_" + date_now + ".csv", index=False)

final_terheegde = max(Path.cwd().glob("THG_artikelen_*.csv"), key=os.path.getctime)
with open(final_terheegde, "rb") as f:
    dbx.files_upload(
        f.read(), "/macro/datafiles/THG/" + final_terheegde.name, mode=dropbox.files.WriteMode("overwrite", None), mute=True
    )

terheegde_info = artikelen_terheegde.rename(
    columns={
        "code": "sku",
        "freeStock": "voorraad",
        "price": "prijs",
        "brand": "merk",
        "category_singularName": "category",
        "image_fileName": "url_plaatje",
        "name": "product_title",
    }
)

terheegde_info_db = terheegde_info[
    [
        "eigen_sku",
        "sku",
        "ean",
        "voorraad",
        "merk",
        "prijs",
        "advies_prijs",
        "category",
        "gewicht",
        "url_plaatje",
        "url_artikel",
        "product_title",
        "lange_omschrijving",
        "verpakings_eenheid",
    ]
]

huidige_datum = datetime.now().strftime("%d_%b_%Y")
terheegde_info_db.to_sql(f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000)

with engine.connect() as con:
    con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
    aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
    totaal_stock = int(con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    totaal_prijs = int(con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    leverancier = f"{current_folder}"
    sql_insert = (
        "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
    )
    con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

engine.dispose()
