import pymssql
from typing import Optional, Tuple
from api_requests import RequestsAPI
from config import Config
from logger import Logger

db_config = Config.DB_CONFIG
log = Logger.write_log
api = RequestsAPI

class DatabaseSQL:
    
    def __init__(self):
        pass

    # Connexion à la BDD
    def get_db_connection(self) -> Tuple[Optional[pymssql.Connection], bool]:
        conn = None
        connected = False
        try:
            # Récupérer les valeurs du dictionnaire sans paramètres interdits et connexion à la base de données
            conn = pymssql.connect(
                server = str(db_config.get("SERVER", "")),
                user = str(db_config.get("USERNAME", "")),
                password = str(db_config.get("PASSWORD", "")),
                database = str(db_config.get("DATABASE", ""))
            )

            try:
                # Vérification de la connexion
                cur = conn.cursor()
                cur.execute("SELECT @@VERSION")
                version = cur.fetchone()[0].split("\n")[0].strip()
                log(f"✅ Connexion Data Warehouse établie. Version : {version}")
                connected = True

            except (pymssql.DatabaseError, pymssql.InterfaceError) as e:
                log(f"⛔️ Erreur de connexion au Data Warehouse : {e}")
                conn.close()
                conn = None

        except Exception as e:
            log(f"⛔️ Échec de connexion au Data Warehouse : {e}")

        finally:
            return conn, connected

    # Suppression des données de la table TM_MAD_Catalog_DROPFR
    def delete_catalog_dropfr_data(self, conn):
        try:
            cursor = conn.cursor()

            # Vérifier si la table contient des données
            query_count_cat = "SELECT COUNT(*) FROM TM_MAD_Catalog_DROPFR"
            cursor.execute(query_count_cat)
            cat_count = cursor.fetchone()[0]

            deleted_rows = 0

            if cat_count > 0:
                query_delete_cat = "DELETE FROM TM_MAD_Catalog_DROPFR"
                cursor.execute(query_delete_cat)
                deleted_rows = cursor.rowcount
                conn.commit() # Valider la suppression
                log(f"✅ {deleted_rows} lignes supprimées de TM_MAD_Catalog_DROPFR\n")
            else:
                log("ℹ️ Aucune donnée à supprimer dans TM_MAD_Catalog_DROPFR\n")

            return deleted_rows

        except Exception as e:
            log(f"❌ Erreur lors de la suppression des informations : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    # Insertion des informations produits dans la table TM_MAD_Catalog_DROPFR
    def insert_cat_data(self, conn, offers, products, access_tk):
        try:
            cursor = conn.cursor()

            query_insert = """
                INSERT INTO TM_MAD_Catalog_DROPFR (
                    productId, gtin, title, description, brand,
                    categoryId1, label1, categoryId2, label2, categoryId3, label3,
                    picture1, picture2, picture3, picture4, picture5, picture6,
                    offerId, condition, sellerId, bestOfferRank,
                    priceWithoutTax, VATRate, DEA, ecotax,
                    inventoryStock, supplyMode, deliveryMode, shippingCostWithoutTax, additionalShippingCostWithoutTax,
                    minDeliveryTime, maxDeliveryTime, sorecop, createdAt
                )
                VALUES (
                    %(productId)s, %(gtin)s, %(title)s, %(description)s, %(brand)s,
                    %(categoryId1)s, %(label1)s, %(categoryId2)s, %(label2)s, %(categoryId3)s, %(label3)s,
                    %(picture1)s, %(picture2)s, %(picture3)s, %(picture4)s, %(picture5)s, %(picture6)s,
                    %(offerId)s, %(condition)s, %(sellerId)s, %(bestOfferRank)s,
                    %(priceWithoutTax)s, %(VATRate)s, %(DEA)s, %(ecotax)s,
                    %(inventory)s, %(supplyMode)s, %(deliveryMode)s, %(shippingCostWithoutTax)s, %(additionalShippingCostWithoutTax)s,
                    %(minDeliveryTime)s, %(maxDeliveryTime)s, %(sorecop)s, %(createdAt)s
                )
            """
            insert_count = 0

            for product, offer in zip(products, offers):
                product_id = product.get("productId")

                # Récupération des catégories
                categoryId1, label1, categoryId2, label2, categoryId3, label3 = self._extract_category(product, access_tk)

                # Récupération des pictures
                pictures = self._extract_pictures(product)

                # Récupération des prix et de la TVA
                prix_ht = self._extract_pricing(offer)

                # Récupération des informations de livraison
                deliv_modes, shipping_cost_ht, additional_shipping_cost_ht, min_time, max_time = self._extract_delivery(offer)

                data = {
                    "productId": product_id,
                    "gtin": product.get("gtin", None),
                    "title": product.get("title", None),
                    "description": product.get("description", None),
                    "brand": product.get("brand", {}).get("label", None),
                    "categoryId1": categoryId1, "label1": label1,
                    "categoryId2": categoryId2, "label2": label2,
                    "categoryId3": categoryId3, "label3": label3,
                    **dict(zip(["picture1", "picture2", "picture3", "picture4", "picture5", "picture6"], pictures)),
                    "offerId": offer.get("offerId", None),
                    "condition": offer.get("condition", None),
                    "sellerId": offer.get("sellerId", None),
                    "bestOfferRank": offer.get("bestOfferRank", None),
                    "priceWithoutTax": prix_ht,
                    "VATRate": 0.20,
                    "DEA": 0, "ecotax": 0,
                    "inventory": offer.get("inventory", {}).get("stock", None),
                    "supplyMode": offer.get("inventory", {}).get("supplyMode", None),
                    "deliveryMode": deliv_modes,
                    "shippingCostWithoutTax": shipping_cost_ht,
                    "additionalShippingCostWithoutTax" : additional_shipping_cost_ht,
                    "minDeliveryTime": min_time,
                    "maxDeliveryTime": max_time,
                    "sorecop": 0,
                    "createdAt": product.get("createdAt", None)
                }

                log(f"📝 Insertion de la paire {data['offerId']} / {product_id} dans TM_MAD_Catalog_DROPFR")
                cursor.execute(query_insert, data)
                insert_count+=1

            conn.commit()

            return insert_count

        except Exception as e:
            log(f"❌ Erreur lors de l'insertion des produits : {e}")
            conn.rollback()
            return 0

        finally:
            cursor.close()

    # Fonctions annexes pour l'insertion des données dans la table TM_MAD_Catalog_DROPFR
    def _extract_category(self, product, access_tk):
        category_ref = product.get("category", "").strip()

        # Extraire les niveaux de catégorie 2, 4 et 6
        category_levels = [category_ref[:i] for i in (2, 4, 6) if len(category_ref) >= i]

        # Compléter avec NULL si on n’a pas tous les niveaux
        while len(category_levels) < 3:
            category_levels.append(None)

        # Récupération des labels pour chaque niveau
        parent_data = []
        for ref in category_levels:
            if ref != None:
                info = api.get_categories_info(access_tk, ref)
                parent_data.append(info["label"] if info and "label" in info else None)
            else:
                parent_data.append(None)

        categoryId1, categoryId2, categoryId3 = category_levels
        label1, label2, label3 = parent_data

        return (categoryId1, label1, categoryId2, label2, categoryId3, label3)
    
    def _extract_pictures(self, product):
        # Prend les 6 premières images triées par position si elle existe
        images = sorted(product.get("images", []), key=lambda x: x.get("position", 9999))
        pictures = [img.get("url", None) for img in images[:6]]
        pictures += [None] * (6 - len(pictures))
        return pictures
    
    def _extract_pricing(self, offer):
        prix_ttc = offer.get("price", {}).get("price")
        # taxes = offer.get("price", {}).get("taxes", [])
        # vat_rate = next((t.get("value") for t in taxes if t.get("code") == "vat"), None)

        if isinstance(prix_ttc, (int, float)):
            prix_ht = round(prix_ttc / (1 + 0.20), 2)
        else:
            prix_ht = None

        return prix_ht

    def _extract_delivery(self, offer):
        delivery_modes = offer.get("inventory", {}).get("deliveryModes", [])
        if delivery_modes:
            mode = delivery_modes[0]
            deliv_modes = mode.get("mode", None)
            shipping_cost_ttc = mode.get("shippingCost", 0.0)
            additional_shipping_cost_ttc = mode.get("additionalShippingCost", 0.0)
            min_time = mode.get("minDeliveryTime", None)
            max_time = mode.get("maxDeliveryTime", None)
        else:
            deliv_modes = None
            shipping_cost_ttc = 0.0
            additional_shipping_cost_ttc = 0.0
            min_time = max_time = None

        shipping_cost_ht = round(shipping_cost_ttc / (1 + 0.20), 2)
        additional_shipping_cost_ht = round(additional_shipping_cost_ttc / (1 + 0.20), 2)
        
        return deliv_modes, shipping_cost_ht, additional_shipping_cost_ht, min_time, max_time