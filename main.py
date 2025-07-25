from database import DatabaseSQL
from logger import Logger
from api_requests import RequestsAPI
from config import Config

db_config = Config.SN_CONFIG
db = DatabaseSQL()
log = Logger.write_log
api = RequestsAPI

def main():
    try:
        Logger.separator()
        log("🚀 Démarrage du processus...\n")

        # Connexion à la base de données
        conn, connected = db.get_db_connection()
        if not conn and not connected:
            return

        # Récupération du token API
        access_tk = api.get_access_token()
        if not access_tk:
            return

        # Suppression des données de la table TM_MAD_Catalog_DROPFR
        db.delete_catalog_dropfr_data(conn)

        # Récupération des offerIds
        offer_ids = api.get_offer_id(access_tk)
        if not offer_ids:
            log("❌ Aucun offerId récupéré depuis l'API /offers/{offerId}")
            return

        total_inserted = 0

        # Récupération des informations liées à l'offre et insertion en BDD
        for offer_id in offer_ids:
            offer = api.get_offer_info(access_tk, offer_id)
            if not offer:
                log(f"⚠️ Offre introuvable pour l'offerId {offer_id}")
                continue

            product_id = offer.get("productId")
            if not product_id:
                log(f"⚠️ productId manquant pour l'offre {offer_id}")
                continue

            product = api.get_product_info(access_tk, product_id)
            if not product:
                log(f"⚠️ Produit introuvable pour le productId {product_id}")
                continue
            
            inserted = db.insert_cat_data(conn, [offer], [product], access_tk)
            total_inserted += inserted
        
        # Fermeture de la connexion
        conn.close()
        log(f"✅ {total_inserted} lignes insérées dans TM_MAD_Catalog_DROPFR\n")
        log("🔚 Fin du processus.")
        Logger.separator()

    except Exception as e:
        log(f"❌ Erreur inattendue : {e}")
        Logger.separator()

if __name__ == "__main__":
    main()