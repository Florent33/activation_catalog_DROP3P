import requests
import re
from config import Config
from logger import Logger

api_config = Config.API_CONFIG
log = Logger.write_log

class RequestsAPI:

    # Génération du token
    def get_access_token():
        url = api_config["TOKEN_URL"]
        payload = f'client_id={api_config["CLIENT_ID"]}&client_secret={api_config["CLIENT_SECRET"]}&grant_type={api_config["GRANT_TYPE"]}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        try:
            response = requests.post(url, headers=headers, data=payload)

            if response.status_code == 200:
                access_token = response.json().get('access_token')
                if access_token:
                    log("✅ Token généré avec succès\n")
                    return access_token
                else:
                    log("❌ Le token d'accès n'a pas été trouvé dans la réponse")
                    return None
            else:
                token_error = response.json().get('error', 'Erreur inconnue')
                log(f"❌ Erreur {response.status_code} lors de l'obtention du token : {token_error}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir le token : {str(e)}")
            Logger.separator()
            return None
     
    ### SECTION OFFERS ###
    # Récupération de l'offerId par page depuis la route /offers
    def get_offer_id(access_token):
        url = f"{api_config['CALL_URL']}/offers"
        headers = {'Authorization': f'Bearer {access_token}'}
        params = {
            "status": "ACTIVE",
            "fields": "offerId,inventory.supplyMode",
            "limit": 1000,
            "updatedAtMin": "2025-07-03T01:00:00.000Z"
        }

        offers_ids = []
        page_number = 1
        offers_per_page = []
        cursor_pattern = re.compile(r'<[^>]*[?&]cursor=([^&]*)[^>]*>; rel="next"')

        while True:
            try:
                log(f"📜 Récupération des offres - Page {page_number}")
                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()
                    items = data.get("items", [])

                    # Ne garder que les offers avec supplyMode = FULFILLMENT
                    filtered_offer_ids = [
                        item.get("offerId") 
                        for item in items
                        if item.get("inventory", {}).get("supplyMode") == "FULFILLMENT"
                    ]
                    offers_ids.extend(filtered_offer_ids)

                    offers_per_page.append(len(filtered_offer_ids))
                    log(f"📦 {len(filtered_offer_ids)} offerId 'FULFILLMENT' récupérés sur la page {page_number}\n")

                    link_header = response.headers.get("Link")
                    if not link_header:
                        log("Fin de pagination, aucun header 'Link' trouvé")
                        break

                    match = cursor_pattern.search(link_header)
                    if match:
                        cursor_value = match.group(1).replace('%3D', '=')
                        params["cursor"] = cursor_value
                        page_number += 1
                        log(f"➡️ Passage à la page suivante : Page {page_number}")
                    else:
                        log("🔚 Fin de la récupération des offerId")
                        break
                else:
                    log(f"❌ Erreur {response.status_code} : {response.text}")
                    break

            except requests.exceptions.RequestException as e:
                log(f"❌ Erreur lors de la requête API : {str(e)}")
                break

        log(f"🔢 {len(offers_ids)} offerId 'FULFILLMENT' récupérés\n")
        return offers_ids
    
    # Récupération des informations offres dont le productId depuis la route /offers/{offerId}
    def get_offer_info(access_token, offer_id):
        url = f"{api_config['CALL_URL']}/offers/{offer_id}"
        headers = {'Authorization': f'Bearer {access_token}'}

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                log(f"❌ Erreur {response.status_code} : {response.text}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir les informations de l'offre {str(e)}")
            Logger.separator()
            return None

    ### SECTION PRODUCTS ###
    # Récupération de toutes les informations par produit depuis la route /products/{productId}
    def get_product_info(access_token, product_id):
        url = f"{api_config['CALL_URL']}/products/{product_id}"
        headers = {'Authorization': f'Bearer {access_token}'}

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                log(f"❌ Erreur {response.status_code} : {response.text}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir les informations par produit {str(e)}")
            Logger.separator()
            return None

    ### SECTION CATEGORIES ###
    # Récupération des informations catégorie par produit depuis la route /categories/{categoryReference}
    def get_categories_info(access_token, category_reference):
        url = f"{api_config['CALL_URL']}/categories/{category_reference}"
        headers = {'Authorization': f'Bearer {access_token}'}

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                log(f"❌ Erreur {response.status_code} : {response.text}")
                Logger.separator()
                return None

        except requests.exceptions.RequestException as e:
            log(f"❌ Erreur lors de la requête pour obtenir les informations de catégorie {str(e)}")
            Logger.separator()
            return None