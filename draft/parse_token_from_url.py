import requests
import xml.etree.ElementTree as ET

# Пространства имён OAI-PMH
ns = {
    "oai": "http://www.openarchives.org/OAI/2.0/"
}

def parse_resumption_token_from_url(url: str):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        token_elem = root.find(".//oai:resumptionToken", ns)

        if token_elem is not None and token_elem.text:
            token = token_elem.text.strip()
            print(f"🔁 resumptionToken найден: {token}")
            return token
        else:
            print("❌ resumptionToken не найден.")
            return None

    except Exception as e:
        print(f"❌ Ошибка при запросе: {e}")
        return None

if __name__ == "__main__":
    url = "http://export.arxiv.org/oai2?verb=ListRecords&from=2016-01-01&until=2016-12-31&metadataPrefix=arXiv"
    parse_resumption_token_from_url(url)
