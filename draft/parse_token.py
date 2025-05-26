import xml.etree.ElementTree as ET

# Пространства имён, такие же как в Airflow-скрипте
ns = {
    "oai": "http://www.openarchives.org/OAI/2.0/"
}

def parse_resumption_token(xml_file_path: str):
    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    token_elem = root.find(".//oai:resumptionToken", ns)

    if token_elem is not None and token_elem.text:
        token = token_elem.text.strip()
        print(f"🔁 resumptionToken найден: {token}")
        return token
    else:
        print("❌ resumptionToken не найден.")
        return None

if __name__ == "__main__":
    parse_resumption_token("oai2.xml")
