import re
import spacy

nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])

def clean_text(text: str) -> str:
    print(f"ochube; clean_text; {text}")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 ]+", "", text)
    doc = nlp(text.lower())
    tokens = [t.lemma_ for t in doc if not t.is_stop and t.is_alpha]
    return " ".join(tokens)
