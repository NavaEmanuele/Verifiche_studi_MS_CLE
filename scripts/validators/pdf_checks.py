
import os, re

# Minimal expected PDFs per profilo (customize with your regional directives if needed)
EXPECTED_CLE = [
    r"(?i).*relazione[_ ]?cle.*\.pdf$",
    r"(?i).*tavola[_ ]?edifici.*\.pdf$",
    r"(?i).*tavola[_ ]?aree[_ ]?emergenza.*\.pdf$",
    r"(?i).*tavola[_ ]?aree[_ ]?ammas.*\.pdf$|(?i).*aree[_ ]?ammassamento.*\.pdf$",
    r"(?i).*viabilita[_ ]?(emergenza|strategica).*\.pdf$"
]

EXPECTED_MS = [
    r"(?i).*relazione[_ ]?ms.*\.pdf$",
    r"(?i).*(instab|instabilit[aà]).*\.pdf$",
    r"(?i).*(stab|zon[a|e].*stabili|amplificaz).*\.pdf$",
    r"(?i).*(isosub|substrato|bedrock).*\.pdf$",
    r"(?i).*indagini.*\.pdf$"
]

def list_pdfs(root):
    pdfs = []
    for dirpath, _, files in os.walk(root):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                rel = os.path.relpath(os.path.join(dirpath, fn), root)
                pdfs.append(rel)
    return pdfs

def check_pdfs(root, profilo):
    patterns = EXPECTED_CLE if profilo == "cle" else EXPECTED_MS
    present = list_pdfs(root)
    out = []
    matched = set()
    for pat in patterns:
        rx = re.compile(pat)
        ok = any(rx.match(p) for p in present)
        if not ok:
            out.append(dict(rule_id="PDF-MISSING", level="warning", target="PDF", message=f"Manca un PDF che soddisfi il pattern: {pat}"))
        else:
            # register first match
            for p in present:
                if rx.match(p):
                    matched.add(p); break
    # flag unreadable or oversized? (placeholder: we only check presence by name)
    return out, present, sorted(matched)
