import os, datetime, markdown
from jinja2 import Template

def build_report(comune, profilo, root, results, outdir):
    fam_file = {"FILE-NOT-FOUND","READ-ERROR","MDB-NOT-FOUND","MDB-CONNECT","TABLE-MISSING","PDF-MISSING"}
    fam_schema = {"FIELD-MISSING","FIELD-TYPE","FIELD-UNIQUE","FIELD-ENUM","FIELD-REGEX","GEOM-TYPE"}
    fam_rel = {"FK-LAYER-FIELD","FK-MDB-READ","FK-NOT-MATCHED"}
    fam_other = set()
    for r in results:
        code = r["rule_id"]
        if code not in fam_file and code not in fam_schema and code not in fam_rel:
            fam_other.add(code)

    def filter_res(pred):
        return [r for r in results if pred(r)]

    sezioni = [
        dict(titolo="File & Struttura", sommario="Presenza/leggibilita di file/layer e PDF.",
             risultati=filter_res(lambda r: r["rule_id"] in fam_file)),
        dict(titolo="Schema campi", sommario="Campi obbligatori, tipo, enum, regex, univocita.",
             risultati=filter_res(lambda r: r["rule_id"] in fam_schema)),
        dict(titolo="Relazioni & MDB", sommario="FK tra layer e MDB, presenza tabelle/campi.",
             risultati=filter_res(lambda r: r["rule_id"] in fam_rel)),
        dict(titolo="Altre regole", sommario="Topologia e vincoli specifici (se implementati).",
             risultati=filter_res(lambda r: r["rule_id"] in fam_other)),
    ]
    n_err = sum(1 for r in results if r["level"]=="error")
    n_war = sum(1 for r in results if r["level"]=="warning")

    tpl_str = \"\"\"# Report Verifica — {{ comune }} ({{ profilo|upper }}) — {{ data }}

**Percorso progetto:** {{ root }}

## Sintesi
- Esito: **{{ esito }}**
- Errori: **{{ n_errori }}** - Warning: **{{ n_warning }}**

## Dettaglio Controlli
{% for sezione in sezioni %}
### {{ sezione.titolo }}
{{ sezione.sommario }}

| # | Regola | Livello | Target | Messaggio |
|--:|:-------|:--------|:-------|:----------|
{% for i, r in enumerate(sezione.risultati, 1) -%}
| {{ i }} | {{ r.rule_id }} | {{ r.level }} | {{ r.target }} | {{ r.message }} |
{% endfor %}

{% endfor %}

*Generato automaticamente.*
\"\"\"
    html = markdown.markdown(Template(tpl_str).render(
        comune=comune,
        profilo=profilo,
        data=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        root=root,
        esito="CONFORME" if n_err==0 else "NON CONFORME",
        n_errori=n_err,
        n_warning=n_war,
        sezioni=sezioni
    ), extensions=["tables"])

    md_path = os.path.join(outdir, f"report_{profilo}_{comune}.md")
    html_path = os.path.join(outdir, f"report_{profilo}_{comune}.html")
    pdf_path = os.path.join(outdir, f"report_{profilo}_{comune}.pdf")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(Template(tpl_str).render(
            comune=comune, profilo=profilo, data=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            root=root, esito="CONFORME" if n_err==0 else "NON CONFORME",
            n_errori=n_err, n_warning=n_war, sezioni=sezioni
        ))

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(pdf_path)
    except Exception:
        pdf_path = None

    return pdf_path or html_path