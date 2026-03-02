import shapefile
def pad_zero(val, length):
    return str(val).zfill(length)

def fix_CL_AC(shp_path):
    sf = shapefile.Reader(shp_path)
    w = shapefile.Writer(shp_path)  # crea writer sovrascrivendo
    w.fields = sf.fields[1:]
    for rec in sf.records():
        # es. pad ID_infra a 10 cifre
        rec['ID_infra'] = pad_zero(rec['ID_infra'], 10)
        # ricompone ID_AC
        rec['ID_AC'] = rec['cod_prov'] + rec['cod_com'] + rec['ID_infra']
        w.record(*rec)
    for shape in sf.shapes():
        w.shape(shape)
    w.close()
