# Daten entpacken von gz Datei

import gzip
import shutil
with gzip.open('name.basics.tsv.gz', 'rb') as f_in:
    with open('name.basics.tsv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
