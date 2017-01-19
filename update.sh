#!/usr/bin/env bash
/opt/python/bin/python remove_fonds.py -f 350
/opt/python/bin/python import_fonds.py -f 350
/opt/python/bin/python import_subfonds.py -f 350
/opt/python/bin/python import_series.py -f 350
/opt/python/bin/python import_contentsTXT.py -f 350
/opt/python/bin/python import_contentsAV.py -f 350
/opt/python/bin/python import_contentsER.py -f 350
