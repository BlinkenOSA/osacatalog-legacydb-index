#!/usr/bin/env bash
/opt/python/bin/python remove_fonds.py
/opt/python/bin/python import_fonds.py
/opt/python/bin/python import_subfonds.py
/opt/python/bin/python import_series.py
/opt/python/bin/python import_contentsTXT.py
/opt/python/bin/python import_contentsAV.py
/opt/python/bin/python import_contentsER.py
