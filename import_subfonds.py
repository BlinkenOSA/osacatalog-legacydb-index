# coding=utf-8
import argparse
import sys
from hashids import Hashids
import simplejson as json
from common_config import con, solr_interface
from common_archival_unit_functions import select_isaar, select_languages, select_related_units, select_themes, \
                                           select_fonds_name, select_extent, make_date_created_search, make_date_created_display


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fonds", help="Number of fonds")
    args = parser.parse_args()
    args_fonds = args.fonds

    sql_string = ''

    if args_fonds:
        sql_string = ' AND subfonds.FondsID = ' + args_fonds

    sql = '''
    SELECT * FROM subfonds INNER JOIN isad ON subfonds.IsadId = isad.Id
    WHERE DatePublic IS NOT NULL %s
    ORDER BY FondsId, subfonds.ID
    ''' % sql_string

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql)

    for row in cursor:
        doc = make_solr_document(row)
        solr_interface.add(doc)

    solr_interface.commit()
    solr_interface.optimize()

    print 'Import Subfonds - Finished!'


def make_json(row, lang='en'):
    creator = []
    hashids = Hashids(salt="osaarchives", min_length=8)
    j = {"id": hashids.encode(row["FondsID"] * 1000000 + row["ID"] * 1000),
         "referenceCode": "HU OSA " + str(row["FondsID"]) + '-' + str(row["ID"])}

    if lang == 'en':
        j["title"] = row["Name"]
    else:
        j["title"] = row["Name2"]

    j["descriptionLevel"] = "Subfonds"

    if row["YearFrom"] is not None:
        j["dateFrom"] = row["YearFrom"]

    if row["YearTo"] is not None:
        j["dateTo"] = row["YearTo"]

    if row["Date(s)"] is not None:
        j["datePredominant"] = row["Date(s)"]

    if row["Archival history"] is not None:
        j["archivalHistory"] = row["Archival history"]

    if row["Immediate source of acquisition or transfer"] is not None:
        j["sourceOfAcquisition"] = row["Immediate source of acquisition or transfer"]

    if row["Scope and content"] is not None:
        j["scopeAndContentNarrative"] = row["Scope and content"]

    if row["Appraisal, destruction and scheduling information"] is not None:
        j["appraisal"] = row["Appraisal, destruction and scheduling information"]

    if row["Accruals"] is not None:
        j["accruals"] = row["Accruals"]

    if row["Physical characteristics and technical requirements"] is not None:
        j["physicalCharacteristics"] = row["Physical characteristics and technical requirements"]

    if row["Publication note"] is not None:
        j["publicationNote"] = row["Publication note"]

    if row["Note"] is not None:
        j["note"] = row["Note"]

    if row["Archivist's Note"] is not None:
        j["archivistsNote"] = row["Archivist's Note"]

    if row["Extent and medium"] is not None:
        j["extent_estimated"] = row["Extent and medium"]

    values = select_extent(row["FondsID"], row["ID"], None, lang)
    if values:
        j["extent"] = values

    if row["Name of creator(s)"] is not None:
        creator.append(row["Name of creator(s)"])

    values = select_isaar(row["FondsID"], row["ID"], None)
    if values:
        for isaar in values:
            creator.append(isaar)

    if creator:
        j["creator"] = sorted(set(creator))

    values = select_languages(row["IsadId"], lang)
    if values:
        j["languages"] = values

    if row["Language/scripts of material"] is not None:
        j["languageStatement"] = row["Language/scripts of material"]

    if row["Conditions governing access"] is not None:
        j["rightsAccess"] = row["Conditions governing access"]

    if row["Conditions governing reproduction"] is not None:
        j["rightsReproduction"] = row["Conditions governing reproduction"]

    if row["Existence and location of originals"] is not None:
        loc = [{"url": "", "info": row["Existence and location of originals"]}]
        j["locationOfOriginals"] = loc

    if row["Existence and location of copies"] is not None:
        loc = [{"url": "", "info": row["Existence and location of copies"]}]
        j["locationOfCopies"] = loc

    if row["Related units of description"] is not None:
        related = [{"url": "", "info": row["Related units of description"]}]
        j["relatedUnits_textual"] = related

    values = select_related_units(row["IsadId"])
    if values:
        j["relatedUnits"] = values

    if row["Date(s) of descriptions"] is not None:
        j["descriptionNotes"] = row["Date(s) of descriptions"]

    return j


def make_json_hu(row):
    json_hu = {}
    sql = '''SELECT * FROM subfonds INNER JOIN isad2 ON subfonds.IsadId = isad2.Id
    WHERE DatePublic IS NOT NULL AND isad2.Id = %s
    ORDER BY subfonds.ID'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % str(row["Id"]))

    row_hu = cursor.fetchone()
    if row_hu is not None:
        json_hu = make_json(row_hu, "hu")

    return json_hu


def make_solr_document(row):
    hashids = Hashids(salt="osaarchives", min_length=8)
    j = make_json(row)
    j_hu = make_json_hu(row)
    identifier = hashids.encode(row["FondsID"] * 1000000 + row["ID"] * 1000)

    doc = {
        "id": identifier,
        "isad_json": json.dumps(j),
        "isad_json_hu": json.dumps(j_hu),

        "record_origin": "Archives",
        "record_origin_facet": "Archives",

        "archival_level": "Archival Unit",
        "archival_level_facet": "Archival Unit",

        "reference_code": j["referenceCode"],
        "reference_code_sort": j["referenceCode"],

        "title": j["title"],
        "title_e": json.dumps(j["title"])[1:-1],
        "title_search": j["title"],
        "title_sort": j["title"],

        "description_level": "Subfonds",
        "description_level_facet": "Subfonds",

        "fonds_sort": row["FondsID"],
        "subfonds_sort": row["ID"],
        "series_sort": 0,

        "date_created": make_date_created_display(row),
        "date_created_search": make_date_created_search(row),
        "date_created_facet": make_date_created_search(row),

        "fonds": row["FondsID"],
        "fonds_name": select_fonds_name(row["FondsID"]),

        "scope_and_content_narrative_search": row["Scope and content"],
        "archival_history_search": row["Archival history"],

        "primary_type": "Archival Unit",
        "primary_type_facet": "Archival Unit"
    }

    if "languages" in j.keys():
        doc["language"] = ", ".join(j["languages"])
        doc["language_facet"] = j["languages"]

    if "creator" in j.keys():
        doc["creator"] = ", ".join(j["creator"])
        doc["creator_facet"] = j["creator"]

    themes = select_themes(row["FondsID"])
    if themes:
        doc["archival_unit_theme"] = themes
        doc["archival_unit_theme_facet"] = themes

    if j_hu:
        if j_hu["title"]:
            doc['title_search_hu'] = j_hu["title"]
            doc['title_original'] = j_hu["title"]
            doc['title_original_e'] = json.dumps(j_hu["title"])[1:-1]
        else:
            doc['title_original_e'] = ""

        if "scopeAndContentNarrative" in j_hu.keys():
            doc["scope_and_content_narrative_search_hu"] = j_hu["scopeAndContentNarrative"]

        if "archivalHistory" in j_hu.keys():
            doc["archival_history_search_hu"] = j_hu["archivalHistory"]

        if "publicationNote" in j_hu.keys():
            doc["publication_note_search_hu"] = j_hu["publicationNote"]

    return doc

if __name__ == '__main__':
    sys.exit(main())
