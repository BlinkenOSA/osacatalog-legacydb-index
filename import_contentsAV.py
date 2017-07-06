import argparse
import sys
import base64
from hashids import Hashids
import simplejson as json
from common_folder_funcitons import make_date_created_display_av, make_date_created_search_av, remove_control_chars, \
    count_duration
from common_archival_unit_functions import get_series_id, get_series_name
from common_config import con, solr_interface


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fonds", help="Number of fonds")
    args = parser.parse_args()
    args_fonds = args.fonds

    if args_fonds:
        sql_string = ' AND main.FondsID = ' + args_fonds
    else:
        sql_string = ''

    counter = 0

    sql = '''SELECT
    main.FondsID,
    main.SubfondsID,
    main.SeriesID,
    main.ListNo,
    main.Container AS ContainerNo,
    container.Description AS ContainerType,
    container.ID AS ContainerTypeID,
    contentsav.ContainerID,
    contentsav.`No` AS SequenceNo,
    contentsav.Title,
    contentsav.Title2,
    contentsav.SequenceInformation,
    contentsav.YearAir,
    contentsav.MonthAir,
    contentsav.DayAir,
    contentsav.YearProduction,
    contentsav.MonthProduction,
    contentsav.DayProduction,
    contentsav.ProductionDateNotes,
    programtypes.ProgramType,
    contentsav.Director,
    contentsav.Copyright,
    contentsav.Producer,
    contentsav.LanguageID,
    contentsav.TimeStart,
    contentsav.TimeEnd,
    contentsav.Duration,
    contentsav.Description,
    contentsav.Notes,
    contentsav.Identifier,
    contentsav.IsRestricted,
    contentsav.Description2,
    contentsav.Notes2,
    contentsav.KW,
    contentsav.InternalNote,
    contentsav.TitleNeedsTransliteration,
    contentsav.TransliterationRule,
    contentsav.CircaSpan,
    countries.Country,
    languages.Language
    FROM main INNER JOIN contentsav ON main.ID = contentsav.ContainerID
    LEFT JOIN programtypes ON contentsav.ProgramTypeID = programtypes.ID
    LEFT JOIN countries ON contentsav.CountryId = countries.ID
    LEFT JOIN languages ON contentsav.LanguageID = languages.ID
    INNER JOIN container ON main.ContType = container.ID
    INNER JOIN listsinseries ON main.FondsID = listsinseries.FondsId AND main.SubfondsID = listsinseries.SubfondsId AND
               main.SeriesID = listsinseries.SeriesId AND main.ListNo = listsinseries.ListNo
    WHERE listsinseries.DatePublic IS NOT NULL
    AND CONCAT_WS("-", main.FondsID, main.SubfondsID, main.SeriesID) NOT IN
        (SELECT reference_code FROM exclude_series) %s''' % sql_string

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql)

    for row in cursor:
        doc = make_solr_document(row)
        try:
            solr_interface.add(doc)
        except ValueError:
            print doc

        if counter % 1000 == 0:
            solr_interface.commit()
            print counter

        counter += 1

    solr_interface.commit()
    solr_interface.optimize()
    print 'Import ContentsAV - Finished!'


def make_solr_document(row):
    hashids = Hashids(salt="osacontent", min_length=8)
    j_eng = make_json(row)
    j_2nd = make_json(row, lang='hun')

    item_json = {'item_json_eng': j_eng}
    if j_2nd:
        item_json['item_json_2nd'] = j_2nd

    identifier = hashids.encode(row["ContainerID"] * 1000 + int(row["SequenceNo"]))

    doc = {
        "id": identifier,
        "item_json": json.dumps(item_json),
        "item_json_e": base64.b64encode(json.dumps(item_json)),

        "record_origin": "Archives",
        "record_origin_facet": "Archives",

        "archival_level": "Folder/Item",
        "archival_level_facet": "Folder/Item",

        "description_level": "Item",
        "description_level_facet": "Item",

        "title": j_eng["title"],
        "title_e": json.dumps(j_eng["title"])[1:-1],
        "title_search": j_eng["title"].strip() if j_eng["title"] else None,
        "title_sort": j_eng["title"],

        "fonds_sort": row["FondsID"],
        "subfonds_sort": row["SubfondsID"],
        "series_sort": row["SeriesID"],

        "container_type": row["ContainerType"],
        "container_type_esort": row["ContainerTypeID"],

        "container_number": row["ContainerNo"],
        "container_number_sort": row["ContainerNo"],

        "contents_summary_search": remove_control_chars(row["Description"]) if row["Description"] else None,
        "contents_summary_search_hu": remove_control_chars(row["Description2"]) if row["Description2"] else None,

        "sequence_number": row["SequenceNo"],
        "sequence_number_sort": row["SequenceNo"],

        "series_id": get_series_id(row["FondsID"], row["SubfondsID"], row["SeriesID"]),
        "series_name": get_series_name(row["FondsID"], row["SubfondsID"], row["SeriesID"]),
        "series_reference_code": '-'.join((str(row["FondsID"]), str(row["SubfondsID"]), str(row["SeriesID"]))),

        "primary_type": "Moving Image",
        "primary_type_facet": "Moving Image"
    }

    date_created_display = make_date_created_display_av(row["YearProduction"], row["MonthProduction"], row["DayProduction"], row["CircaSpan"])
    if date_created_display != "":
        doc["date_created"] = date_created_display
    else:
        date_created_display = make_date_created_display_av(row["YearAir"], row["MonthAir"], row["DayAir"], 0)
        if date_created_display != "":
            doc["date_created"] = date_created_display

    date_created_search = make_date_created_search_av(row["YearProduction"], row["CircaSpan"])
    if date_created_search:
        doc["date_created_facet"] = date_created_search
        doc["date_created_search"] = date_created_search
    else:
        date_created_search = make_date_created_search_av(row["YearAir"], 0)
        if date_created_search != "":
            doc["date_created_facet"] = date_created_search
            doc["date_created_search"] = date_created_search

    if "duration" in j_eng:
        doc["duration"] = j_eng["duration"]

    if j_eng["contributors"]:
        for contributor in j_eng["contributors"]:
            if contributor["role"] == "Director" or contributor == "Creator/Author":
                doc["creator"] = contributor["name"]
                doc["creator_search"] = contributor["name"]
                doc["creator_unstem_search"] = contributor["name"]
                doc["creator_facet"] = contributor["name"]
                doc["director"] = contributor["name"]

    if "form_genre" in j_eng.keys():
        doc["genre_facet"] = j_eng["form_genre"]

    if j_eng["language"]:
        doc["language_facet"] = j_eng["language"]

    if "associatedCountry" in j_eng.keys():
        doc["associated_country_search"] = j_eng["associatedCountry"]
        doc["added_geo_facet"] = j_eng["associatedCountry"]

    if "titleOriginal" in j_eng.keys():
        doc["title_original"] = j_eng["titleOriginal"]
        doc["title_original_search"] = j_eng["titleOriginal"]

    return doc


def make_json(row, lang='eng'):
    if lang == 'eng':
        hashids = Hashids(salt="osacontentav", min_length=8)
        j = {"id": hashids.encode(row["ContainerID"] * 1000 + int(row["SequenceNo"]))}

        if row["Title"] is not None:
            if row["Title2"] is not None:
                title = row["Title2"]
                j["titleOriginal"] = row["Title"]
            else:
                title = row["Title"]
        else:
            title = row["Title2"]

        j["title"] = remove_control_chars(title)

        if row["SequenceInformation"]:
            j["title"] = row["SequenceInformation"] + ' ' + title

        j["level"] = "Item"
        j["primaryType"] = "Moving Image"

        j["containerNumber"] = row["ContainerNo"]
        j["containerType"] = row["ContainerType"]
        j["sequenceNumber"] = row["SequenceNo"]
        j["seriesReferenceCode"] = '-'.join((str(row["FondsID"]), str(row["SubfondsID"]), str(row["SeriesID"])))

        if make_date_created_display_av(row["YearProduction"], row["MonthProduction"], row["DayProduction"], row["CircaSpan"]) != "":
            j["dateCreated"] = make_date_created_display_av(row["YearProduction"], row["MonthProduction"], row["DayProduction"], row["CircaSpan"])

        if row["Notes"] is not None:
            j["note"] = row["Notes"]

        j["dates"] = []
        production_date = make_date_created_display_av(row["YearProduction"], row["MonthProduction"], row["DayProduction"], row["CircaSpan"])
        if production_date != "":
            j["dates"].append({"dateType": "Date of Production", "date": production_date})

        air_date = make_date_created_display_av(row["YearAir"], row["MonthAir"], row["DayAir"], 0)
        if air_date != "":
            j["dates"].append({"dateType": "Date Aired", "date": air_date})

        if row["ProgramType"] is not None:
            j["form_genre"] = row["ProgramType"]

        contributor = select_contributor(row["ContainerID"], row["ContainerNo"])
        if contributor is not None:
            j["contributors"] = contributor

        if row["Country"] is not None:
            j["associatedCountry"] = row["Country"]

        language = []
        if row["Language"] is not None:
            language.append(row["Language"])

        langs = select_languages(row["ContainerID"], row["ContainerNo"])
        if langs is not None:
            for l in langs:
                language.append(l)

        if language is not None:
            j["language"] = language

        language_statement = select_language_statement(row["ContainerID"], row["ContainerNo"])
        if language_statement is not None:
            j["languageStatement"] = "; ".join(language_statement)

        if row["Description"] is not None:
            j["contentsSummary"] = row["Description"]

        time_start = row["TimeStart"]
        if time_start is not None:
            j["timeStart"] = "%02d:%02d:%02d" % (time_start.hour, time_start.minute, time_start.second)

        time_end = row["TimeEnd"]
        if time_end is not None:
            j["timeEnd"] = "%02d:%02d:%02d" % (time_end.hour, time_end.minute, time_end.second)

        duration = row["Duration"]
        if duration is not None:
            j["duration"] = count_duration(duration.hour, duration.minute, duration.second, lang)

    else:
        j = {}

        if row["Description2"] is not None:
            j["metadataLanguage"] = "Hungarian"
            j["contentsSummary"] = row["Description2"]

            duration = row["Duration"]
            if duration is not None:
                d = count_duration(duration.hour, duration.minute, duration.second, lang)
                if d:
                    j["duration"] = d

    return j


def select_contributor(container, no):
    contributor = []

    sql = '''SELECT contentsav.Title,
    `contributors`.ContributorName,
    contributorroles.Role
    FROM contentsav INNER JOIN `contributors` ON contentsav.ContainerID = `contributors`.ContainerId AND
                                                 contentsav.`No` = `contributors`.EntryNo
    INNER JOIN contributorroles ON `contributors`.RoleId = contributorroles.Id
    WHERE contentsav.ContainerID = %s AND contentsav.No = %s'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % (container, no))

    for row in cursor:
        contributor.append({"name": row["ContributorName"], "role": row["Role"]})

    return contributor


def select_languages(container, no):
    lang = []

    sql = '''SELECT languages.Language
        FROM contentsav INNER JOIN languagesinentries ON contentsav.ContainerID = languagesinentries.ContainerId AND
                                                         contentsav.`No` = languagesinentries.EntryNo
        INNER JOIN languages ON languagesinentries.LanguageId = languages.ID
        WHERE languagesinentries.UseId = 1 AND contentsav.ContainerID = %s AND contentsav.No = %s'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % (container, no))

    for row in cursor:
        lang.append(row["Language"])

    return lang


def select_language_statement(container, no):
    ls = []

    sql = '''SELECT CONCAT(LanguageUse, " in ", GROUP_CONCAT(Language SEPARATOR ', ')) AS lang_statement
        FROM contentsav INNER JOIN languagesinentries ON contentsav.ContainerID = languagesinentries.ContainerId AND
                                                         contentsav.`No` = languagesinentries.EntryNo
        INNER JOIN languages ON languagesinentries.LanguageId = languages.ID
        INNER JOIN osalanguageuses ON languagesinentries.UseId = osalanguageuses.Id
        WHERE languagesinentries.UseId <> 1 AND contentsav.ContainerID = %s AND contentsav.No = %s
        GROUP BY contentsav.ContainerID, contentsav.No, UseId, LanguageUse'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % (container, no))

    for row in cursor:
        ls.append(row["lang_statement"])

    return ls


if __name__ == '__main__':
    sys.exit(main())
