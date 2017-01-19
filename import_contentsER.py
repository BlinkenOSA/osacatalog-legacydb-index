import argparse
import sys
from hashids import Hashids
import simplejson as json
import base64
from common_folder_funcitons import make_date, make_date_created, get_level
from common_archival_unit_functions import get_series_name, get_series_id
from common_config import con, solr_interface


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fonds", help="Number of fonds")
    args = parser.parse_args()
    args_fonds = args.fonds

    sql_string = ''

    if args_fonds:
        sql_string = ' AND main.FondsID = ' + args_fonds

    counter = 0

    sql = '''SELECT
    contentser.ContainerID,
    main.FondsID,
    main.SubfondsID,
    main.SeriesID,
    main.ListNo,
    main.Container AS ContainerNo,
    container.Description AS ContainerType,
    container.ID AS ContainerTypeID,
    contentser.`No` AS SequenceNo,
    contentser.Description,
    contentser.YearStart,
    contentser.MonthStart,
    contentser.DayStart,
    contentser.YearEnd,
    contentser.MonthEnd,
    contentser.DayEnd,
    contentser.Notes,
    contentser.Identifier
    FROM main INNER JOIN contentser ON main.ID = contentser.ContainerID
    INNER JOIN container ON main.ContType = container.ID
    INNER JOIN listsinseries ON main.FondsID = listsinseries.FondsId AND main.SubfondsID = listsinseries.SubfondsId AND
                                main.SeriesID = listsinseries.SeriesId AND main.ListNo = listsinseries.ListNo
    WHERE listsinseries.DatePublic IS NOT NULL
        AND contentser.Description IS NOT NULL
        AND CONCAT_WS("-", main.FondsID, main.SubfondsID, main.SeriesID) NOT IN
            (SELECT reference_code FROM exclude_series) %s''' % sql_string

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql)

    for row in cursor:
        doc = make_solr_document(row)
        solr_interface.add(doc)

        if counter % 1000 == 0:
            solr_interface.commit()
            print counter

        counter += 1

    solr_interface.commit()
    solr_interface.optimize()

    print 'Import ContentsER - Finished!'


def make_json(row):
    hashids = Hashids(salt="osacontent", min_length=8)
    j = {"id": hashids.encode(row["ContainerID"] * 1000 + int(row["SequenceNo"]))}

    if row["Description"] is not None:
        j["title"] = row["Description"]
    else:
        j["title"] = row["TranslatedTitle"]

    j["level"] = get_level(row)
    j["primaryType"] = "Electronic Record"

    j["containerNumber"] = row["ContainerNo"]
    j["containerType"] = row["ContainerType"]
    j["sequenceNumber"] = row["SequenceNo"]

    if make_date(row["YearStart"], row["MonthStart"], row["DayStart"]) != "":
        j["dateFrom"] = make_date(row["YearStart"], row["MonthStart"], row["DayStart"])

    if make_date(row["YearEnd"], row["MonthEnd"], row["DayEnd"]) != "":
        j["dateTo"] = make_date(row["YearEnd"], row["MonthEnd"], row["DayEnd"])

    if row["Notes"] is not None:
        j["note"] = row["Notes"]

    j["seriesReferenceCode"] = '-'.join((str(row["FondsID"]), str(row["SubfondsID"]), str(row["SeriesID"])))

    return j


def make_solr_document(row):
    hashids = Hashids(salt="osacontent", min_length=8)
    j = make_json(row)

    identifier = hashids.encode(row["ContainerID"] * 1000 + int(row["SequenceNo"]))

    doc = {
        "id": identifier,
        "item_json": json.dumps(j),
        "item_json_e": base64.b64encode(json.dumps(j)),

        "record_origin": "Archives",
        "record_origin_facet": "Archives",

        "archival_level": "Folder/Item",
        "archival_level_facet": "Folder/Item",

        "description_level": j["level"],
        "description_level_facet": j["level"],

        "title": j["title"],
        "title_e": json.dumps(j["title"])[1:-1],
        "title_search": j["title"],
        "title_sort": j["title"],

        "fonds_sort": row["FondsID"],
        "subfonds_sort": row["SubfondsID"],
        "series_sort": row["SeriesID"],

        "container_type": row["ContainerType"],
        "container_type_esort": row["ContainerTypeID"],

        "container_number": row["ContainerNo"],
        "container_number_sort": row["ContainerNo"],

        "sequence_number": row["SequenceNo"],
        "sequence_number_sort": row["SequenceNo"],

        "series_id": get_series_id(row["FondsID"], row["SubfondsID"], row["SeriesID"]),
        "series_name": get_series_name(row["FondsID"], row["SubfondsID"], row["SeriesID"]),
        "series_reference_code": j["seriesReferenceCode"],

        "contents_summary_search": row["Description"],

        "primary_type": "Electronic Record",
        "primary_type_facet": "Electronic Record"
    }

    if row["YearStart"] > 0:
        doc["date_created"] = row["YearStart"]
        doc["date_created_facet"] = row["YearStart"]

    cdate = make_date_created(make_date(row["YearStart"], row["MonthStart"], row["DayStart"]),
                              make_date(row["YearEnd"], row["MonthEnd"], row["DayEnd"]))
    if cdate != "":
        doc["creation_date"] = cdate

    return doc


if __name__ == '__main__':
    sys.exit(main())
