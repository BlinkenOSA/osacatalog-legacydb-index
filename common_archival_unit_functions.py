# coding=utf-8
from hashids import Hashids
from common_config import con


def get_series_name(fonds, subfonds, series):
    series_name = []
    sql = '''SELECT fonds.`Name` AS fondsName,
    subfonds.`Name` AS subfondsName,
    series.`Name` AS seriesName
    FROM series INNER JOIN subfonds ON series.SubfondsID = subfonds.ID AND series.FondsID = subfonds.FondsID
    INNER JOIN fonds ON subfonds.FondsID = fonds.ID
    WHERE series.FondsID = %s AND series.SubfondsID = %s AND series.ID = %s'''

    cursor = con.cursor(buffered=True)
    cursor.execute(sql % (fonds, subfonds, series))

    for row in cursor:
        if subfonds == 0:
            series_name.append("HU OSA " + str(fonds) + "-" + "0" + "-" + str(series) + " " + row[0] + ": " + row[2])
        else:
            series_name.append("HU OSA " + str(fonds) + "-" + str(subfonds) + "-" + str(series) + " " + row[0] + ": " +
                               row[1] + ": " + row[2])

    return series_name[0]


def get_series_id(fonds, subfonds, series):
    hashids = Hashids(salt="osaarchives", min_length=8)
    return hashids.encode(fonds * 1000000 + subfonds * 1000 + series)


def make_date_created_display(row):
    if row["YearFrom"] > 0:
        date = str(row["YearFrom"])

        if row["YearTo"]:
            if row["YearFrom"] != row["YearTo"]:
                date = date + " - " + str(row["YearTo"])
    else:
        date = ""

    return date


def make_date_created_search(row):
    date = []

    if row["YearFrom"] > 0:
        year_from = row["YearFrom"]

        if row["YearTo"]:
            year_to = row["YearTo"]
            for year in xrange(year_from, year_to + 1):
                date.append(year)
        else:
            date.append(str(year_from))

    return date


def select_isaar(fonds_id, subfonds_id, series_id):
    isaar = []
    cursor = con.cursor(buffered=True)

    if fonds_id is not None and subfonds_id is None and series_id is None:
        sql = '''SELECT `Authority Entry` FROM isaar WHERE FondsID = %s AND SubfondsID IS NULL AND SeriesID IS NULL'''
        cursor.execute(sql % fonds_id)
    elif fonds_id is not None and subfonds_id is not None and series_id is None:
        sql = '''SELECT `Authority Entry` FROM isaar WHERE FondsID = %s AND SubfondsID =%s AND SeriesID IS NULL'''
        cursor.execute(sql % (fonds_id, subfonds_id))
    else:
        sql = '''SELECT `Authority Entry` FROM isaar WHERE FondsID = %s AND SubfondsID =%s AND SeriesID =%s'''
        cursor.execute(sql % (fonds_id, subfonds_id, series_id))

    for row in cursor:
        isaar.append(row[0])

    return isaar


def select_languages(isad_id, lang='en'):
    languages = []

    if lang == 'en':
        lng = 'languages.`Language`, '
    else:
        lng = 'languages.`LanguageHUN`, '

    sql = '''SELECT ''' + lng + '''isad.Id
             FROM isad INNER JOIN languagesinisad ON isad.Id = languagesinisad.IsadId
             INNER JOIN languages ON languagesinisad.LanguageId = languages.ID
             WHERE isad.Id = %s
             ORDER BY isad.Id'''

    cursor = con.cursor(buffered=True)
    cursor.execute(sql % isad_id)

    for row in cursor:
        languages.append(row[0])

    return languages


def select_related_units(isad_id):
    hashids = Hashids(salt="osaarchives", min_length=8)
    related_units = []

    # Fonds
    sql = '''SELECT isadinisad.PrimaryIsadId,
            fonds.`Name`,
            fonds.ID
            FROM isadinisad INNER JOIN fonds ON isadinisad.RelatedIsadId = fonds.IsadId
            WHERE isadinisad.PrimaryIsadId = %s'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % isad_id)

    for row in cursor:
        related_id = hashids.encode(row["ID"] * 1000000)
        related_units.append({"url": related_id, "name": "HU OSA " + str(row["ID"]) + " " + row["Name"]})

    # Subfonds
    sql = '''SELECT subfonds.`Name`,
            subfonds.FondsID,
            subfonds.ID
            FROM isadinisad INNER JOIN subfonds ON isadinisad.RelatedIsadId = subfonds.IsadId
            WHERE isadinisad.PrimaryIsadId = %s'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % isad_id)

    for row in cursor:
        related_id = hashids.encode(row["FondsID"] * 1000000 + row["ID"] * 1000)
        if row["ID"] != 0:
            related_units.append(
                {"url": related_id, "name": "HU OSA " + str(row["FondsID"]) + "-" + str(row["ID"]) + " " + row["Name"]})

    # Series
    sql = '''SELECT series.FondsID,
            series.SubfondsID,
            series.ID,
            series.`Name`
            FROM isadinisad INNER JOIN series ON isadinisad.PrimaryIsadId = series.IsadId
            WHERE isadinisad.PrimaryIsadId = %s'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % isad_id)

    for row in cursor:
        related_id = hashids.encode(row["FondsID"] * 1000000 + row["SubfondsID"] * 1000 + row["ID"])
        related_units.append({"url": related_id,
                              "name": "HU OSA " + str(row["FondsID"]) + "-" + str(row["SubfondsID"]) + " " + str(
                                  row["ID"]) + " " + row["Name"]})

    return related_units


def select_fonds_name(fonds_id):
    name = ""
    sql = '''SELECT Name FROM fonds WHERE ID = %s'''

    cursor = con.cursor(buffered=True)
    cursor.execute(sql % fonds_id)

    for row in cursor:
        name = "HU OSA " + str(fonds_id) + ' ' + row[0]

    return name


def select_subfonds_name(fonds_id, subfonds_id):
    name = ""
    sql = '''SELECT Name FROM subfonds WHERE FondsID = %s AND ID = %s'''

    cursor = con.cursor(buffered=True)
    cursor.execute(sql % (fonds_id, subfonds_id))

    for row in cursor:
        if row[0]:
            name = "HU OSA " + str(fonds_id) + '-' + str(subfonds_id) + ' ' + row[0]

    return name


def select_themes(fonds):
    themes = []
    sql = '''SELECT CommunismAndColdWar, HumanRights, SorosInstitution FROM fonds WHERE fonds.ID = %s'''

    cursor = con.cursor(dictionary=True, buffered=True)
    cursor.execute(sql % fonds)

    for row in cursor:
        if row["CommunismAndColdWar"]:
            themes.append("Communism and Cold War")
        if row["HumanRights"]:
            themes.append("Human Rights")
        if row["SorosInstitution"]:
            themes.append("Civil Society")

    return themes


def select_extent(fonds_id, subfonds_id, series_id, lang='en'):
    extent = []
    total = 0

    if lang == 'en':
        dscr = 'container.Description, '
    else:
        dscr = 'container.DescriptionHUN, '

    cursor = con.cursor(dictionary=True, buffered=True)

    # Fonds
    if fonds_id is not None and subfonds_id is None and series_id is None:
        sql = '''SELECT fonds.ID, ''' + dscr + \
              '''COUNT(container.ID) AS ContainerCount,
                 ROUND(SUM(container.W)/1000, 2) AS LinearMeter
                 FROM fonds INNER JOIN main ON fonds.ID = main.FondsID
                 INNER JOIN container ON main.ContType = container.ID
                 WHERE fonds.ID = %s
                 GROUP BY main.ContType'''
        cursor.execute(sql % fonds_id)
    # Subfonds
    elif fonds_id is not None and subfonds_id is not None and series_id is None:
        sql = '''SELECT ''' + dscr + \
              '''COUNT(container.ID) AS ContainerCount,
                 ROUND(SUM(container.W)/1000, 2) AS LinearMeter
                 FROM subfonds INNER JOIN main ON subfonds.FondsID = main.FondsID AND
                                                  subfonds.ID = main.SubfondsID
                 INNER JOIN container ON main.ContType = container.ID
                 WHERE subfonds.FondsID = %s AND subfonds.ID = %s
                 GROUP BY main.ContType'''
        cursor.execute(sql % (fonds_id, subfonds_id))
    # Series
    else:
        sql = '''SELECT ''' + dscr + \
              '''COUNT(container.ID) AS ContainerCount,
                 ROUND(SUM(container.W)/1000, 2) AS LinearMeter
                 FROM series INNER JOIN main ON series.FondsID = main.FondsID AND
                                                series.SubfondsID = main.SubfondsID AND
                                                series.ID = main.SeriesID
                 INNER JOIN container ON main.ContType = container.ID
                 WHERE series.FondsID = %s AND series.SubfondsID = %s AND series.ID = %s
                 GROUP BY main.ContType'''
        cursor.execute(sql % (fonds_id, subfonds_id, series_id))

    for row in cursor:
        if lang == 'en':
            extent.append(str(row["ContainerCount"]) + ' ' + row["Description"] + ', ' + str(row["LinearMeter"]) +
                          ' linear meters')
        else:
            extent.append(str(row["ContainerCount"]) + ' ' + row["DescriptionHUN"] + ', ' + str(row["LinearMeter"]) +
                          u' folyóméter')

        total += row["LinearMeter"]

    if lang == 'en':
        extent.append("Total: " + str(total) + ' linear meters')
    else:
        extent.append("Total: " + str(total) + u' folyóméter')

    return extent
