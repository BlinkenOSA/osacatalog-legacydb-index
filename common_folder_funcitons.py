# coding=utf-8
import sys
import re
from common_config import con

all_chars = (unichr(i) for i in xrange(sys.maxunicode))
control_chars = ''.join(map(unichr, range(0, 32) + range(127, 160)))
control_char_re = re.compile('[%s]' % re.escape(control_chars))


def make_date_created_display(row):
    date = ""

    if row["CircaSpan"]:
        date += "ca. "

    if row["YearStart"] > 0:
        date += str(row["YearStart"])
        if 0 < row["MonthStart"] < 13:
            date += "-%02d" % (row["MonthStart"])
            if 0 < row["DayStart"] < 32:
                date += "-%02d" % (row["DayStart"])

    if row["YearEnd"] != row["YearStart"]:
        if row["YearEnd"] > 0:
            date += " - %02d" % (row["YearEnd"])
            if 0 < row["MonthEnd"] < 13:
                date += "-%02d" % (row["MonthEnd"])
                if 0 < row["MonthEnd"] < 32:
                    date += "-%02d" % (row["MonthEnd"])

    return date


def make_date_created_display_av(year, month, day, circa):
    date = ""

    if circa:
        date += "ca. "

    if year > 0:
        date += str(year)
        if 0 < month < 13:
            date += "-%02d" % (month)
            if 0 < day < 32:
                date += "-%02d" % (day)

    return date


def make_date_created_search(row):
    date = []

    if row["CircaSpan"]:
        circa = row["CircaSpan"]
    else:
        circa = 0

    if row["YearStart"] > 0:
        year_from = row["YearStart"]
    else:
        year_from = 0

    if row["YearEnd"] > 0:
        year_to = row["YearEnd"]
    else:
        year_to = year_from

    if circa == 0:
        if year_from != 0:
            if year_from < year_to:
                for year in xrange(year_from, year_to + 1):
                    date.append(year)
            else:
                for year in xrange(year_to, year_from + 1):
                    date.append(year)
    else:
        if circa > 5:
            circa = 5
        if year_from != 0:
            for year in xrange(year_from - circa, year_from + circa):
                date.append(year)

    return date


def make_date_created_search_av(year, circa):
    date = []

    if not circa:
        circa = 0

    if year <= 0:
        year = 0

    if circa == 0:
        if year != 0:
            date.append(year)
    else:
        if circa > 5:
            circa = 5
        if year != 0:
            for y in xrange(year - circa, year + circa):
                date.append(y)

    return date


def remove_control_chars(s):
    return control_char_re.sub('', s)


def get_level(row):
    level = []
    sql = '''SELECT `Item/Folder` FROM fa_series_level WHERE Fonds = %s AND Subfonds = %s AND Series = %s'''

    cursor = con.cursor(buffered=True)
    cursor.execute(sql % (row["FondsID"], row["SubfondsID"], row["SeriesID"]))

    for row in cursor:
        if row[0] == "":
            level.append("Folder")
        else:
            if row[0] == "I":
                level.append("Item")
            elif row[0] == "F":
                level.append("Folder")
            else:
                level.append("Folder")

    if not level:
        level.append("Item")

    return level[0]


def count_duration(hours, minutes, seconds, lang='eng'):
    if lang == 'eng':
        m = 'min.'
        s = 'sec.'
    else:
        m = 'p.'
        s = 'mp.'

    minutes += hours * 60

    if seconds > 0:
        if minutes > 0:
            return "%s %s %s %s" % (minutes, m, seconds, s)
        else:
            return "%s %s" % (seconds, s)
    else:
        if minutes > 0:
            return "%s %s" % (minutes, m)
        else:
            return None