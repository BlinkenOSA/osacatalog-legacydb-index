# coding=utf-8
import sys
import re
from common_config import con

all_chars = (unichr(i) for i in xrange(sys.maxunicode))
control_chars = ''.join(map(unichr, range(0, 32) + range(127, 160)))
control_char_re = re.compile('[%s]' % re.escape(control_chars))


def make_date(year, month, day):
    output = ""

    if year > 0:
        output += str(year)

    if 0 < month < 13:
        output = output + '-' + str(month)
    else:
        return output

    if 0 < day < 32:
        output = output + '-' + str(day)
    else:
        return output

    return output


def make_date_created(from_date, to_date):
    output = ""
    if from_date != "":
        output = from_date
    else:
        return output

    if to_date != "":
        output = output + "-" + to_date
    else:
        return output

    return output


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
