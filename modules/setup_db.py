__author__ = 'dex'

import re, os, gzip, pandas as pd, subprocess, sys
from cStringIO import StringIO


def uniCat(fields, fieldSep="|\n|"):
    "Unique concatenation of "
    return fieldSep.join(fields.astype(str).unique())


def walkFiles(dir, endswith=".gz"):
    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, filenames in os.walk(dir):
        for filename in filenames:
            if filename.endswith(endswith): yield os.path.join(root, filename)


def getCleanColumns(columns):
    p = re.compile('[\W_]+')
    clean = [p.sub('_', col).lower() for col in columns]
    return clean


def getDfFromStream(stream):
    stream.seek(0)
    index_col = '%s_title' % entity
    # open("sample.txt", "wb").write(stream.getvalue())
    df = pd.io.parsers.read_table(stream) \
        .dropna(how='all') \
        .groupby(index_col) \
        .aggregate(uniCat) \
        .T
    df.index.attribute_name = index_col
    df.columns = getCleanColumns(df.columns)
    return df


def create_unique_indices_on_postgres(indices):
    # Modified from http://osdir.com/ml/web2py/2010-09/msg00952.html
    '''Creates a set of indices if they do not exist'''
    # # Edit this list of table columns to index
    # # The format is [('table', 'column1, column2, ...'),...]
    print "checking indicies:"
    for table, columns in indices:
        relname = "%s_%s_idx" % (table.replace("_", ""),
                                 columns.replace(" ", "") \
                                 .replace("_", "") \
                                 .replace(",", "_"))
        print "\t%s" % relname,
        index_exists = \
            db.executesql("select count(*) from pg_class where relname = '%s';" % relname)[0][0] == 1
        if not index_exists:
            print "CREATING"
            db.executesql('create unique index %s on %s (%s);'
                          % (relname, table, columns))
        db.commit()
        print "OK"
    print "Done!"


def create_indices_on_postgres(indices, unique=True):
    # Modified from http://osdir.com/ml/web2py/2010-09/msg00952.html
    '''Creates a set of indices if they do not exist'''
    # # Edit this list of table columns to index
    # # The format is [('table', 'column1, column2, ...'),...]
    print "checking indicies:"
    for table, columns in indices:
        relname = "%s_%s_idx" % (table.replace("_", ""),
                                 columns.replace(" ", "") \
                                 .replace("_", "") \
                                 .replace(",", "_"))
        print "\t%s" % relname,
        index_exists = \
            db.executesql("select count(*) from pg_class where relname = '%s';" % relname)[0][0] == 1
        if not index_exists:
            print "CREATING"
            sql = 'create' \
                  + (' unique' if unique else '') \
                  + ' index %s on %s (%s);' % (relname, table, columns)
            print sql
            db.executesql(sql)
            db.commit()
    print "OK"


print "Done!"


def getEntity2stream(filename, entities):
    entity2stream = {}
    try:
        for line in gzip.open(filename, 'rb'):
            for entity in entities:
                if entity not in entity2stream:
                    entity2stream[entity] = StringIO()
                header = "!%s_" % entity
                if line.startswith(header):
                    print >> entity2stream[entity], line[len("!"):]
    except IOError:
        f = open("errors.txt", "a")
        print >> f, "IOError in %s" % filename
        f.close()

    return entity2stream


def getSampleCrossTab():
    print "reading attribute names"
    attribute_names = db().select(Sample_Attribute.attribute_name,
                                  orderby=Sample_Attribute.attribute_name,
                                  distinct=True)
    # CREATE VIEW
    print "creating view"
    attributesSql = "," \
        .join(["""string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '|||') as %s \n""" \
               % (row['attribute_name'], row['attribute_name'])
               for row in attribute_names])
    sql = """CREATE SEQUENCE sample_attribute_sequence;
             CREATE MATERIALIZED VIEW sample_view AS
             SELECT NEXTVAL('sample_attribute_sequence') as id, series_id, platform_id, sample_id,""" \
          + attributesSql \
          + """ FROM sample
                 JOIN sample_attribute ON sample.id = sample_id
                 JOIN series ON series.id = series_id
                 JOIN platform ON platform.id = platform_id
               GROUP BY series_id, platform_id, sample_id;
               DROP SEQUENCE sample_attribute_sequence;"""
    print sql
    db.executesql(sql)

    # CREATE FTS
    print "creating FTS"
    attributesSql = "||" \
        .join(["""to_tsvector(coalesce(%s, ''))""" % row['attribute_name']
               for row in attribute_names])
    sql = """CREATE MATERIALIZED VIEW sample_view_fts AS
                SELECT sample_view.id,
                    (
                      to_tsvector(gse_name)||%s
                    ) AS doc
                FROM sample_view
                JOIN series ON series_id = series.id;""" % attributesSql
    print sql
    db.executesql(sql)

    print "indexing FTS"
    sql = "CREATE INDEX sample_view_fts_idx ON sample_view_fts USING gin(doc);"
    print sql
    db.executesql(sql)

    db.executesql("""
        CREATE INDEX idx_fts_sample_atribute ON sample_attribute
        USING GIN ((
          to_tsvector('english', attribute_name) ||
          to_tsvector('english', attribute_value)
        ));
    """)

    db.commit()


def getSeriesCrossTab():
    print "reading attribute names"
    attribute_names = db().select(Series_Attribute.attribute_name,
                                  orderby=Series_Attribute.attribute_name,
                                  distinct=True)
    print "creating view"
    sql = 'CREATE SEQUENCE series_attribute_sequence;'
    print sql
    db.executesql(sql)
    attributesSql = "," \
        .join(["""string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '|||') as %s \n""" \
               % (row['attribute_name'], row['attribute_name'])
               for row in attribute_names])
    sql = """CREATE MATERIALIZED VIEW series_view AS
             SELECT NEXTVAL('series_attribute_sequence') as id, series_id, """ \
          + attributesSql \
          + """ FROM series_attribute
               GROUP BY series_id;"""
    print sql
    db.executesql(sql)

    print "creating FTS"
    # CREATE FTS
    attributesSql = "||" \
        .join(["""to_tsvector(coalesce(%s, ''))""" % row['attribute_name']
               for row in attribute_names])
    sql = """CREATE MATERIALIZED VIEW series_view_fts AS
                SELECT series_view.id,
                    (
                      to_tsvector(gse_name)||%s
                    ) AS doc
                FROM series_view
                JOIN series ON series_id = series.id;""" % attributesSql
    print sql
    db.executesql(sql)

    print "indexing FTS"
    sql = "CREATE INDEX series_view_fts_idx ON series_view_fts USING gin(doc);"
    print sql
    db.executesql(sql)

    db.executesql("""
        CREATE INDEX idx_fts_series_atribute ON series_attribute
        USING GIN ((
          to_tsvector('english', attribute_name) ||
          to_tsvector('english', attribute_value)
        ));
    """)
    db.commit()


if __name__ == '__main__':

    # create_indices_on_postgres([('sample_attribute', 'sample_id, attribute_name')])
    # create_indices_on_postgres([('sample_view', 'sample_id')], unique=False)
    getSeriesCrossTab()
    1 / 0
    getSampleCrossTab()
    1 / 0

    isLast = lastGse = False
    row = db.executesql("select gse_name from series order by id desc limit 1;")
    if row:
        lastGse = row[0][0]
    # for filename in walkFiles("/Users/dex/geo_mirror/DATA/SeriesMatrix"):
    for filename in walkFiles("geo_mirror/DATA/SeriesMatrix"):
        basename = os.path.basename(filename)
        gse_name = basename.split("_")[0].split("-")[0]
        print basename
        if (gse_name == lastGse) or not lastGse:
            isLast = True
        if not isLast:
            print "\tskipping"
            continue

        entities = "Series Platform Sample".split()
        entity2stream = getEntity2stream(filename, entities)

        # print "\tbuilding series"
        series = getDfFromStream(entity2stream['Series'])
        # series.to_csv('series.csv')
        if ('series_type' not in series.columns) or (series.series_type != "Expression profiling by array").any():
            # print "skipping for", ('type' in series.columns) and series.type
            continue
        if ('series_platform_taxid' not in series.columns) or (series.series_platform_taxid != '9606').any():
            # print "skipping for", ('platform_taxid' in series.columns) and series.platform_taxid
            continue
        assert len(series.index) == 1
        series_rec = db(Series.gse_name == gse_name).select().first() \
                     or Series(Series.insert(gse_name=gse_name))
        for attribute_name in series.columns:
            attribute_value = uniCat(series[attribute_name])
            # attribute_name = attribute_name.replace("Series_", "")
            series_attribute_rec = db((Series_Attribute.series_id == series_rec.id) & \
                                      (Series_Attribute.attribute_name == attribute_name)).select().first() \
                                   or Series_Attribute(Series_Attribute.insert(series_id=series_rec.id,
                                                                               attribute_name=attribute_name,
                                                                               attribute_value=attribute_value))

        # print "\tbuilding samples",
        samples = getDfFromStream(entity2stream['Sample'])
        gpls = samples['sample_platform_id'].unique()
        assert len(gpls) == 1
        gpl_name = gpls[0]
        print "\t", gpl_name
        platform_rec = db(Platform.gpl_name == gpl_name).select().first() \
                       or Platform(Platform.insert(gpl_name=gpl_name))

        samples['gsm_name'] = samples.sample_geo_accession
        samples = samples.set_index('gsm_name')
        for gsm_name in samples.index:
            sample_rec = db((Sample.gsm_name == gsm_name) & \
                            (Sample.series_id == series_rec.id) & \
                            (Sample.platform_id == platform_rec.id)).select().first() \
                         or Sample(Sample.insert(gsm_name=gsm_name,
                                                 series_id=series_rec.id,
                                                 platform_id=platform_rec.id))
            attribute_name2value = samples.ix[gsm_name].to_dict()
            for attribute_name in attribute_name2value:
                attribute_value = attribute_name2value[attribute_name].strip()
                # if attribute_value:
                # if type(attribute_value) == str:
                if attribute_value:
                    attribute_value = attribute_value.decode('utf8').encode('utf8')  # weir UTF problem with GSE31631
                    # attribute_name = attribute_name.replace("Sample_", "")
                    sample_attribute_rec = db((Sample_Attribute.sample_id == sample_rec.id) & \
                                              (Sample_Attribute.attribute_name == attribute_name)).select().first() \
                                           or Sample_Attribute(Sample_Attribute.insert(sample_id=sample_rec.id,
                                                                                       attribute_name=attribute_name,
                                                                                       attribute_value=attribute_value))
        db.commit()


