__author__ = 'dex'

import re, os, gzip, pandas as pd, subprocess, sys
from cStringIO import StringIO


def uni_cat(fields, fieldSep="|\n|"):
    "Unique concatenation of "
    return fieldSep.join(fields.astype(str).unique())


def walk_files(dir, endswith=".gz"):
    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, filenames in os.walk(dir):
        for filename in filenames:
            if filename.endswith(endswith): yield os.path.join(root, filename)


def get_clean_columns(columns):
    p = re.compile('[\W_]+')
    clean = [p.sub('_', col).lower() for col in columns]
    return clean


def get_df_from_stream(stream):
    stream.seek(0)
    index_col = '%s_title' % entity
    # open("sample.txt", "wb").write(stream.getvalue())
    df = pd.io.parsers.read_table(stream) \
        .dropna(how='all') \
        .groupby(index_col) \
        .aggregate(uni_cat) \
        .T
    df.index.attribute_name = index_col
    df.columns = get_clean_columns(df.columns)
    return df


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


def get_entity2stream(filename, entities):
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


def get_sample_cross_tab():
    print "reading attribute names"
    attribute_names = db().select(Sample_Attribute.attribute_name,
                                  orderby=Sample_Attribute.attribute_name,
                                  distinct=True)
    # INDEX source attribute
    create_indices_on_postgres([('sample_attribute', 'attribute_name')], unique=False)  # for view cols
    create_indices_on_postgres([('sample_attribute', 'sample_id, attribute_name')])  # for aggregate function

    # CREATE VIEW
    print "creating view"
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS sample_view CASCADE;")
    db.executesql("DROP INDEX IF EXISTS sample_view_fts_idx CASCADE;")
    db.executesql("DROP SEQUENCE IF EXISTS sample_attribute_sequence CASCADE;")
    db.executesql("""CREATE SEQUENCE sample_attribute_sequence;""")
    attributesSql = "," \
        .join(["""string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///') as %s \n""" \
               % (row['attribute_name'], row['attribute_name'])
               for row in attribute_names])
    docSql = "to_tsvector('english', gse_name) || " + \
             "||".join([
                 """to_tsvector('english', coalesce(string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///'), ''))\n""" \
                 % (row['attribute_name'])
                 for row in attribute_names]) + " AS doc"

    sql = """CREATE MATERIALIZED VIEW sample_view AS
             SELECT NEXTVAL('sample_attribute_sequence') as id,
             %s,
             series_id, platform_id, sample_id,
             %s
             FROM sample
                 JOIN sample_attribute ON sample.id = sample_id
                 JOIN series ON series.id = series_id
                 JOIN platform ON platform.id = platform_id
               GROUP BY gse_name, series_id, platform_id, sample_id;""" % (docSql, attributesSql)
    print sql
    # db.executesql(sql)
    create_indices_on_postgres([('sample_view', 'id')])
    print "indexing FTS"
    sql = "CREATE INDEX sample_view_fts_idx ON sample_view USING gin(doc);"
    # print sql
    db.executesql(sql)

    # reset results
    Sample_View_Results.truncate()

    db.commit()


def get_series_cross_tab():
    print "reading attribute names"
    attribute_names = db().select(Series_Attribute.attribute_name,
                                  orderby=Series_Attribute.attribute_name,
                                  distinct=True)
    print "creating view"
    create_indices_on_postgres([('series_attribute', 'attribute_name')], unique=False)  # for view cols
    create_indices_on_postgres([('series_attribute', 'series_id, attribute_name')])  # for aggregate

    db.executesql("DROP MATERIALIZED VIEW IF EXISTS series_view CASCADE ;")
    db.executesql("DROP INDEX IF EXISTS series_view_fts_idx CASCADE ;")
    db.executesql("DROP SEQUENCE IF EXISTS series_attribute_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE series_attribute_sequence;")
    attributesSql = "," \
        .join(["""string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///') as %s \n""" \
               % (row['attribute_name'], row['attribute_name'])
               for row in attribute_names])
    docSql = "||" \
                 .join([
        """to_tsvector('english', coalesce(string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///'), ''))\n""" \
        % (row['attribute_name'])
        for row in attribute_names]) + " AS doc"

    sql = """CREATE MATERIALIZED VIEW series_view AS
             SELECT NEXTVAL('series_attribute_sequence') as id,
             %s,
             series_id,
             %s
             FROM series_attribute
             GROUP BY series_id;""" % (docSql, attributesSql)
    # print sql
    db.executesql(sql)
    create_indices_on_postgres([('series_view', 'id')])
    print "indexing FTS"
    sql = "CREATE INDEX series_view_fts_idx ON series_view USING gin(doc);"
    # print sql
    db.executesql(sql)
    # reset results
    Series_View_Results.truncate()
    db.commit()


def get_sample_tag_cross_tab():
    print "reading tag names"
    rows = db(Tag).select(orderby=Tag.tag_name)

    # CREATE VIEW
    print "creating view"
    db.executesql("DROP SEQUENCE IF EXISTS sample_tag_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE sample_tag_sequence;")
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS sample_tag_view ;")

    create_indices_on_postgres([('sample_tag', 'sample_id, series_tag_id')])  # for aggregate function

    # print "creating view"
    tagsSql = "," \
        .join(["""string_agg(CASE tag_id WHEN %s THEN annotation END, '|||') as %s \n""" \
               % (row['id'], row['tag_name'])
               for row in rows])

    docSql = """to_tsvector('english', gse_name) ||
                to_tsvector('english', gpl_name) ||
                to_tsvector('english', gsm_name) || """ + \
             "||".join([
                 """
                 to_tsvector('english', coalesce(string_agg(CASE tag_name WHEN '%s' THEN annotation || ' ' || tag_name ||  ' ' || description END, '///'), ''))
                 """ % (row['tag_name'])
                 for row in rows]) + " AS doc"
    #
    sql = """CREATE MATERIALIZED VIEW sample_tag_view AS
             SELECT NEXTVAL('sample_tag_sequence') as id, \
            %s,
             series.id as series_id,
             platform.id as platform_id,
             sample.id as sample_id,
             %s

             FROM sample_tag
                 JOIN series_tag ON sample_tag.series_tag_id = series_tag.id
                 JOIN tag ON tag.id = tag_id
                 JOIN series ON series.id = series_tag.series_id
                 JOIN platform ON platform.id = platform_id
                 JOIN sample ON sample.id = sample_id
               GROUP BY gse_name, gpl_name, gsm_name, series.id, platform.id, sample.id;""" % (docSql, tagsSql)
    print sql
    db.executesql(sql)
    print "indexing FTS"
    sql = "CREATE INDEX sample_tag_view_fts_idx ON sample_tag_view USING gin(doc);"
    # print sql
    db.executesql(sql)

    get_series_tag_cross_tab()
    # reset results
    Sample_Tag_View_Results.truncate()





    db.commit()
    session.all_sample_tag_names = None


def get_series_tag_cross_tab():
    print "reading tag names"
    rows = db().select(Tag.tag_name,
                       orderby=Tag.tag_name,
                       distinct=True)
    # CREATE VIEW
    print "creating view"
    db.executesql("DROP SEQUENCE IF EXISTS series_tag_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE series_tag_sequence;")
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS series_tag_view ;")

    db.executesql(
        """
        CREATE OR REPLACE FUNCTION concat_tsvectors(tsv1 TSVECTOR, tsv2 TSVECTOR)
          RETURNS TSVECTOR AS $$
        BEGIN
          RETURN coalesce(tsv1, to_tsvector('english', ''))
                 || coalesce(tsv2, to_tsvector('english', ''));
        END;
        $$ LANGUAGE plpgsql;
        """)

    db.executesql("DROP AGGREGATE IF EXISTS tsvector_agg( TSVECTOR );")

    db.executesql("""
            CREATE AGGREGATE tsvector_agg (
            BASETYPE = TSVECTOR,
            SFUNC = concat_tsvectors,
            STYPE = TSVECTOR,
            INITCOND = ''
            );"""
    )
    tagsSql = "," \
        .join(["""count(%s) as %s \n""" \
               % (row['tag_name'], row['tag_name'])
               for row in rows])
    sql = """CREATE MATERIALIZED VIEW series_tag_view AS
             SELECT NEXTVAL('series_tag_sequence') as id,
             series_id, platform_id,
             tsvector_agg(doc) as doc,
             count(*) as samples,""" \
          + tagsSql \
          + """ FROM sample_tag_view
               GROUP BY series_id, platform_id;"""
    print sql
    db.executesql(sql)
    print "indexing FTS"
    sql = "CREATE INDEX series_tag_view_fts_idx ON series_tag_view USING gin(doc);"
    # print sql
    # reset results
    Series_Tag_View_Results.truncate()

    db.executesql(sql)


def insert_attributes():
    isLast = lastGse = False
    row = db.executesql("select gse_name from series order by id desc limit 1;")
    if row:
        lastGse = row[0][0]
    # for filename in walk_files("/Users/dex/geo_mirror/DATA/SeriesMatrix"):
    for filename in walk_files("geo_mirror/DATA/SeriesMatrix"):
        basename = os.path.basename(filename)
        gse_name = basename.split("_")[0].split("-")[0]
        print basename
        if (gse_name == lastGse) or not lastGse:
            isLast = True
        if not isLast:
            print "\tskipping"
            continue

        entities = "Series Platform Sample".split()
        entity2stream = get_entity2stream(filename, entities)

        # print "\tbuilding series"
        series = get_df_from_stream(entity2stream['Series'])
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
            attribute_value = uni_cat(series[attribute_name])
            # attribute_name = attribute_name.replace("Series_", "")
            series_attribute_rec = db((Series_Attribute.series_id == series_rec.id) & \
                                      (Series_Attribute.attribute_name == attribute_name)).select().first() \
                                   or Series_Attribute(Series_Attribute.insert(series_id=series_rec.id,
                                                                               attribute_name=attribute_name,
                                                                               attribute_value=attribute_value))

        # print "\tbuilding samples",
        samples = get_df_from_stream(entity2stream['Sample'])
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


if __name__ == '__main__':
    # get_series_cross_tab()
    get_sample_cross_tab()
    # get_sample_tag_cross_tab()

