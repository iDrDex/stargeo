__author__ = 'dex'

from geo_pipeline.Gse import *
import glob, os, re

def create_search_index():
    db.executesql("""
    DROP MATERIALIZED VIEW sample_attribute_search;
    CREATE MATERIALIZED VIEW sample_attribute_search AS
      SELECT
        sample_attribute.id,
        sample_id,
        series_id,
        platform_id,
        gse_name,
        gpl_name,
        gsm_name,
        name,
        value,
        to_tsvector('simple', gse_name) ||
        to_tsvector('simple', gpl_name) ||
        to_tsvector('simple', gsm_name) ||
        to_tsvector('simple', name) ||
        to_tsvector('simple', value) AS doc
      FROM
        sample_attribute
        JOIN sample ON sample.id = sample_id
        JOIN series ON series.id = series_id
        JOIN platform ON platform.id = platform_id;
    CREATE INDEX idx_fts_sample_atribute ON sample_attribute_search USING GIN (doc);

    REFRESH MATERIALIZED VIEW sample_attribute_search;
    """)

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


if __name__ == '__main__':
    create_unique_indices_on_postgres([('sample_attribute', 'sample_id, name')])

    filenames = sorted(glob.glob('geo_mirror/DATA/SeriesMatrix/*'))
    length = len(filenames)

    filenames = sorted(glob.glob("csvs/GSE*.series.csv"))
    # filenames = ["csvs/GSE4482.series.csv"]
    for i, filename in enumerate(filenames):
        # gse_name = os.path.basename(filename)
        gse_name = os.path.basename(filename)[:-11]
        print i, "/", len(filenames), gse_name
        if db(Series.gse_name == gse_name).select().first():
            continue

        gse = Gse(gse_name, doData=False, doSamples=False)
        if ('Series_type' not in gse.series.columns) or (gse.series.Series_type != "Expression profiling by array").any():
            continue
        if ('Series_sample_taxid' not in gse.series.columns) or (gse.series.Series_sample_taxid != 9606).any():
            continue

        print "\tbuilding series"
        series_rec = db(Series.gse_name == gse_name).select().first() \
                     or Series(Series.insert(gse_name=gse_name))
        for name in gse.series.columns:
            value = uniCat(gse.series[name])
            name = name.replace("Series_", "")
            series_attribute_rec = db((Series_Attribute.series_id == series_rec.id) & \
                                      (Series_Attribute.name == name)).select().first() \
                                   or Series_Attribute(Series_Attribute.insert(series_id=series_rec.id,
                                                                               name=name,
                                                                               value=value))
        print "\tbuilding samples"
        gse = Gse(gse_name, doData=False, sep=None)

        for gsm_name in gse.samples.index:
            # print gsm_name
            gpl_name = gse.samples.ix[gsm_name].Sample_platform_id
            platform_rec = db(Platform.gpl_name == gpl_name).select().first() \
                           or Platform(Platform.insert(gpl_name=gpl_name))

            sample_rec = db((Sample.gsm_name == gsm_name) & \
                            (Sample.series_id == series_rec.id) & \
                            (Sample.platform_id == platform_rec.id)).select().first() \
                         or Sample(Sample.insert(gsm_name=gsm_name,
                                                 series_id=series_rec.id,
                                                 platform_id=platform_rec.id))
            attribute_name2value = gse.samples.ix[gsm_name].to_dict()
            for name in attribute_name2value:
                value = attribute_name2value[name]
                if type(value) == str:
                    value = value.decode('utf8').encode('utf8')  # weir UTF problem with GSE31631
                name = name.replace("Sample_", "")

                sample_attribute_rec = db((Sample_Attribute.sample_id == sample_rec.id) & \
                                          (Sample_Attribute.name == name)).select().first() \
                                       or Sample_Attribute(Sample_Attribute.insert(sample_id=sample_rec.id,
                                                                                   name=name,
                                                                                   value=value))

        db.commit()
