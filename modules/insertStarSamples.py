__author__ = 'dex'
from geo_pipeline.Gse import *
from psycopg2 import DataError
import glob, os, re

lastGseRec = db(Series).select().last()
lastGse = lastGseRec and lastGseRec.gse_name or None
toSkip = True
print lastGse
filenames = sorted(glob.glob('geo_mirror/DATA/SeriesMatrix/*'))
length = len(filenames)

# p = re.compile("GSE\d+")
# errors = set(p.findall(open('errors.err').read()))
# errorFile = open('errors.err', 'a')

filenames = sorted(glob.glob("csvs/GSE*.series.csv"))
# filenames = ["csvs/GSE4482.series.csv"]
for i, filename in enumerate(filenames):
    # gse_name = os.path.basename(filename)
    gse_name = os.path.basename(filename)[:-11]
    print i, "/", len(filenames), gse_name
    if db(Series.gse_name == gse_name).select().first():
        continue
    # 1/0
    # if lastGse:
    #     toSkip = toSkip and gse_name <> lastGse
    #     if toSkip:
    #         continue
    # if gse_name in errors:
    #     print gse_name, errors
    #     continue
    # if gse_name <> 'GSE39655':

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
    # db.commit()
    # continue
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
                value = value.decode('utf8').encode('utf8') #weir UTF problem with GSE31631
            name = name.replace("Sample_", "")

            sample_attribute_rec = db((Sample_Attribute.sample_id == sample_rec.id) & \
                                      (Sample_Attribute.name == name)).select().first() \
                                   or Sample_Attribute(Sample_Attribute.insert(sample_id=sample_rec.id,
                                                                               name=name,
                                                                               value=value))

    db.commit()
