__author__ = 'dex'

import logging
logger = logging.getLogger("stargeo.analysis")
logger.setLevel(logging.DEBUG)

from itertools import imap

from gluon.scheduler import Scheduler

def task_analyze(analysis_name, description, case_query, control_query, modifier_query, debug=False):
    logger.info('Started %s analysis', analysis_name)
    df = get_analysis_df(case_query, control_query, modifier_query)
    logger.debug('Loaded %s analysis dataframe', analysis_name)
    debug and df.to_csv("%s.analysis_df.csv"%analysis_name)

    # Load GSE data, make and concat all fold change analyses results.
    # NOTE: we are doing load_gse() lazily here to avoid loading all matrices at once.
    print "Calculating fold changes..."
    gses = (load_gse(df, series_id) for series_id in df.series_id.unique())
    fold_changes = pd.concat(imap(get_fold_change_analysis, gses))
    debug and fold_changes.to_csv("%s.fc.csv" % debug)

    print "Meta-Analyzing..."
    balanced = getFullMetaAnalysis(fold_changes, debug=debug).reset_index()
    debug and balanced.to_csv("%s.meta.csv" % debug)

    balanced.columns = balanced.columns.map(lambda x: x.replace(".", "_"))
    analysis_id = Analysis.insert(analysis_name=analysis_name,
                                  description=description,
                                  case_query=case_query,
                                  control_query=control_query,
                                  modifier_query=modifier_query,
                                  series_count = len(df.series_id.unique()),
                                  platform_count = len(df.platform_id.unique()),
                                  sample_count = len(df.sample_id.unique()),
                                  series_ids = df.series_id.unique().tolist(),
                                  platform_ids = df.platform_id.unique().tolist(),
                                  sample_ids = df.sample_id.unique().tolist(),
                                  )
    balanced['analysis_id'] = int(analysis_id)
    # replace infs with None for db insert
    balanced = balanced.replace([np.inf, -np.inf], np.nan)
    # http://stackoverflow.com/questions/14162723/replacing-pandas-or-numpy-nan-with-a-none-to-use-with-mysqldb
    balanced = balanced.where((pd.notnull(balanced)), None)
    rows = balanced[Balanced_Meta.fields[1:]].T.to_dict().values()
    print "Inserting..."
    Balanced_Meta.bulk_insert(rows)
    db.commit()
    print "DONE!"


def get_fold_change_analysis(gse):
    # TODO: get rid of unneeded OOP interface
    return GseAnalyzer(gse).getResults(how='fc', debug=False)

def load_gse(df, series_id):
    gpl2data = {}
    gpl2probes = {}

    for platform_id in df.query("""series_id == %s""" % series_id).platform_id.unique():
        gpl_name = Platform[platform_id].gpl_name
        gpl2data[gpl_name] = get_data(series_id, platform_id)
        gpl2probes[gpl_name] = get_probes(platform_id)
    samples = df.query('series_id == %s' % series_id)
    gse_name = Series[series_id].gse_name
    return Gse(gse_name, samples, gpl2data, gpl2probes)


scheduler = Scheduler(db,
                      dict(task_analyze=task_analyze),
                      utc_time=True, #Set scheduler to UTC,
                      migrate="my_",
)
request.now = request.utcnow #set up workers on UTC


#convenience for controllers
Scheduler_Task = db['scheduler_task']
Scheduler_Task.vars.represent = lambda name, row: BEAUTIFY(eval(name))

