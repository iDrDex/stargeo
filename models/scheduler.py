__author__ = 'dex'

from gluon.scheduler import Scheduler

def task_analyze(analysis_name, description, case_query, control_query, modifier_query):
    df = get_analysis_df(case_query, control_query, modifier_query)
    # construct the GSEs
    gses = []
    for series_id in df.series_id.unique():
        gpl2data = {}
        gpl2probes = {}

        for platform_id in df.query("""series_id == %s""" % series_id).platform_id.unique():
            gpl_name = Platform[platform_id].gpl_name
            gpl2data[gpl_name] = get_data(series_id, platform_id)
            gpl2probes[gpl_name] = get_probes(platform_id)
        samples = df.query('series_id == %s' % series_id)
        gse_name = Series[series_id].gse_name
        gses.append(Gse(gse_name, samples, gpl2data, gpl2probes))

    analyzer = MetaAnalyzer(gses)
    balanced = analyzer.getBalancedResults().reset_index()

    balanced.columns = balanced.columns.map(lambda x: x.replace(".", "_"))

    analysis_id = Analysis.insert(analysis_name=analysis_name,
                                  description=description,
                                  case_query=case_query,
                                  control_query=control_query,
                                  modifier_query=modifier_query
                                  )
    balanced['analysis_id'] = int(analysis_id)
    # replace infs with None for db insert
    balanced = balanced.replace([np.inf, -np.inf], np.nan)
    # http://stackoverflow.com/questions/14162723/replacing-pandas-or-numpy-nan-with-a-none-to-use-with-mysqldb
    balanced = balanced.where((pd.notnull(balanced)), None)
    rows = balanced[Balanced_Meta.fields[1:]].T.to_dict().values()
    Balanced_Meta.bulk_insert(rows)
    db.commit()

scheduler = Scheduler(db,
                      dict(task_analyze=task_analyze),
                      utc_time=True
)


