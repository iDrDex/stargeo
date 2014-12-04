# coding: utf8

Series = db.define_table('series',
                         Field('id', 'id', readable=False, writable=False),
                         Field('gse_name', 'text', writable=False),
                         format='%(gse_name)s',
                         migrate='series.table'
)

Series_Attribute = db.define_table('series_attribute',
                                   Field('id', 'id', readable=False, writable=False),
                                   Field('series_id', 'reference series', writable=False),
                                   Field('attribute_name', 'text', writable=False),
                                   Field('attribute_value', 'text', writable=False),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='series_attribute.table'
)

Series_Attribute_Header = db.define_table('series_attribute_header',
                                          Field('id', 'id', readable=False, writable=False),
                                          Field('header'),
                                          migrate='series_attribute_header.table'
)

Series_View = db.define_table('series_view',
                              Field('id', 'id', readable=False, writable=False),
                              Field('series_id', 'reference series', writable=False),
                              *[Field(rec.header, 'text',
                                      writable=False,
                                      represent=lambda value, row: value or "")
                                for rec in db(Series_Attribute_Header).select()],
                              migrate=False)

Search = db.define_table('search',
                         Field('id', 'id', readable=False, writable=False),
                         Field('fts_query', unique=True),
                         format='%(fts_query)s',
                         migrate='search.table'
)

User_Search = db.define_table('user_search',
                              Field('id', 'id', readable=False, writable=False),
                              Field('search_id', "reference search"),
                              Field('keywords'),
                              Field('fts_query'),
                              auth.signature,
                              format='%(search_id)s_%(auth_user_id)s',
                              migrate='user_search.table'
)

Series_View_Results = db.define_table('series_view_results',
                                      Field('id', 'id', readable=False, writable=False),
                                      Field('series_view_id', 'integer', readable=False, writable=False),
                                      Field('search_id', 'reference search', readable=False, writable=False),
                                      format='%(keywords)s',
                                      migrate='series_view_results.table'
)

Platform = db.define_table('platform',
                           Field('id', 'id', readable=False, writable=False),
                           Field('gpl_name', 'text', writable=False),
                           Field('identifier', 'text', writable=False),
                           Field('scopes', 'text', writable=False),
                           Field('scopes', 'text', writable=False),
                           Field('identifier', 'text', writable=False),
                           Field('datafile', 'text', writable=False),
                           format='%(gpl_name)s',
                           migrate='platform.table'
)

Platform_Attribute = db.define_table('platform_attribute',
                                     Field('id', 'id', readable=False, writable=False),
                                     Field('platform_id', 'reference platform', writable=False),
                                     Field('attribute_name', 'text', writable=False),
                                     Field('attribute_value', 'text', writable=False),
                                     format='%(attribute_name)s_%(attribute_value)s',
                                     migrate='platform_attribute.table'
)

Platform_Probe = db.define_table('platform_probe',
                                 Field('id', 'id', readable=False, writable=False),
                                 Field('platform_id', 'reference platform', writable=False),
                                 Field('probe', 'text', writable=False),
                                 Field('myGene_sym', 'text', writable=False),
                                 Field('myGene_entrez', 'integer', writable=False),
                                 format='%(platform_id)s_%(probe)s',
                                 migrate='platform_probe.table'
)

Sample = db.define_table('sample',
                         Field('id', 'id', readable=False, writable=False),
                         Field('series_id', 'reference series', writable=False),
                         Field('platform_id', 'reference platform', writable=False),
                         Field('gsm_name', 'text', writable=False),
                         format='%(gsm_name)s',  # _%(series_id.gse_name)s_%(platform_id.gpl_name)s',
                         migrate='sample.table'
)

Sample_Attribute = db.define_table('sample_attribute',
                                   Field('id', 'id', readable=False, writable=False),
                                   Field('sample_id', 'reference sample', writable=False),
                                   Field('attribute_name', 'string', 256),
                                   Field('attribute_value', 'text', writable=False),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='sample_attribute.table'
)

Sample_Attribute_Header = db.define_table('sample_attribute_header',
                                          Field('id', 'id', readable=False, writable=False),
                                          Field('header'),
                                          Field('num', 'integer'),
                                          migrate='sample_attribute_header'
)

Sample_View = db.define_table('sample_view',
                              Field('id', 'id', readable=False, writable=False),
                              Field('series_id', 'reference series', writable=False),
                              Field('platform_id', 'reference platform', writable=False),
                              Field('sample_id', 'reference sample', writable=False),
                              *[Field(rec.header, 'text',
                                      writable=False,
                                      represent=lambda value, row: value or "")
                                for rec in db(Sample_Attribute_Header).select()],
                              migrate=False)

Sample_View_Results = db.define_table('sample_view_results',
                                      Field('id', 'id', readable=False, writable=False),
                                      Field('sample_view_id', 'integer', readable=False, writable=False),
                                      Field('search_id', 'reference search', readable=False, writable=False),
                                      format='%(keywords)s',
                                      migrate='sample_view_results.table'
)

Tag = db.define_table('tag',
                      Field('id', 'id', readable=False, writable=False),
                      Field('tag_name', unique=True),
                      Field('description'),
                      auth.signature,
                      format='%(tag_name)s',
                      migrate='tag.table'
)

Series_Tag = db.define_table('series_tag',
                             Field('id', 'id', readable=False, writable=False),
                             Field('series_id', 'reference series', writable=False),
                             Field('platform_id', 'reference platform'),
                             Field('tag_id', 'reference tag'),
                             Field('header'),
                             Field('regex', requires=IS_NOT_EMPTY()),
                             Field('show_invariant', 'boolean'),
                             auth.signature,
                             format='%(tag_id.tag_name)s_%(series_id.gse_name)s_%(platform_id.gpl_name)s',
                             migrate="series_tag.table"
)

Sample_Tag = db.define_table('sample_tag',
                             Field('id', 'id', readable=False, writable=False),
                             Field('sample_id', 'reference sample', writable=False),
                             Field('series_tag_id', 'reference series_tag', writable=False),
                             Field('annotation', 'text'),
                             auth.signature,
                             format='%(annotation)s',
                             migrate="sample_tag.table"
)

Sample_View_Annotation_Filter = db.define_table('sample_view_annotation_filter',
                                                Field('id', 'id', readable=False, writable=False),
                                                Field('sample_view_id', 'integer', readable=False, writable=False),
                                                Field('annotation', 'text',
                                                      represent=lambda value, row: value or ""),
                                                Field('session_id', default=response.session_id,
                                                      readable=False, writable=False),
                                                Field('created_on', 'datetime', default=request.now, readable=False,
                                                      writable=False),
                                                format='%(annotation)s',
                                                migrate="sample_view_annotation_filter.table"
)

Sample_Tag_View = db.define_table('sample_tag_view',
                                  Field('id', 'id', readable=False, writable=False),
                                  Field('series_id', 'reference series', writable=False),
                                  Field('platform_id', 'reference platform', writable=False),
                                  Field('sample_id', 'reference sample', writable=False),
                                  *[Field(field, 'text',
                                          writable=False,
                                          represent=lambda value, row: value or "")
                                    for field in [row.tag_name
                                                  for row in
                                                  db().select(Tag.tag_name,
                                                              distinct=True,
                                                              orderby=Tag.tag_name)]],
                                  migrate=False)

Sample_Tag_View_Results = db.define_table('sample_tag_view_results',
                                          Field('id', 'id', readable=False, writable=False),
                                          Field('sample_tag_view_id', 'integer', readable=False, writable=False),
                                          Field('search_id', 'reference search', readable=False, writable=False),
                                          format='%(keywords)s',
                                          migrate='sample_tag_view_results.table'
)

Series_Tag_View = db.define_table('series_tag_view',
                                  Field('id', 'id', readable=False, writable=False),
                                  Field('series_id', 'reference series', writable=False),
                                  Field('platform_id', 'reference platform', writable=False),
                                  Field('samples', 'integer', writable=False),
                                  *[Field(field, 'integer',
                                          writable=False,
                                          represent=lambda value, row: value or "")
                                    for field in [row.tag_name
                                                  for row in
                                                  db().select(Tag.tag_name,
                                                              distinct=True,
                                                              orderby=Tag.tag_name)]],
                                  migrate=False)

Series_Tag_View_Results = db.define_table('series_tag_view_results',
                                          Field('id', 'id', readable=False, writable=False),
                                          Field('series_tag_view_id', 'integer', readable=False, writable=False),
                                          Field('search_id', 'reference search', readable=False, writable=False),
                                          format='%(keywords)s',
                                          migrate='series_tag_view_results.table'
)


def update_sample_cross_tab(form, arg):  # wrapper for ondelete from home page
    get_sample_tag_cross_tab()


Count = db.define_table('count',
                        Field('id', 'id', readable=False, writable=False),
                        Field('what'),
                        Field('count', 'integer'),
                        migrate="count.table"
)

# Special pandas processor
# http://stackoverflow.com/questions/17039699/web2py-webserver-best-way-to-keep-connection-to-external-sql-server
def pandas_processor(rows, fields, columns, cacheable):
    from pandas import DataFrame

    # print "Reading PANDAS df"
    df = DataFrame \
        .from_records(rows, columns=columns) \
        .convert_objects(convert_numeric=True)
    # print "done"
    return df


class IS_PANDAS_QUERY:
    def __init__(self, error_message='Must be a valid query'):
        self.error_message = error_message
        self.df = get_full_df()

    def __call__(self, value):
        try:
            if value:
                self.df.query(value.lower())
            return value, None
        except:
            return (value, self.error_message)


Analysis = db.define_table('analysis',
                           Field('id', 'id', readable=False, writable=False),
                           Field('analysis_name', requires=IS_NOT_EMPTY()),
                           Field('description'),
                           Field('case_query', requires=[IS_NOT_EMPTY(), IS_PANDAS_QUERY()]),
                           Field('control_query', requires=[IS_NOT_EMPTY(), IS_PANDAS_QUERY()]),
                           Field('modifier_query', requires=IS_PANDAS_QUERY()),
                           # Field('case_query', requires=[IS_NOT_EMPTY()]),
                           # Field('control_query', requires=[IS_NOT_EMPTY()]),
                           # Field('modifier_query', requires=[]),
                           Field('series_count', 'integer', readable=False, writable=False),
                           Field('platform_count', 'integer', readable=False, writable=False),
                           Field('sample_count', 'integer', readable=False, writable=False),
                           Field('series_ids', 'list:reference series', readable=False, writable=False),
                           Field('platform_ids', 'list:reference platform', readable=False, writable=False),
                           Field('sample_ids', 'list:reference sample', readable=False, writable=False),
                           auth.signature,
                           migrate="analysis.table"
)

Balanced_Meta = db.define_table('balanced_meta',
                                Field('id', 'id', readable=False, writable=False),
                                Field('analysis_id', 'reference analysis', readable=False, writable=False),
                                Field('mygene_sym',
                                      represent=lambda name, row: A(name,
                                                                    _href="http://www.genecards.org/cgi-bin/carddisp.pl?gene=%s" % row.mygene_sym,
                                                                    _target="blank")),
                                Field('mygene_entrez', 'integer'),
                                Field('direction'),
                                Field('k', 'double'),
                                Field('TE_fixed', 'double'),
                                Field('TE_random', 'double'),
                                Field('pval_fixed', 'double'),
                                Field('pval_random', 'double'),
                                Field('C', 'double'),
                                Field('H', 'double'),
                                Field('I2', 'double'),
                                Field('Q', 'double'),
                                Field('TE', 'double'),
                                Field('TE_tau', 'double'),
                                # Field('bylab'),
                                # Field('byvar'),
                                # Field('call', 'double'),
                                Field('caseDataCount', 'double'),
                                Field('comb_fixed', 'double'),
                                Field('comb_random', 'double'),
                                # Field('complab'),
                                Field('controlDataCount', 'double'),
                                Field('data', 'double'),
                                Field('df_Q', 'double'),
                                # Field('df_hakn', 'double'),
                                # Field('hakn', 'double'),
                                # Field('label_c'),
                                # Field('label_e'),
                                # Field('label_left'),
                                # Field('label_right'),
                                # Field('level', 'double'),
                                # Field('level_comb', 'double'),
                                # Field('level_predict', 'double'),
                                # Field('lower_H', 'double'),
                                # Field('lower_I2', 'double'),
                                Field('lower_fixed', 'double'),
                                Field('lower_predict', 'double'),
                                Field('lower_random', 'double'),
                                Field('mean_c', 'double'),
                                Field('mean_e', 'double'),
                                # Field('method'),
                                # Field('method_bias'),
                                # Field('method_tau'),
                                Field('n_c', 'double'),
                                Field('n_e', 'double'),
                                # Field('outclab', 'double'),
                                # Field('prediction', 'double'),
                                # Field('print_byvar', 'double'),
                                Field('sd_c', 'double'),
                                Field('sd_e', 'double'),
                                Field('se_tau2', 'double'),
                                Field('seTE', 'double'),
                                Field('seTE_fixed', 'double'),
                                # Field('seTE_predict', 'double'),
                                Field('seTE_random', 'double'),
                                # Field('sm'),
                                # Field('studlab'),
                                # Field('subset'),
                                Field('tau', 'double'),
                                Field('tau_common', 'double'),
                                # Field('tau_preset', 'double'),
                                # Field('title'),
                                # Field('upper_H', 'double'),
                                # Field('upper_I2', 'double'),
                                Field('upper_fixed', 'double'),
                                # Field('upper_predict', 'double'),
                                Field('upper_random', 'double'),
                                Field('version'),
                                Field('w_fixed', 'double'),
                                Field('w_random', 'double'),
                                # Field('warn', 'boolean'),
                                Field('zval_fixed', 'double'),
                                Field('zval_random', 'double'),
                                # Field('effect_size', 'double'),

                                format='%(mmygene_sym)s',
                                migrate='balanced_meta.table'

)